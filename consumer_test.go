package amqp

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"gitlab.com/gorib/amqp/model"
	"gitlab.com/gorib/session"
)

// The only way to install a transport func: assigning the global directly
// leaves it pointing at a finished subtest's mock.
func swapTransportFunc(t *testing.T, fn func(dsn string) (transportAPI, error)) {
	t.Helper()
	previous := newTransportFunc
	t.Cleanup(func() { newTransportFunc = previous })
	newTransportFunc = fn
}

func initTransportFunc(t *testing.T, ft *mockTransport) *mockTransport {
	t.Helper()
	swapTransportFunc(t, func(dsn string) (transportAPI, error) { return ft, nil })
	return ft
}

// The channel handover orders a transport's fields against the consumer goroutine.
func recordTransports(t *testing.T, messages func(n int) chan amqp.Delivery) <-chan *mockTransport {
	t.Helper()
	created := make(chan *mockTransport, 8)
	n := 0
	swapTransportFunc(t, func(dsn string) (transportAPI, error) {
		ft := &mockTransport{messages: messages(n)}
		n++
		created <- ft
		return ft, nil
	})
	return created
}

func nextTransport(t *testing.T, created <-chan *mockTransport, expected string) *mockTransport {
	t.Helper()
	select {
	case ft := <-created:
		return ft
	case <-time.After(2 * time.Second):
		t.Fatal(expected)
		return nil
	}
}

type testRecover struct{}

func (testRecover) Name() string { return "recover" }
func (testRecover) Order() int   { return 500 }

func (testRecover) Wrap(next func(msgs []Source) error) func(msgs []Source) error {
	return func(msgs []Source) (err error) {
		defer func() {
			if r := recover(); r != nil && err == nil {
				err = errors.New("panic")
			}
		}()
		return next(msgs)
	}
}

func TestSwapTransportFuncRestoresAfterSubtest(t *testing.T) {
	original := newTransportFunc

	t.Run("swaps", func(t *testing.T) {
		swapTransportFunc(t, func(dsn string) (transportAPI, error) { return nil, nil })
		if reflect.ValueOf(newTransportFunc).Pointer() == reflect.ValueOf(original).Pointer() {
			t.Fatal("transport func not swapped")
		}
	})

	if reflect.ValueOf(newTransportFunc).Pointer() != reflect.ValueOf(original).Pointer() {
		t.Error("transport func leaked out of the subtest")
	}
}

func TestConsumer(t *testing.T) {
	trFunc := newTransportFunc
	defer func() {
		newTransportFunc = trFunc
	}()

	t.Run("NilHandler", func(t *testing.T) {
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		require.ErrorContains(t, cons.Consume(context.Background()), "message handler is undefined")
	})

	t.Run("SuccessConstructor", func(t *testing.T) {
		dsn := "amqp://user:pass@host/vhost"
		ex := "exchg"
		q := "queue1"
		rk := "a.b.*"
		ps := uint16(7)
		fi := 3 * time.Second

		cons, err := NewConsumer(dsn, ex, q,
			WithConsumerPageSize(ps),
			WithConsumerFlushInterval(fi),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind(rk))

		require.Equal(t, dsn, cons.dsn)
		require.Equal(t, ex, cons.exchange)
		require.Equal(t, q, cons.queue)
		require.Equal(t, []string{rk}, cons.routingKeys)
		require.EqualValues(t, ps, cons.pageSize)
		require.Equal(t, fi, cons.flushInterval)
		require.Equal(t, 300*time.Second, cons.failsafeTimeout)
	})

	t.Run("ZeroPageSizeDefaults", func(t *testing.T) {
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(0),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		require.EqualValues(t, uint16(1), cons.pageSize)
		require.Equal(t, time.Second, cons.flushInterval)
		require.Equal(t, "amqp://", cons.dsn)
		require.Equal(t, "ex", cons.exchange)
		require.Equal(t, "q", cons.queue)
	})

	t.Run("BindEmptyRejected", func(t *testing.T) {
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.ErrorContains(t, cons.Bind(""), "routing key is empty")
	})

	t.Run("BindDeduplicates", func(t *testing.T) {
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("a"))
		require.NoError(t, cons.Bind("a"))
		require.NoError(t, cons.Bind("b"))
		require.Equal(t, []string{"a", "b"}, cons.routingKeys)
	})

	t.Run("InvalidConstructorFlushInterval", func(t *testing.T) {
		_, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerFlushInterval(500*time.Millisecond),
		)
		require.Error(t, err)
		require.ErrorContains(t, err, "flush interval")
	})

	t.Run("InvalidIntervals", func(t *testing.T) {
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		require.Error(t, cons.SetFlushInterval(500*time.Millisecond))
		require.Error(t, cons.SetReconnectInterval(500*time.Millisecond))
	})

	t.Run("InvalidConfig", func(t *testing.T) {
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		require.NoError(t, cons.SetExchange(""))
		//nolint:errcheck
		cons.SetHandler(func(msgs []Source) error { return nil })
		require.ErrorContains(t, cons.Consume(context.Background()), "invalid consumer config")
	})

	t.Run("Session", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 2)})

		msg := amqp.Delivery{
			Body:       []byte("{\"ok\":true}"),
			RoutingKey: "service.action",
			Headers: amqp.Table{
				model.UserAgentHeader:    "ua-bot/1.0",
				model.UserLanguageHeader: "ru,en;q=0.8",
				model.SourceIpHeader:     "203.0.113.7",
			},
			CorrelationId: "corr-123",
			Acknowledger:  &mockAcker{},
		}

		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		done := make(chan struct{})
		//nolint:errcheck
		cons.SetHandler(func(msgs []Source) error {
			require.Len(t, msgs, 1)
			require.Equal(t, "service.action", msgs[0].RoutingKey())
			require.Equal(t, []byte("{\"ok\":true}"), msgs[0].Body())
			s := session.FromContext(msgs[0].Context())
			require.NotNil(t, s)
			require.Equal(t, msg.RoutingKey, s.Uri)
			require.Equal(t, "AMQP", s.Method)
			require.Equal(t, string(msg.Body), s.Body)
			require.Equal(t, msg.CorrelationId, s.CorrelationId)
			require.Equal(t, msg.Headers[model.UserAgentHeader], s.UserAgent)
			require.Equal(t, msg.Headers[model.UserLanguageHeader], s.UserLanguage)
			require.Equal(t, msg.Headers[model.SourceIpHeader], s.SourceIp)
			close(done)
			return nil
		})

		ctx := context.Background()
		errCh := make(chan error, 1)
		go func() { errCh <- cons.Consume(ctx) }()

		ft.messages <- msg

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("handler with session not called in time")
		}

		require.NoError(t, cons.Close())
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("consumer did not stop")
		}
	})

	t.Run("SessionPerMessageInBatch", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 4)})

		hCalled := make(chan struct{})
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(2),
			WithConsumerFlushInterval(time.Hour),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		//nolint:errcheck
		cons.SetHandler(func(msgs []Source) error {
			require.Len(t, msgs, 2)
			// Each delivery now carries its own session, even in a batch.
			require.NotNil(t, session.FromContext(msgs[0].Context()))
			close(hCalled)
			return nil
		})

		errCh := make(chan error, 1)
		go func() { errCh <- cons.Consume(context.Background()) }()

		ack1 := &mockAcker{}
		ack2 := &mockAcker{}
		ft.messages <- amqp.Delivery{Body: []byte("a"), RoutingKey: "rk", Acknowledger: ack1}
		ft.messages <- amqp.Delivery{Body: []byte("b"), RoutingKey: "rk", Acknowledger: ack2}

		select {
		case <-hCalled:
		case <-time.After(2 * time.Second):
			t.Fatal("batched handler not called in time")
		}
		require.NoError(t, cons.Close())
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("consumer did not stop after close")
		}
	})

	t.Run("WrongTransport", func(t *testing.T) {
		swapTransportFunc(t, func(dsn string) (transportAPI, error) { return nil, errors.New("error") })
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		//nolint:errcheck
		cons.SetHandler(func(msgs []Source) error {
			return nil
		})
		require.ErrorContains(t, cons.Consume(context.Background()), "error")
	})

	t.Run("ExplicitWildcardBind", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 1)})
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("#"))
		//nolint:errcheck
		cons.SetHandler(func(msgs []Source) error { return nil })
		done := make(chan struct{})
		go func() { _ = cons.Consume(context.Background()); close(done) }()
		require.NoError(t, cons.Close())
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("consumer did not stop")
		}
		require.Len(t, ft.consumeArgs, 1)
		require.Equal(t, "#", ft.consumeArgs[0].rk)
	})

	t.Run("ExchangeDeclareFail", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 10)})
		ft.exchangeErr = errors.New("error")
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		//nolint:errcheck
		cons.SetHandler(func(msgs []Source) error {
			return nil
		})
		require.ErrorContains(t, cons.Consume(context.Background()), "error")
	})

	t.Run("DlxExchangeDeclareSuccess", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 1)})
		dlx := "dlx-exchange"
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
			WithConsumerQueueArguments(QueueConfig{DeadLetterExchange: dlx}),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		require.Equal(t, dlx, cons.dlxExchange)

		//nolint:errcheck
		cons.SetHandler(func(msgs []Source) error { return nil })
		done := make(chan struct{})
		go func() { _ = cons.Consume(context.Background()); close(done) }()

		require.NoError(t, cons.Close())
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("consumer did not stop")
		}

		require.Contains(t, ft.exchangeDecls, dlx)
		require.Contains(t, ft.exchangeDecls, "ex")
	})

	t.Run("ConsumeFail", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 10)})
		ft.consumeErr = errors.New("error")
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		//nolint:errcheck
		cons.SetHandler(func(msgs []Source) error {
			return nil
		})
		require.ErrorContains(t, cons.Consume(context.Background()), "error")
	})

	t.Run("FailsafeTimeout", func(t *testing.T) {
		created := recordTransports(t, func(int) chan amqp.Delivery {
			return make(chan amqp.Delivery, 10)
		})
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Hour),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		require.NoError(t, cons.SetReconnectInterval(1*time.Second))
		//nolint:errcheck
		cons.SetHandler(func(msgs []Source) error {
			return nil
		})

		errCh := make(chan error, 1)
		go func() { errCh <- cons.Consume(context.Background()) }()

		first := nextTransport(t, created, "consumer did not connect")
		second := nextTransport(t, created, "failsafe timeout did not force a reconnect")

		require.NoError(t, cons.Close())
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("consumer did not stop")
		}

		require.True(t, first.closed)
		require.True(t, second.closed)
	})

	t.Run("ReconnectAndProcessAfterTimeout", func(t *testing.T) {
		created := recordTransports(t, func(int) chan amqp.Delivery {
			return make(chan amqp.Delivery, 10)
		})
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Hour),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		require.NoError(t, cons.SetReconnectInterval(1*time.Second))

		done := make(chan struct{})
		//nolint:errcheck
		cons.SetHandler(func(msgs []Source) error {
			close(done)
			return nil
		})

		errCh := make(chan error, 1)
		go func() { errCh <- cons.Consume(context.Background()) }()

		first := nextTransport(t, created, "consumer did not connect")
		second := nextTransport(t, created, "failsafe timeout did not force a reconnect")

		ack := &mockAcker{}
		second.messages <- amqp.Delivery{Body: []byte("z"), RoutingKey: "rk", Acknowledger: ack}

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("handler not called after reconnect")
		}

		require.NoError(t, cons.Close())
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("consumer did not stop")
		}
		require.True(t, first.closed)
	})

	t.Run("ClosedDeliveryChannelReconnects", func(t *testing.T) {
		created := recordTransports(t, func(n int) chan amqp.Delivery {
			msgs := make(chan amqp.Delivery, 1)
			if n == 0 {
				// what amqp091 does when the connection or channel drops
				close(msgs)
			}
			return msgs
		})

		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Hour),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		// Long enough that only the closed channel can drive the reconnect.
		require.NoError(t, cons.SetReconnectInterval(time.Hour))

		var handlerCalls atomic.Int64
		//nolint:errcheck
		cons.SetHandler(func(msgs []Source) error {
			handlerCalls.Add(1)
			return nil
		})

		errCh := make(chan error, 1)
		go func() { errCh <- cons.Consume(context.Background()) }()

		first := nextTransport(t, created, "consumer did not connect")
		nextTransport(t, created, "consumer did not reconnect after the delivery channel closed")

		require.NoError(t, cons.Close())
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("consumer did not stop")
		}

		require.Zero(t, handlerCalls.Load(), "handler called on a closed delivery channel")
		require.True(t, first.closed)
	})

	t.Run("Ack", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 10)})
		ack1 := &mockAcker{}
		ack2 := &mockAcker{}
		msg1 := amqp.Delivery{Body: []byte("a"), RoutingKey: "rs", Acknowledger: ack1}
		msg2 := amqp.Delivery{Body: []byte("b"), RoutingKey: "rk", Acknowledger: ack2}

		var gotKeys []string
		var gotBodies [][]byte
		done := make(chan struct{})
		h := func(msgs []Source) error {
			require.Len(t, msgs, 2)
			for _, m := range msgs {
				gotKeys = append(gotKeys, m.RoutingKey())
				gotBodies = append(gotBodies, m.Body())
			}
			close(done)
			return nil
		}
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(2),
			WithConsumerFlushInterval(time.Hour),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		//nolint:errcheck
		cons.SetHandler(h)

		ctx := context.Background()
		errCh := make(chan error, 1)
		go func() { errCh <- cons.Consume(ctx) }()

		ft.messages <- msg1
		ft.messages <- msg2

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("handler not called in time")
		}
		require.NoError(t, cons.Close())
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("consumer did not stop")
		}

		require.Equal(t, []string{"ex"}, ft.exchangeDecls)
		require.Len(t, ft.consumeArgs, 1)
		args := ft.consumeArgs[0]
		require.Equal(t, "q", args.q)
		require.Equal(t, "rk", args.rk)
		require.Equal(t, "ex", args.ex)
		require.EqualValues(t, 2, args.prefetch)

		require.Equal(t, []string{"rs", "rk"}, gotKeys)
		require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, gotBodies)
		require.Equal(t, 0, ack1.ackCalls)
		require.Equal(t, 1, ack2.ackCalls)
	})

	t.Run("Nack", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 2)})
		ack := &mockAcker{}
		msg := amqp.Delivery{Body: []byte("x"), RoutingKey: "rk", Acknowledger: ack}
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		handled := make(chan struct{})
		//nolint:errcheck
		cons.SetHandler(testRecover{}.Wrap(func(msgs []Source) error {
			close(handled)
			panic("some error")
		}))
		ctx := context.Background()
		errCh := make(chan error, 1)
		go func() { errCh <- cons.Consume(ctx) }()
		ft.messages <- msg

		select {
		case <-handled:
		case <-time.After(2 * time.Second):
			t.Fatal("handler not called in time")
		}
		require.NoError(t, cons.Close())
		select {
		case <-errCh:
		case <-time.After(2*time.Second + retrySleep):
			t.Fatal("consumer did not stop")
		}

		require.Equal(t, 1, ack.nackCalls)
		require.Equal(t, 0, ack.ackCalls)
		require.True(t, ack.lastRequeue)
		require.False(t, ack.lastMultiple)
	})

	t.Run("NackBatchMultiple", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 2)})
		ack1 := &mockAcker{}
		ack2 := &mockAcker{}
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(2),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		handled := make(chan struct{})
		//nolint:errcheck
		cons.SetHandler(testRecover{}.Wrap(func(msgs []Source) error {
			close(handled)
			panic("boom")
		}))
		errCh := make(chan error, 1)
		go func() { errCh <- cons.Consume(context.Background()) }()
		ft.messages <- amqp.Delivery{Body: []byte("a"), RoutingKey: "rk", Acknowledger: ack1}
		ft.messages <- amqp.Delivery{Body: []byte("b"), RoutingKey: "rk", Acknowledger: ack2}

		select {
		case <-handled:
		case <-time.After(2 * time.Second):
			t.Fatal("batched handler not called in time")
		}
		require.NoError(t, cons.Close())
		select {
		case <-errCh:
		case <-time.After(2*time.Second + retrySleep):
			t.Fatal("consumer did not stop")
		}

		require.Equal(t, 0, ack1.nackCalls)
		require.Equal(t, 1, ack2.nackCalls)
		require.True(t, ack2.lastMultiple)
		require.True(t, ack2.lastRequeue)
	})

	t.Run("FlushTicker", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 1)})
		ack := &mockAcker{}
		msg := amqp.Delivery{Body: []byte("only"), RoutingKey: "rk", Acknowledger: ack}

		done := make(chan struct{})
		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(5),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk"))
		require.NoError(t, cons.SetReconnectInterval(time.Hour))
		//nolint:errcheck
		cons.SetHandler(func(msgs []Source) error {
			require.Len(t, msgs, 1)
			require.Equal(t, "rk", msgs[0].RoutingKey())
			require.Equal(t, []byte("only"), msgs[0].Body())
			close(done)
			return nil
		})

		ctx := context.Background()
		errCh := make(chan error, 1)
		go func() { errCh <- cons.Consume(ctx) }()

		ft.messages <- msg

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("flush ticker did not trigger batch processing")
		}

		require.NoError(t, cons.Close())
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("consumer did not stop after close")
		}

		require.Equal(t, 1, ack.ackCalls)
		require.True(t, ack.lastMultiple)
	})

	t.Run("PageSizeZeroDefaultsToOne", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 1)})

		ack := &mockAcker{}
		msg := amqp.Delivery{
			Body:          []byte("p0"),
			RoutingKey:    "rk0",
			Acknowledger:  ack,
			CorrelationId: "corr-p0",
			Headers: amqp.Table{
				model.UserAgentHeader:    "ua/0",
				model.UserLanguageHeader: "en",
				model.SourceIpHeader:     "203.0.113.9",
			},
		}

		cons, err := NewConsumer("amqp://", "ex", "q",
			WithConsumerPageSize(0),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.Bind("rk0"))

		done := make(chan struct{})
		//nolint:errcheck
		cons.SetHandler(func(msgs []Source) error {
			require.Len(t, msgs, 1)
			require.Equal(t, "rk0", msgs[0].RoutingKey())
			require.Equal(t, []byte("p0"), msgs[0].Body())
			s := session.FromContext(msgs[0].Context())
			require.NotNil(t, s)
			require.Equal(t, msg.CorrelationId, s.CorrelationId)
			close(done)
			return nil
		})

		errCh := make(chan error, 1)
		go func() { errCh <- cons.Consume(context.Background()) }()

		ft.messages <- msg

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("handler not called for pageSize=0 defaulting to 1")
		}

		require.NoError(t, cons.Close())
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("consumer did not stop")
		}

		require.Equal(t, 1, ack.ackCalls)
		require.False(t, ack.lastMultiple)
		require.Len(t, ft.consumeArgs, 1)
		require.EqualValues(t, 1, ft.consumeArgs[0].prefetch)
	})
}
