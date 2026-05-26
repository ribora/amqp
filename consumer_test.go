package amqp

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"gitlab.com/gorib/amqp/model"
	"gitlab.com/gorib/session"
)

func initTransportFunc(t *testing.T, ft *mockTransport) *mockTransport {
	t.Helper()
	newTransportFunc = func(dsn string) (transportAPI, error) { return ft, nil }
	return ft
}

type testRecover struct{}

func (testRecover) Name() string { return "recover" }
func (testRecover) Order() int   { return 500 }

func (testRecover) Wrap(next func(ctx context.Context, keys []string, documents [][]byte) error) func(ctx context.Context, keys []string, documents [][]byte) error {
	return func(ctx context.Context, keys []string, documents [][]byte) (err error) {
		defer func() {
			if r := recover(); r != nil && err == nil {
				err = errors.New("panic")
			}
		}()
		return next(ctx, keys, documents)
	}
}

func TestConsumer(t *testing.T) {
	trFunc := newTransportFunc
	defer func() {
		newTransportFunc = trFunc
	}()

	t.Run("NilHandler", func(t *testing.T) {
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.ErrorContains(t, cons.Consume(context.Background()), "message handler is undefined")
	})

	t.Run("SuccessConstructor", func(t *testing.T) {
		dsn := "amqp://user:pass@host/vhost"
		ex := "exchg"
		q := "queue1"
		rk := "a.b.*"
		ps := uint16(7)
		fi := 3 * time.Second

		cons, err := NewConsumer(dsn, ex, q, rk,
			WithConsumerPageSize(ps),
			WithConsumerFlushInterval(fi),
		)
		require.NoError(t, err)

		require.Equal(t, dsn, cons.dsn)
		require.Equal(t, ex, cons.exchange)
		require.Equal(t, q, cons.queue)
		require.Equal(t, rk, cons.routingKey)
		require.EqualValues(t, ps, cons.pageSize)
		require.Equal(t, fi, cons.flushInterval)
		require.Equal(t, 300*time.Second, cons.failsafeTimeout)
	})

	t.Run("SuccessEmptyRoutingKeyAndZeroPageSize", func(t *testing.T) {
		cons, err := NewConsumer("amqp://", "ex", "q", "",
			WithConsumerPageSize(0),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.Equal(t, "#", cons.routingKey)
		require.EqualValues(t, uint16(1), cons.pageSize)
		require.Equal(t, time.Second, cons.flushInterval)
		require.Equal(t, "amqp://", cons.dsn)
		require.Equal(t, "ex", cons.exchange)
		require.Equal(t, "q", cons.queue)
	})

	t.Run("InvalidConstructorFlushInterval", func(t *testing.T) {
		_, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerFlushInterval(500*time.Millisecond),
		)
		require.Error(t, err)
		require.ErrorContains(t, err, "flush interval")
	})

	t.Run("InvalidIntervals", func(t *testing.T) {
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.Error(t, cons.SetFlushInterval(500*time.Millisecond))
		require.Error(t, cons.SetReconnectInterval(500*time.Millisecond))
	})

	t.Run("InvalidConfig", func(t *testing.T) {
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.SetExchange(""))
		cons.SetHandler(func(ctx context.Context, keys []string, documents [][]byte) error { return nil })
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

		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		done := make(chan struct{})
		cons.SetHandler(func(ctx context.Context, keys []string, documents [][]byte) error {
			require.Len(t, keys, 1)
			require.Equal(t, "service.action", keys[0])
			require.Equal(t, [][]byte{[]byte("{\"ok\":true}")}, documents)
			s := session.FromContext(ctx)
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

	t.Run("NoSessionForBatch", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 4)})

		hCalled := make(chan struct{})
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(2),
			WithConsumerFlushInterval(time.Hour),
		)
		require.NoError(t, err)
		cons.SetHandler(func(ctx context.Context, keys []string, documents [][]byte) error {
			require.Len(t, keys, 2)
			require.Nil(t, session.FromContext(ctx))
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
		newTransportFunc = func(dsn string) (transportAPI, error) { return nil, errors.New("error") }
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		cons.SetHandler(func(ctx context.Context, keys []string, documents [][]byte) error {
			return nil
		})
		require.ErrorContains(t, cons.Consume(context.Background()), "error")
	})

	t.Run("DefaultRoutingKeyWildcard", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 1)})
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.SetRoutingKey(""))
		cons.SetHandler(func(ctx context.Context, keys []string, documents [][]byte) error { return nil })
		done := make(chan struct{})
		go func() { _ = cons.Consume(context.Background()); close(done) }()
		time.Sleep(50 * time.Millisecond)
		require.Len(t, ft.consumeArgs, 1)
		require.Equal(t, "#", ft.consumeArgs[0].rk)
		require.NoError(t, cons.Close())
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("consumer did not stop")
		}
	})

	t.Run("ExchangeDeclareFail", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 10)})
		ft.exchangeErr = errors.New("error")
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		cons.SetHandler(func(ctx context.Context, keys []string, documents [][]byte) error {
			return nil
		})
		require.ErrorContains(t, cons.Consume(context.Background()), "error")
	})

	t.Run("DlxExchangeDeclareSuccess", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 1)})
		dlx := "dlx-exchange"
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
			WithConsumerQueueArguments(QueueConfig{DeadLetterExchange: dlx}),
		)
		require.NoError(t, err)
		require.Equal(t, dlx, cons.dlxExchange)

		cons.SetHandler(func(ctx context.Context, keys []string, documents [][]byte) error { return nil })
		go func() { _ = cons.Consume(context.Background()) }()
		time.Sleep(50 * time.Millisecond)

		require.Contains(t, ft.exchangeDecls, dlx)
		require.Contains(t, ft.exchangeDecls, "ex")
		_ = cons.Close()
	})

	t.Run("ConsumeFail", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 10)})
		ft.consumeErr = errors.New("error")
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		cons.SetHandler(func(ctx context.Context, keys []string, documents [][]byte) error {
			return nil
		})
		require.ErrorContains(t, cons.Consume(context.Background()), "error")
	})

	t.Run("FailsafeTimeout", func(t *testing.T) {
		var transports []*mockTransport
		calls := 0
		newTransportFunc = func(dsn string) (transportAPI, error) {
			calls++
			ft := &mockTransport{messages: make(chan amqp.Delivery, 10)}
			transports = append(transports, ft)
			return ft, nil
		}
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Hour),
		)
		require.NoError(t, err)
		require.NoError(t, cons.SetReconnectInterval(1*time.Second))
		cons.SetHandler(func(ctx context.Context, keys []string, documents [][]byte) error {
			return nil
		})

		errCh := make(chan error, 1)
		go func() { errCh <- cons.Consume(context.Background()) }()

		time.Sleep(1200 * time.Millisecond)
		require.NoError(t, cons.Close())
		require.GreaterOrEqual(t, calls, 2)

		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("consumer did not stop")
		}

		for _, ft := range transports {
			require.True(t, ft.closed)
		}
	})

	t.Run("ReconnectAndProcessAfterTimeout", func(t *testing.T) {
		var transports []*mockTransport
		newTransportFunc = func(dsn string) (transportAPI, error) {
			ft := &mockTransport{messages: make(chan amqp.Delivery, 10)}
			transports = append(transports, ft)
			return ft, nil
		}
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Hour),
		)
		require.NoError(t, err)
		require.NoError(t, cons.SetReconnectInterval(1*time.Second))

		done := make(chan struct{})
		cons.SetHandler(func(ctx context.Context, keys []string, documents [][]byte) error {
			close(done)
			return nil
		})

		errCh := make(chan error, 1)
		go func() { errCh <- cons.Consume(context.Background()) }()

		time.Sleep(1200 * time.Millisecond)
		require.GreaterOrEqual(t, len(transports), 2)

		ack := &mockAcker{}
		transports[1].messages <- amqp.Delivery{Body: []byte("z"), RoutingKey: "rk", Acknowledger: ack}

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
		require.True(t, transports[0].closed)
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
		h := func(ctx context.Context, keys []string, payloads [][]byte) error {
			require.Len(t, keys, 2)
			gotKeys = keys
			gotBodies = append([][]byte(nil), payloads...)
			close(done)
			return nil
		}
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(2),
			WithConsumerFlushInterval(time.Hour),
		)
		require.NoError(t, err)
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
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(1),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		cons.SetHandler(testRecover{}.Wrap(func(ctx context.Context, keys []string, documents [][]byte) error {
			panic("some error")
		}))
		ctx := context.Background()
		go cons.Consume(ctx)
		ft.messages <- msg
		time.Sleep(50 * time.Millisecond)
		_ = cons.Close()
		require.Equal(t, 1, ack.nackCalls)
		require.Equal(t, 0, ack.ackCalls)
		require.True(t, ack.lastRequeue)
		require.False(t, ack.lastMultiple)
	})

	t.Run("NackBatchMultiple", func(t *testing.T) {
		ft := initTransportFunc(t, &mockTransport{messages: make(chan amqp.Delivery, 2)})
		ack1 := &mockAcker{}
		ack2 := &mockAcker{}
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(2),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		cons.SetHandler(testRecover{}.Wrap(func(ctx context.Context, keys []string, documents [][]byte) error {
			panic("boom")
		}))
		go cons.Consume(context.Background())
		ft.messages <- amqp.Delivery{Body: []byte("a"), RoutingKey: "rk", Acknowledger: ack1}
		ft.messages <- amqp.Delivery{Body: []byte("b"), RoutingKey: "rk", Acknowledger: ack2}
		time.Sleep(50 * time.Millisecond)
		_ = cons.Close()
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
		cons, err := NewConsumer("amqp://", "ex", "q", "rk",
			WithConsumerPageSize(5),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)
		require.NoError(t, cons.SetReconnectInterval(time.Hour))
		cons.SetHandler(func(ctx context.Context, keys []string, documents [][]byte) error {
			require.Len(t, keys, 1)
			require.Equal(t, "rk", keys[0])
			require.Equal(t, [][]byte{[]byte("only")}, documents)
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

		cons, err := NewConsumer("amqp://", "ex", "q", "rk0",
			WithConsumerPageSize(0),
			WithConsumerFlushInterval(time.Second),
		)
		require.NoError(t, err)

		done := make(chan struct{})
		cons.SetHandler(func(ctx context.Context, keys []string, documents [][]byte) error {
			require.Len(t, keys, 1)
			require.Equal(t, "rk0", keys[0])
			require.Equal(t, [][]byte{[]byte("p0")}, documents)
			s := session.FromContext(ctx)
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
