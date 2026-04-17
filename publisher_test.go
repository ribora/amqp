package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"gitlab.com/gorib/amqp/model"
	"gitlab.com/gorib/session"
)

func TestPublisher(t *testing.T) {
	trFunc := newTransportFunc
	defer func() {
		newTransportFunc = trFunc
	}()

	t.Run("WrongTransport", func(t *testing.T) {
		pub, err := NewPublisher("amqp://", "ex")
		require.NoError(t, err)

		newTransportFunc = func(dsn string) (transportAPI, error) { return nil, errors.New("error") }
		require.ErrorContains(t, pub.Publish(context.Background(), "id", "message"), "error")
	})

	t.Run("ExchangeDeclareFail", func(t *testing.T) {
		pub, err := NewPublisher("amqp://", "ex")
		require.NoError(t, err)

		ft := &mockTransport{messages: make(chan amqp.Delivery, 10)}
		newTransportFunc = func(dsn string) (transportAPI, error) { return ft, nil }
		ft.exchangeErr = errors.New("error")
		require.ErrorContains(t, pub.Publish(context.Background(), "id", "message"), "error")
	})

	t.Run("Object", func(t *testing.T) {
		pub, err := NewPublisher("amqp://", "ex")
		require.NoError(t, err)

		ft := &mockTransport{}
		newTransportFunc = func(dsn string) (transportAPI, error) { return ft, nil }

		src := struct {
			A string
			N int
		}{"x", 7}
		require.NoError(t, pub.Publish(context.Background(), "k1", src))
		require.Len(t, ft.exchangeDecls, 1)
		require.Len(t, ft.published, 1)
		b, _ := json.Marshal(src)
		require.Equal(t, b, ft.published[0].msg.Body)
	})

	t.Run("EncodedMessage", func(t *testing.T) {
		pub, err := NewPublisher("amqp://", "ex")
		require.NoError(t, err)

		ft := &mockTransport{}
		newTransportFunc = func(dsn string) (transportAPI, error) { return ft, nil }

		payload := EncodedMessage("raw")
		require.NoError(t, pub.Publish(context.Background(), "k2", payload))
		require.Len(t, ft.exchangeDecls, 1)
		require.Len(t, ft.published, 1)
		require.Equal(t, []byte("raw"), ft.published[0].msg.Body)
	})

	t.Run("ErrClosed", func(t *testing.T) {
		pub, err := NewPublisher("amqp://", "ex")
		require.NoError(t, err)

		var calls int
		ft := &mockTransport{}
		newTransportFunc = func(dsn string) (transportAPI, error) { calls++; return ft, nil }

		ft.publishErrs = []error{amqp.ErrClosed, nil}

		err = pub.Publish(context.Background(), "k3", map[string]int{"a": 1})
		require.ErrorIs(t, err, amqp.ErrClosed)
		require.True(t, ft.closed)

		require.NoError(t, pub.Publish(context.Background(), "k4", map[string]int{"b": 2}))
		require.Equal(t, calls, 2)
	})

	t.Run("SessionHeaders", func(t *testing.T) {
		pub, err := NewPublisher("amqp://", "ex")
		require.NoError(t, err)

		ft := &mockTransport{}
		newTransportFunc = func(dsn string) (transportAPI, error) { return ft, nil }

		s := session.Session{
			CorrelationId: "corr-42",
			UserAgent:     "ua-bot/2.0",
			UserLanguage:  "en-US,en;q=0.9",
			SourceIp:      "198.51.100.23",
		}
		ctx := session.NewContextWithSession(context.Background(), s)

		require.NoError(t, pub.Publish(ctx, "rk.session", map[string]any{"ok": true}))
		require.Len(t, ft.exchangeDecls, 1)
		require.Len(t, ft.published, 1)
		msg := ft.published[0].msg
		require.Equal(t, s.CorrelationId, msg.CorrelationId)
		require.NotNil(t, msg.Headers)
		require.Equal(t, s.UserAgent, msg.Headers[model.UserAgentHeader])
		require.Equal(t, s.UserLanguage, msg.Headers[model.UserLanguageHeader])
		require.Equal(t, s.SourceIp, msg.Headers[model.SourceIpHeader])
	})

	t.Run("MarshalError", func(t *testing.T) {
		pub, err := NewPublisher("amqp://", "ex")
		require.NoError(t, err)

		ft := &mockTransport{}
		newTransportFunc = func(dsn string) (transportAPI, error) { return ft, nil }

		type badFn = func()
		err = pub.Publish(context.Background(), "k5", badFn(func() {}))
		require.ErrorContains(t, err, "rabbit payload:")
		require.Len(t, ft.published, 0)
	})

	t.Run("CloseNilTransport", func(t *testing.T) {
		pub, err := NewPublisher("amqp://", "ex")
		require.NoError(t, err)
		require.NoError(t, pub.Close())
	})

	t.Run("Cleanup", func(t *testing.T) {
		ft := &mockTransport{}
		newTransportFunc = func(dsn string) (transportAPI, error) { return ft, nil }
		pub := &publisher{dsn: "", exchange: "ex", tr: ft}
		require.NoError(t, pub.Close())
		require.True(t, ft.closed)
	})
}
