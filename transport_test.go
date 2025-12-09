package rabbit

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// Test that dialing a clearly invalid DSN fails and the error is wrapped
// with the expected "rabbit conn:" prefix from newTransport.
func TestTransport(t *testing.T) {
	t.Run("DialError", func(t *testing.T) {
		// Use an unroutable localhost port to avoid external DNS and guarantee failure fast.
		dsn := "amqp://127.0.0.1:1/"
		_, err := newTransportFunc(dsn)
		require.ErrorContains(t, err, "rabbit conn:")
	})

	t.Run("ExchangeDeclareErr", func(t *testing.T) {
		fc := &mockConn{}
		ch := &mockChan{}
		tr := &transport{connection: fc, channel: ch}
		ch.exchangeErr = errors.New("boom")

		err := tr.ExchangeDeclare("exB")
		require.ErrorContains(t, err, "rabbit exchange:")
	})

	t.Run("ExchangeDeclare", func(t *testing.T) {
		fc := &mockConn{}
		ch := &mockChan{}
		tr := &transport{connection: fc, channel: ch}

		require.NoError(t, tr.ExchangeDeclare("exA"))
		require.Len(t, ch.exchangeDecls, 1)
		require.Equal(t, "exA", ch.exchangeDecls[0].name)
		require.Equal(t, "topic", ch.exchangeDecls[0].kind)
		require.True(t, ch.exchangeDecls[0].durable)
	})

	t.Run("CloseErr", func(t *testing.T) {
		fc := &mockConn{closeErr: errors.New("cclose")}
		ch := &mockChan{}
		tr := &transport{connection: fc, channel: ch}
		require.Error(t, tr.Close())
	})

	t.Run("Close", func(t *testing.T) {
		fc := &mockConn{}
		ch := &mockChan{}
		tr := &transport{connection: fc, channel: ch}
		require.NoError(t, tr.Close())
		require.True(t, fc.closed)
	})

	t.Run("Publish", func(t *testing.T) {
		fc := &mockConn{}
		ch := &mockChan{}
		tr := &transport{connection: fc, channel: ch}

		ctx := context.Background()
		msg := amqp.Publishing{ContentType: "text/plain", Body: []byte("hello")}
		require.NoError(t, tr.PublishWithContext(ctx, "ex", "key", msg))
		require.Len(t, ch.publishes, 1)
		p := ch.publishes[0]
		require.Equal(t, "ex", p.exchange)
		require.Equal(t, "key", p.key)
		require.False(t, p.mandatory)
		require.False(t, p.immediate)
		require.Equal(t, msg, p.msg)
	})

	t.Run("QueueDeclareError", func(t *testing.T) {
		fc := &mockConn{}
		ch := &mockChan{}
		tr := &transport{connection: fc, channel: ch}

		ch.queueDeclareErr = errors.New("qdecl")
		_, err := tr.Consume("q1", "rk", "ex", 7)
		require.ErrorContains(t, err, "topic queue:")
	})

	t.Run("QueueBindError", func(t *testing.T) {
		fc := &mockConn{}
		ch := &mockChan{}
		tr := &transport{connection: fc, channel: ch}

		ch.queueBindErr = errors.New("qbind")
		_, err := tr.Consume("q2", "rk2", "ex2", 3)
		require.ErrorContains(t, err, "topic bind:")
	})

	t.Run("Consume", func(t *testing.T) {
		fc := &mockConn{}
		ch := &mockChan{}
		tr := &transport{connection: fc, channel: ch}

		ch.deliveries = make(chan amqp.Delivery)
		_, err := tr.Consume("q3", "rk3", "ex3", 5)
		require.NoError(t, err)

		// Verify QoS and Consume were called with expected args
		require.Len(t, ch.qos, 1)
		require.Equal(t, 5, ch.qos[0].prefetchCount)
		require.Equal(t, 0, ch.qos[0].prefetchSize)
		require.False(t, ch.qos[0].global)

		require.Len(t, ch.consumes, 1)
		require.Equal(t, "q3", ch.consumes[0].queue)
		require.Equal(t, "", ch.consumes[0].consumer)
		require.False(t, ch.consumes[0].autoAck)
		require.False(t, ch.consumes[0].exclusive)
		require.False(t, ch.consumes[0].noLocal)
		require.False(t, ch.consumes[0].noWait)
	})

	t.Run("QosError", func(t *testing.T) {
		fc := &mockConn{}
		ch := &mockChan{}
		tr := &transport{connection: fc, channel: ch}

		ch.qosErr = errors.New("qosfail")
		_, err := tr.Consume("q4", "rk4", "ex4", 9)
		require.ErrorContains(t, err, "qos:")
	})
}
