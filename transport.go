package amqp

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Confirmation is the broker's answer to one publishing.
type Confirmation interface {
	WaitContext(ctx context.Context) (acked bool, err error)
}

type transportAPI interface {
	ExchangeDeclare(exchange, kind string) error
	Confirm() error
	Consume(queue string, routingKeys []string, exchange string, prefetchCount uint16, args amqp.Table) (<-chan amqp.Delivery, error)
	PublishWithContext(ctx context.Context, exchange string, id string, message amqp.Publishing) (Confirmation, error)
	Close() error
}

var newTransportFunc = func(dsn string) (transportAPI, error) {
	return newTransport(dsn)
}

type amqpConnection interface {
	Close() error
}

type amqpChannel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Confirm(noWait bool) error
	PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error)
}

func newTransport(dsn string) (*transport, error) {
	connection, err := amqp.Dial(dsn)
	if err != nil {
		return nil, fmt.Errorf("rabbit conn: %w", err)
	}

	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("rabbit channel: %w", err)
	}

	return &transport{
		connection: connection,
		channel:    channel,
	}, nil
}

type transport struct {
	connection amqpConnection
	channel    amqpChannel
}

func (t *transport) ExchangeDeclare(exchange, kind string) error {
	if kind == "" {
		kind = "topic"
	}
	err := t.channel.ExchangeDeclare(exchange, kind, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("rabbit exchange: %w", err)
	}
	return nil
}

func (t *transport) Consume(queue string, routingKeys []string, exchange string, prefetchCount uint16, args amqp.Table) (<-chan amqp.Delivery, error) {
	q, err := t.channel.QueueDeclare(queue, true, false, false, false, args)
	if err != nil {
		return nil, fmt.Errorf("topic queue: %w", err)
	}
	for _, rk := range routingKeys {
		if err := t.channel.QueueBind(q.Name, rk, exchange, false, nil); err != nil {
			return nil, fmt.Errorf("topic bind %q: %w", rk, err)
		}
	}
	if prefetchCount > 1 {
		err = t.channel.Qos(int(prefetchCount), 0, false)
		if err != nil {
			return nil, fmt.Errorf("qos: %w", err)
		}
	}
	return t.channel.Consume(queue, "", false, false, false, false, nil)
}

func (t *transport) Confirm() error {
	if err := t.channel.Confirm(false); err != nil {
		return fmt.Errorf("rabbit confirm mode: %w", err)
	}
	return nil
}

func (t *transport) PublishWithContext(ctx context.Context, exchange string, id string, message amqp.Publishing) (Confirmation, error) {
	confirmation, err := t.channel.PublishWithDeferredConfirmWithContext(ctx, exchange, id, false, false, message)
	if err != nil {
		return nil, err
	}
	// amqp091 returns nil with no error when the channel is not in confirm mode.
	if confirmation == nil {
		return nil, ErrNotConfirming
	}
	return confirmation, nil
}

func (t *transport) Close() error {
	return t.connection.Close()
}
