package amqp

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type transportAPI interface {
	ExchangeDeclare(exchange string) error
	Consume(queue, routingKey, exchange string, prefetchCount uint16) (<-chan amqp.Delivery, error)
	PublishWithContext(ctx context.Context, exchange string, id string, message amqp.Publishing) error
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
	PublishWithContext(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
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

func (t *transport) ExchangeDeclare(exchange string) error {
	err := t.channel.ExchangeDeclare(exchange, "topic", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("rabbit exchange: %w", err)
	}
	return nil
}

func (t *transport) Consume(queue, routingKey, exchange string, prefetchCount uint16) (<-chan amqp.Delivery, error) {
	q, err := t.channel.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("topic queue: %w", err)
	}
	err = t.channel.QueueBind(q.Name, routingKey, exchange, false, nil)
	if err != nil {
		return nil, fmt.Errorf("topic bind: %w", err)
	}
	if prefetchCount > 1 {
		err = t.channel.Qos(int(prefetchCount), 0, false)
		if err != nil {
			return nil, fmt.Errorf("qos: %w", err)
		}
	}
	return t.channel.Consume(queue, "", false, false, false, false, nil)
}

func (t *transport) PublishWithContext(ctx context.Context, exchange string, id string, message amqp.Publishing) error {
	return t.channel.PublishWithContext(ctx, exchange, id, false, false, message)
}

func (t *transport) Close() error {
	return t.connection.Close()
}
