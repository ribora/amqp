package amqp

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type mockConn struct {
	closed   bool
	closeErr error
}

func (f *mockConn) Close() error { f.closed = true; return f.closeErr }

type mockChan struct {
	// Recorded calls
	exchangeDecls []struct {
		name                                  string
		kind                                  string
		durable, autoDelete, internal, noWait bool
		args                                  amqp.Table
	}
	queueDecls []struct {
		name                                   string
		durable, autoDelete, exclusive, noWait bool
		args                                   amqp.Table
	}
	queueBinds []struct {
		name, key, exchange string
		noWait              bool
		args                amqp.Table
	}
	qos []struct {
		prefetchCount, prefetchSize int
		global                      bool
	}
	consumes []struct {
		queue, consumer                     string
		autoAck, exclusive, noLocal, noWait bool
		args                                amqp.Table
	}
	publishes []struct {
		exchange, key        string
		mandatory, immediate bool
		msg                  amqp.Publishing
	}
	confirms int

	// Behavior controls
	exchangeErr     error
	queueDeclareErr error
	queueBindErr    error
	qosErr          error
	consumeErr      error
	deliveries      chan amqp.Delivery
	publishErr      error
	confirmErr      error
	notConfirming   bool
}

func (c *mockChan) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	c.exchangeDecls = append(c.exchangeDecls, struct {
		name, kind                            string
		durable, autoDelete, internal, noWait bool
		args                                  amqp.Table
	}{name, kind, durable, autoDelete, internal, noWait, args})
	return c.exchangeErr
}
func (c *mockChan) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	c.queueDecls = append(c.queueDecls, struct {
		name                                   string
		durable, autoDelete, exclusive, noWait bool
		args                                   amqp.Table
	}{name, durable, autoDelete, exclusive, noWait, args})
	if c.queueDeclareErr != nil {
		return amqp.Queue{}, c.queueDeclareErr
	}
	return amqp.Queue{Name: name}, nil
}
func (c *mockChan) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	c.queueBinds = append(c.queueBinds, struct {
		name, key, exchange string
		noWait              bool
		args                amqp.Table
	}{name, key, exchange, noWait, args})
	return c.queueBindErr
}
func (c *mockChan) Qos(prefetchCount, prefetchSize int, global bool) error {
	c.qos = append(c.qos, struct {
		prefetchCount, prefetchSize int
		global                      bool
	}{prefetchCount, prefetchSize, global})
	return c.qosErr
}
func (c *mockChan) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	c.consumes = append(c.consumes, struct {
		queue, consumer                     string
		autoAck, exclusive, noLocal, noWait bool
		args                                amqp.Table
	}{queue, consumer, autoAck, exclusive, noLocal, noWait, args})
	if c.consumeErr != nil {
		return nil, c.consumeErr
	}
	if c.deliveries == nil {
		c.deliveries = make(chan amqp.Delivery)
	}
	return c.deliveries, nil
}
func (c *mockChan) Confirm(noWait bool) error {
	c.confirms++
	return c.confirmErr
}
func (c *mockChan) PublishWithDeferredConfirmWithContext(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	c.publishes = append(c.publishes, struct {
		exchange, key        string
		mandatory, immediate bool
		msg                  amqp.Publishing
	}{exchange, key, mandatory, immediate, msg})
	if c.publishErr != nil {
		return nil, c.publishErr
	}
	if c.notConfirming {
		return nil, nil
	}
	// done is unexported, so it stays nil and this confirmation can never resolve.
	// Waiting is covered through mockConfirmation.
	return &amqp.DeferredConfirmation{DeliveryTag: uint64(len(c.publishes))}, nil
}
