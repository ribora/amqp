package amqp

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// mockAcker implements amqp.Acknowledger
type mockAcker struct {
	mu           sync.Mutex
	ackCalls     int
	nackCalls    int
	lastMultiple bool
	lastRequeue  bool
}

func (f *mockAcker) Ack(tag uint64, multiple bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ackCalls++
	f.lastMultiple = multiple
	return nil
}

func (f *mockAcker) Nack(tag uint64, multiple bool, requeue bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nackCalls++
	f.lastMultiple = multiple
	f.lastRequeue = requeue
	return nil
}

func (f *mockAcker) Reject(tag uint64, requeue bool) error { return nil }

// mockTransport satisfies transportAPI
type mockTransport struct {
	messages      chan amqp.Delivery
	exchangeDecls []string
	consumeArgs   []struct {
		q, rk, ex string
		prefetch  uint16
	}
	published []struct {
		ex, key string
		msg     amqp.Publishing
	}
	closed bool

	exchangeErr error
	consumeErr  error
	// publishErrs provides errors for sequential PublishWithContext calls
	publishErrs    []error
	publishCallCnt int
}

func (f *mockTransport) ExchangeDeclare(exchange string) error {
	f.exchangeDecls = append(f.exchangeDecls, exchange)
	return f.exchangeErr
}

func (f *mockTransport) Consume(queue, routingKey, exchange string, prefetchCount uint16) (<-chan amqp.Delivery, error) {
	f.consumeArgs = append(f.consumeArgs, struct {
		q, rk, ex string
		prefetch  uint16
	}{queue, routingKey, exchange, prefetchCount})
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	return f.messages, nil
}

func (f *mockTransport) PublishWithContext(ctx context.Context, exchange string, id string, message amqp.Publishing) error {
	f.published = append(f.published, struct {
		ex, key string
		msg     amqp.Publishing
	}{exchange, id, message})
	if f.publishCallCnt < len(f.publishErrs) {
		err := f.publishErrs[f.publishCallCnt]
		f.publishCallCnt++
		return err
	}
	return nil
}

func (f *mockTransport) Close() error { f.closed = true; return nil }
