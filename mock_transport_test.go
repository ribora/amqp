package amqp

import (
	"context"
	"strings"
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

	confirmCalls int

	exchangeErr error
	consumeErr  error
	confirmErr  error
	// publishErrs provides errors for sequential PublishWithContext calls
	publishErrs []error
	// confirmations answers sequential PublishWithContext calls; past its end the
	// broker acks.
	confirmations  []Confirmation
	publishCallCnt int
}

func (f *mockTransport) ExchangeDeclare(exchange, kind string) error {
	f.exchangeDecls = append(f.exchangeDecls, exchange)
	return f.exchangeErr
}

func (f *mockTransport) Confirm() error {
	f.confirmCalls++
	return f.confirmErr
}

func (f *mockTransport) Consume(queue string, routingKeys []string, exchange string, prefetchCount uint16, args amqp.Table) (<-chan amqp.Delivery, error) {
	f.consumeArgs = append(f.consumeArgs, struct {
		q, rk, ex string
		prefetch  uint16
	}{queue, strings.Join(routingKeys, ","), exchange, prefetchCount})
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	return f.messages, nil
}

func (f *mockTransport) PublishWithContext(ctx context.Context, exchange string, id string, message amqp.Publishing) (Confirmation, error) {
	f.published = append(f.published, struct {
		ex, key string
		msg     amqp.Publishing
	}{exchange, id, message})
	call := f.publishCallCnt
	f.publishCallCnt++
	if call < len(f.publishErrs) && f.publishErrs[call] != nil {
		return nil, f.publishErrs[call]
	}
	if call < len(f.confirmations) {
		return f.confirmations[call], nil
	}
	return &mockConfirmation{acked: true}, nil
}

func (f *mockTransport) Close() error { f.closed = true; return nil }

// mockConfirmation satisfies Confirmation. With block set it never resolves on its
// own, which is the partitioned-connection case the wait has to bound.
type mockConfirmation struct {
	acked bool
	block bool
}

func (c *mockConfirmation) WaitContext(ctx context.Context) (bool, error) {
	if c.block {
		<-ctx.Done()
		return false, ctx.Err()
	}
	return c.acked, nil
}
