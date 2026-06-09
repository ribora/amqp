package amqp

import (
	"context"
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"gitlab.com/gorib/amqp/model"
	"gitlab.com/gorib/session"
)

var (
	retrySleep = time.Second
)

func NewConsumer(dsn, exchange, queue string, opts ...ConsumerOptions) (*consumer, error) {
	c := &consumer{
		stop:            make(chan struct{}),
		failsafeTimeout: 300 * time.Second,
	}
	if err := c.SetDsn(dsn); err != nil {
		return nil, err
	}
	if err := c.SetExchange(exchange); err != nil {
		return nil, err
	}
	if err := c.SetQueue(queue); err != nil {
		return nil, err
	}
	if err := c.SetExchangeKind(""); err != nil {
		return nil, err
	}
	if err := c.SetFlushInterval(5 * time.Second); err != nil {
		return nil, err
	}
	if err := c.SetPageSize(1); err != nil {
		return nil, err
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	return c, nil
}

type consumer struct {
	dsn             string
	exchange        string
	exchangeKind    string
	dlxExchange     string
	queue           string
	queueArgs       amqp.Table
	routingKeys     []string
	pageSize        uint16
	flushInterval   time.Duration
	failsafeTimeout time.Duration

	stop    chan struct{}
	stopper sync.Once
	handler func(msgs []Source) error

	tr transportAPI
}

func (c *consumer) SetHandler(handler func(msgs []Source) error) error {
	c.handler = handler
	return nil
}

func (c *consumer) SetDsn(dsn string) error {
	c.dsn = dsn
	return nil
}

func (c *consumer) SetExchange(exchange string) error {
	c.exchange = exchange
	return nil
}

func (c *consumer) SetExchangeKind(kind string) error {
	if kind == "" {
		kind = "topic"
	}
	c.exchangeKind = kind
	return nil
}

func (c *consumer) SetQueue(queue string) error {
	c.queue = queue
	return nil
}

// Bind adds a routing-key binding for the consumer's queue. Idempotent at
// the broker level (QueueBind is itself idempotent), and de-duplicated here
// so we don't do redundant network calls if the caller registers the same
// key twice. Empty key is rejected — use "#" explicitly for fan-out.
func (c *consumer) Bind(routingKey string) error {
	if routingKey == "" {
		return errors.New("routing key is empty")
	}
	for _, k := range c.routingKeys {
		if k == routingKey {
			return nil
		}
	}
	c.routingKeys = append(c.routingKeys, routingKey)
	return nil
}

func (c *consumer) SetPageSize(pageSize uint16) error {
	if pageSize == 0 {
		pageSize = 1
	}
	c.pageSize = pageSize
	return nil
}

func (c *consumer) SetFlushInterval(interval time.Duration) error {
	if interval < time.Second {
		return errors.New("flush interval must be greater or equal than 1 second")
	}
	c.flushInterval = interval
	return nil
}

func (c *consumer) SetReconnectInterval(interval time.Duration) error {
	if interval < time.Second {
		return errors.New("reconnect interval must be greater or equal than 1 second")
	}
	c.failsafeTimeout = interval
	return nil
}

func (c *consumer) setQueueArgs(args amqp.Table) error {
	c.queueArgs = args
	return nil
}

func (c *consumer) Consume(ctx context.Context) error {
	for {
		final, err := c.consume(ctx)
		if err != nil {
			return err
		}
		if final {
			return nil
		}
	}
}

func (c *consumer) consume(ctx context.Context) (_ bool, err error) {
	if c.handler == nil {
		return false, errors.New("message handler is undefined")
	}
	if c.dsn == "" || c.exchange == "" || c.queue == "" {
		return false, errors.New("invalid consumer config")
	}
	if len(c.routingKeys) == 0 {
		return false, errors.New("no routing keys bound — call Bind before Consume")
	}
	c.tr, err = newTransportFunc(c.dsn)
	if err != nil {
		return false, err
	}
	defer c.tr.Close()
	err = c.tr.ExchangeDeclare(c.exchange, c.exchangeKind)
	if err != nil {
		return false, err
	}

	if c.dlxExchange != "" {
		err = c.tr.ExchangeDeclare(c.dlxExchange, c.exchangeKind)
		if err != nil {
			return false, err
		}
	}

	var messages <-chan amqp.Delivery
	messages, err = c.tr.Consume(c.queue, c.routingKeys, c.exchange, c.pageSize, c.queueArgs)
	if err != nil {
		return false, err
	}

	batch := make([]amqp.Delivery, 0, c.pageSize)

	// failsafe timeout: force reconnect after configured duration
	deadline := time.NewTimer(c.failsafeTimeout)
	// proceed batch every n seconds
	flushTicker := time.NewTicker(c.flushInterval)
	var needExit, needReconnect, needFlush bool
	for {
		select {
		case <-c.stop:
			needExit = true
			needFlush = true
		case <-deadline.C:
			needReconnect = true
			needFlush = true
		case <-flushTicker.C:
			if c.pageSize == 1 {
				continue
			}
			needFlush = true
		case message := <-messages:
			batch = append(batch, message)
		}
		if len(batch) >= int(c.pageSize) || needFlush && len(batch) > 0 {
			lastMsg := batch[len(batch)-1]

			sources := make([]Source, 0, len(batch))
			for _, msg := range batch {
				mctx := session.NewContextWithSession(ctx, buildSession(msg))
				mctx = ContextWithHeaders(mctx, msg.Headers)
				sources = append(sources, newSource(mctx, msg))
			}

			err = c.handler(sources)
			if err != nil {
				_ = lastMsg.Nack(c.pageSize > 1, true)
				time.Sleep(retrySleep)
			} else {
				_ = lastMsg.Ack(c.pageSize > 1)
			}
			batch = batch[:0]
		}
		if needExit || needReconnect {
			return needExit, nil
		}
		needFlush = false
	}
}

func buildSession(msg amqp.Delivery) session.Session {
	s := session.Session{
		Uri:           msg.RoutingKey,
		Method:        "AMQP",
		Body:          string(msg.Body),
		CorrelationId: msg.CorrelationId,
	}
	if userAgent, ok := msg.Headers[model.UserAgentHeader].(string); ok {
		s.UserAgent = userAgent
	}
	if userLanguage, ok := msg.Headers[model.UserLanguageHeader].(string); ok {
		s.UserLanguage = userLanguage
	}
	if sourceIp, ok := msg.Headers[model.SourceIpHeader].(string); ok {
		s.SourceIp = sourceIp
	}
	if tp, ok := msg.Headers[model.TraceparentHeader].(string); ok {
		s.Traceparent = tp
	}
	if ts, ok := msg.Headers[model.TracestateHeader].(string); ok {
		s.Tracestate = ts
	}
	return s
}

func (c *consumer) Close() error {
	if c.stop != nil {
		c.stopper.Do(func() {
			close(c.stop)
		})
	}
	return nil
}
