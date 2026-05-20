package amqp

import (
	"context"
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"gitlab.com/gorib/amqp/model"
	"gitlab.com/gorib/failure"
	"gitlab.com/gorib/hilton"
	"gitlab.com/gorib/pry"
	"gitlab.com/gorib/session"
)

var (
	retrySleep = time.Second
)

func NewConsumer(dsn, exchange, queue, routingKey string, opts ...ConsumerOptions) (*consumer, error) {
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
	if err := c.SetRoutingKey(routingKey); err != nil {
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
	routingKey      string
	pageSize        uint16
	flushInterval   time.Duration
	failsafeTimeout time.Duration

	logger pry.Logger

	stop    chan struct{}
	stopper sync.Once
	handler func(ctx context.Context, keys []string, documents [][]byte) error

	tr transportAPI
}

func (c *consumer) SetHandler(handler func(ctx context.Context, keys []string, documents [][]byte) error) error {
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

func (c *consumer) SetRoutingKey(routingKey string) error {
	if routingKey == "" {
		routingKey = "#"
	}
	c.routingKey = routingKey
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
	messages, err = c.tr.Consume(c.queue, c.routingKey, c.exchange, c.pageSize, c.queueArgs)
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
			keys := make([]string, 0, len(batch))
			documents := make([][]byte, 0, len(batch))
			for _, msg := range batch {
				keys = append(keys, msg.RoutingKey)
				documents = append(documents, msg.Body)
			}
			if c.pageSize == 1 {
				s := session.Session{
					Uri:           lastMsg.RoutingKey,
					Method:        "AMQP",
					Body:          string(lastMsg.Body),
					CorrelationId: lastMsg.CorrelationId,
				}
				if userAgent, ok := lastMsg.Headers[model.UserAgentHeader].(string); ok {
					s.UserAgent = userAgent
				}
				if userLanguage, ok := lastMsg.Headers[model.UserLanguageHeader].(string); ok {
					s.UserLanguage = userLanguage
				}
				if sourceIp, ok := lastMsg.Headers[model.SourceIpHeader].(string); ok {
					s.SourceIp = sourceIp
				}
				if tp, ok := lastMsg.Headers[model.TraceparentHeader].(string); ok {
					s.Traceparent = tp
				}
				if ts, ok := lastMsg.Headers[model.TracestateHeader].(string); ok {
					s.Tracestate = ts
				}
				ctx = session.NewContextWithSession(ctx, s)
			}
			provider := hilton.ProviderFromContext(ctx)
			// If consumer ctx had no provider, fall through with Noop().
			handlerCtx := ctx
			if sc, ok := hilton.ParseTraceparent(headerString(lastMsg.Headers, model.TraceparentHeader)); ok {
				handlerCtx = hilton.ContextWithRemoteSpanContext(handlerCtx, sc)
			}
			handlerCtx, span := provider.Tracer(instrumentationName).Start(handlerCtx, c.queue+" process",
				hilton.WithSpanKind(hilton.SpanKindConsumer),
				hilton.WithAttributes(
					hilton.StringAttr("messaging.system", "rabbitmq"),
					hilton.StringAttr("messaging.destination.name", c.exchange),
					hilton.StringAttr("messaging.rabbitmq.destination.routing_key", lastMsg.RoutingKey),
					hilton.StringAttr("messaging.operation.type", "process"),
				),
			)
			if s := session.FromContext(handlerCtx); s != nil {
				if tp := hilton.FormatTraceparent(span.SpanContext()); tp != "" {
					s.Traceparent = tp
				}
			}
			err = c.proceed(func() error {
				return c.handler(handlerCtx, keys, documents)
			})
			if err != nil {
				span.RecordError(err)
				span.SetStatus(hilton.StatusError, err.Error())
			}
			span.End()
			if err != nil {
				// All business logic should be handled in the handler.
				// So only infrastructure problems expected.
				// Let's wait for 1 second.
				if c.logger != nil {
					c.logger.Error(err, pry.Ctx(ctx))
				}
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

func (c *consumer) proceed(h func() error) (err error) {
	defer func() {
		if r := recover(); r != nil && err == nil {
			err = failure.Panic(r)
		}
	}()
	return h()
}

func headerString(h amqp.Table, key string) string {
	if v, ok := h[key].(string); ok {
		return v
	}
	return ""
}

func (c *consumer) Close() error {
	if c.stop != nil {
		c.stopper.Do(func() {
			close(c.stop)
		})
	}
	return nil
}
