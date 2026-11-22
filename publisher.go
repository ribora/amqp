package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ribora/amqp/model"
	"github.com/ribora/hilton"
	"github.com/ribora/session"
)

const instrumentationName = "github.com/ribora/amqp"

// Some bound is needed: on a connection that dies without closing, nothing
// resolves the confirmation until the heartbeat expires, a minute and more.
const defaultConfirmTimeout = 5 * time.Second

var (
	// The broker answered, and the answer was no.
	ErrNotConfirmed = errors.New("rabbit confirm: the broker did not acknowledge the publishing")
	// The channel was never put into confirm mode, so no answer is coming.
	ErrNotConfirming = errors.New("rabbit confirm: the channel is not in confirm mode")
)

type EncodedMessage []byte

// Exists for the compile-time checks in recorder.go.
type Publisher interface {
	Publish(ctx context.Context, key string, document any) error
	Close() error
}

type PublisherOptions func(*publisher) error

func WithExchangeKind(kind string) PublisherOptions {
	return func(p *publisher) error {
		p.exchangeKind = kind
		return nil
	}
}

// Zero or less leaves the wait bounded only by the context the caller passes.
func WithConfirmTimeout(timeout time.Duration) PublisherOptions {
	return func(p *publisher) error {
		p.confirmTimeout = timeout
		return nil
	}
}

func NewPublisher(dsn, exchange string, opts ...PublisherOptions) (*publisher, error) {
	p := &publisher{
		dsn:            dsn,
		exchange:       exchange,
		exchangeKind:   "topic",
		confirmTimeout: defaultConfirmTimeout,
	}
	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil, err
		}
	}
	return p, nil
}

type publisher struct {
	dsn            string
	exchange       string
	exchangeKind   string
	confirmTimeout time.Duration

	tr       transportAPI
	trLocker sync.Mutex
}

func (r *publisher) Publish(ctx context.Context, key string, document any) (err error) {
	provider := hilton.ProviderFromContext(ctx)
	ctx, span := provider.Tracer(instrumentationName).Start(ctx, r.exchange+" publish",
		hilton.WithSpanKind(hilton.SpanKindProducer),
		hilton.WithAttributes(
			hilton.Attr("messaging.system", "rabbitmq"),
			hilton.Attr("messaging.destination.name", r.exchange),
			hilton.Attr("messaging.rabbitmq.destination.routing_key", key),
			hilton.Attr("messaging.operation.type", "publish"),
		),
	)
	defer span.End()
	if tp := hilton.FormatTraceparent(span.SpanContext()); tp != "" {
		ctx = session.WithTraceparent(ctx, tp)
	}

	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(hilton.StatusError, err.Error())
		}
	}()

	var payload []byte
	if enc, ok := document.(EncodedMessage); ok {
		payload = enc
	} else {
		payload, err = json.Marshal(document)
		if err != nil {
			return fmt.Errorf("rabbit payload: %w", err)
		}
	}
	message := amqp.Publishing{
		ContentType: "text/plain",
		Body:        payload,
	}

	if s := session.FromContext(ctx); s != nil {
		// Initialize headers map to avoid nil map assignment panics
		if message.Headers == nil {
			message.Headers = amqp.Table{}
		}
		if s.CorrelationId != "" {
			message.CorrelationId = s.CorrelationId
		}
		if s.UserAgent != "" {
			message.Headers[model.UserAgentHeader] = s.UserAgent
		}
		if s.UserLanguage != "" {
			message.Headers[model.UserLanguageHeader] = s.UserLanguage
		}
		if s.SourceIp != "" {
			message.Headers[model.SourceIpHeader] = s.SourceIp
		}
	}

	traceparent, tracestate := session.TraceFromContext(ctx)
	if traceparent != "" || tracestate != "" {
		if message.Headers == nil {
			message.Headers = amqp.Table{}
		}
		if traceparent != "" {
			message.Headers[model.TraceparentHeader] = traceparent
		}
		if tracestate != "" {
			message.Headers[model.TracestateHeader] = tracestate
		}
	}

	tr, err := r.transport()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			r.trLocker.Lock()
			defer r.trLocker.Unlock()
			if r.tr != nil {
				_ = r.tr.Close()
				r.tr = nil
			}
		}
	}()

	confirmation, err := tr.PublishWithContext(ctx, r.exchange, key, message)
	if err != nil {
		return err
	}

	wait := ctx
	if r.confirmTimeout > 0 {
		var cancel context.CancelFunc
		wait, cancel = context.WithTimeout(ctx, r.confirmTimeout)
		defer cancel()
	}
	acked, err := confirmation.WaitContext(wait)
	if err != nil {
		return fmt.Errorf("rabbit confirm: %w", err)
	}
	if !acked {
		return ErrNotConfirmed
	}
	return nil
}

func (r *publisher) transport() (transportAPI, error) {
	r.trLocker.Lock()
	defer r.trLocker.Unlock()
	if r.tr != nil {
		return r.tr, nil
	}
	tr, err := newTransportFunc(r.dsn)
	if err != nil {
		return nil, err
	}
	// Not in newTransport: the consumer shares that constructor and never publishes.
	if err = tr.Confirm(); err != nil {
		_ = tr.Close()
		return nil, err
	}
	if err = tr.ExchangeDeclare(r.exchange, r.exchangeKind); err != nil {
		_ = tr.Close()
		return nil, err
	}
	r.tr = tr
	return tr, nil
}

func (r *publisher) Close() error {
	r.trLocker.Lock()
	defer r.trLocker.Unlock()
	if r.tr != nil {
		return r.tr.Close()
	}
	return nil
}
