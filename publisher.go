package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"gitlab.com/gorib/amqp/model"
	"gitlab.com/gorib/session"
)

type EncodedMessage []byte

type PublisherOptions func(*publisher) error

func WithExchangeKind(kind string) PublisherOptions {
	return func(p *publisher) error {
		p.exchangeKind = kind
		return nil
	}
}

func NewPublisher(dsn, exchange string, opts ...PublisherOptions) (*publisher, error) {
	p := &publisher{
		dsn:          dsn,
		exchange:     exchange,
		exchangeKind: "topic",
	}
	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil, err
		}
	}
	return p, nil
}

type publisher struct {
	dsn          string
	exchange     string
	exchangeKind string

	tr       transportAPI
	trLocker sync.Mutex
}

func (r *publisher) Publish(ctx context.Context, key string, document any) (err error) {

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
		if s.Traceparent != "" {
			message.Headers[model.TraceparentHeader] = s.Traceparent
		}
		if s.Tracestate != "" {
			message.Headers[model.TracestateHeader] = s.Tracestate
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

	return tr.PublishWithContext(ctx, r.exchange, key, message)
}

func (r *publisher) transport() (_ transportAPI, err error) {
	r.trLocker.Lock()
	defer r.trLocker.Unlock()
	if r.tr != nil {
		return r.tr, nil
	}
	r.tr, err = newTransportFunc(r.dsn)
	if err != nil {
		return nil, err
	}
	err = r.tr.ExchangeDeclare(r.exchange, r.exchangeKind)
	return r.tr, err
}

func (r *publisher) Close() error {
	r.trLocker.Lock()
	defer r.trLocker.Unlock()
	if r.tr != nil {
		return r.tr.Close()
	}
	return nil
}
