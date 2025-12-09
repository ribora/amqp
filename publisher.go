package rabbit

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

func NewPublisher(dsn, exchange string) *publisher {
	return &publisher{
		dsn:      dsn,
		exchange: exchange,
	}
}

type publisher struct {
	dsn      string
	exchange string

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
			return fmt.Errorf("rabbit payload: %v", err)
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

	tr, err := r.transport()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = r.Close()
			r.tr = nil
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
	err = r.tr.ExchangeDeclare(r.exchange)
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
