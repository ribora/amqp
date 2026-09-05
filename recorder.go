package amqp

import (
	"context"
	"sync"
)

// No exchange: it is fixed in NewPublisher, so a replaced publisher never sees it.
func NewPublisherRecorder() *PublisherRecorder { return &PublisherRecorder{} }

var (
	_ Publisher = (*publisher)(nil)
	_ Publisher = (*PublisherRecorder)(nil)
)

type Published struct {
	Key      string
	Document any
}

type PublisherRecorder struct {
	mu        sync.Mutex
	published []Published
	err       error
}

// Every Publish from now on fails, for a scenario about a broker being down.
func (r *PublisherRecorder) FailWith(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func (r *PublisherRecorder) Publish(_ context.Context, key string, document any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.published = append(r.published, Published{Key: key, Document: document})
	return nil
}

func (r *PublisherRecorder) Close() error { return nil }

func (r *PublisherRecorder) Published() []Published {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Published(nil), r.published...)
}

func (r *PublisherRecorder) WithKey(key string) []Published {
	r.mu.Lock()
	defer r.mu.Unlock()
	var found []Published
	for _, p := range r.published {
		if p.Key == key {
			found = append(found, p)
		}
	}
	return found
}

func (r *PublisherRecorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.published, r.err = nil, nil
}
