package amqp

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"

	"gitlab.com/gorib/whisk/scan"
)

type Source interface {
	scan.Source

	RoutingKey() string
	Exchange() string
	Header(key string) string

	WithContext(ctx context.Context) Source
}

func newSource(ctx context.Context, d amqp.Delivery) Source {
	return &deliverySource{ctx: ctx, d: d}
}

type deliverySource struct {
	ctx context.Context
	d   amqp.Delivery
}

func (s *deliverySource) Body() []byte             { return s.d.Body }
func (s *deliverySource) Context() context.Context { return s.ctx }
func (s *deliverySource) RoutingKey() string       { return s.d.RoutingKey }
func (s *deliverySource) Exchange() string         { return s.d.Exchange }

func (s *deliverySource) WithContext(ctx context.Context) Source {
	cp := *s
	cp.ctx = ctx
	return &cp
}

func (s *deliverySource) Header(key string) string {
	if v, ok := s.d.Headers[key].(string); ok {
		return v
	}
	return ""
}

func (s *deliverySource) Lookup(scope, key string) string {
	return lookup(s, scope, key)
}

func (s *deliverySource) LookupAll(scope, key string) []string {
	return lookupAll(s, scope, key)
}

// One place for the scopes: a hand-assembled Source and one wrapping a delivery
// must answer identically, or a scope added to one leaves the other short.
type scoped interface {
	Header(key string) string
	RoutingKey() string
	Exchange() string
}

func lookup(s scoped, scope, key string) string {
	switch scope {
	case "header":
		return s.Header(key)
	case "routing-key":
		return s.RoutingKey()
	case "exchange":
		return s.Exchange()
	}
	return ""
}

func lookupAll(s scoped, scope, key string) []string {
	if v := lookup(s, scope, key); v != "" {
		return []string{v}
	}
	return nil
}
