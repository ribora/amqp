package amqp

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type headersKey struct{}

func ContextWithHeaders(ctx context.Context, h amqp.Table) context.Context {
	return context.WithValue(ctx, headersKey{}, h)
}

func HeadersFromContext(ctx context.Context) amqp.Table {
	if h, ok := ctx.Value(headersKey{}).(amqp.Table); ok {
		return h
	}
	return nil
}

type TableCarrier amqp.Table

func (c TableCarrier) Get(key string) string {
	if v, ok := c[key].(string); ok {
		return v
	}
	return ""
}
func (c TableCarrier) Set(key, value string) { c[key] = value }
func (c TableCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
