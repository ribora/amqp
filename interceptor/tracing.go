package interceptor

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/gorib/amqp"
	"gitlab.com/gorib/hilton"
	"gitlab.com/gorib/session"
)

const instrumentationName = "gitlab.com/gorib/amqp"

// NewTracing extracts incoming W3C trace context from AMQP headers, starts a
// CONSUMER-kind span, injects provider into ctx, and mirrors traceparent into
// session. Records messaging.process.duration and messaging.client.consumed.messages (per-message,
// meaningful when the consumer batches via pageSize>1).
func NewTracing(provider hilton.Provider) *tracingMW {
	meter := provider.Meter(instrumentationName)
	return &tracingMW{
		provider: provider,
		tracer:   provider.Tracer(instrumentationName),
		duration: meter.Histogram("messaging.process.duration", hilton.WithDescription("Duration of AMQP message processing"), hilton.WithUnit("s")),
		messages: meter.Counter("messaging.client.consumed.messages", hilton.WithDescription("Number of AMQP messages consumed by the client")),
	}
}

type tracingMW struct {
	provider hilton.Provider
	tracer   hilton.Tracer
	duration hilton.Histogram
	messages hilton.Counter
}

func (tracingMW) Name() string { return "tracing" }
func (tracingMW) Order() int   { return 100 }

func (t *tracingMW) Wrap(next func(ctx context.Context, keys []string, documents [][]byte) error) func(ctx context.Context, keys []string, documents [][]byte) error {
	return func(ctx context.Context, keys []string, documents [][]byte) error {
		ctx = hilton.ContextWithProvider(ctx, t.provider)
		if headers := amqp.HeadersFromContext(ctx); headers != nil {
			ctx = t.provider.Propagator().Extract(ctx, amqp.TableCarrier(headers))
		}

		routingKey := ""
		if len(keys) > 0 {
			routingKey = keys[len(keys)-1]
		}

		ctx, span := t.tracer.Start(ctx, "amqp process",
			hilton.WithSpanKind(hilton.SpanKindConsumer),
			hilton.WithAttributes(
				hilton.Attr("messaging.system", "rabbitmq"),
				hilton.Attr("messaging.rabbitmq.destination.routing_key", routingKey),
				hilton.Attr("messaging.operation.type", "process"),
			),
		)
		if s := session.FromContext(ctx); s != nil {
			if tp := hilton.FormatTraceparent(span.SpanContext()); tp != "" {
				s.Traceparent = tp
			}
		}

		start := time.Now()
		err := next(ctx, keys, documents)
		took := time.Since(start).Seconds()

		attrs := []hilton.Attribute{
			hilton.Attr("messaging.system", "rabbitmq"),
			hilton.Attr("messaging.operation.type", "process"),
		}
		if err != nil {
			span.RecordError(err)
			span.SetStatus(hilton.StatusError, err.Error())
			attrs = append(attrs, hilton.Attr("error.type", fmt.Sprintf("%T", err)))
		}
		span.End()

		t.duration.Record(ctx, took, attrs...)
		t.messages.Add(ctx, int64(len(documents)), attrs...)

		return err
	}
}
