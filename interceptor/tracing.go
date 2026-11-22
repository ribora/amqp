package interceptor

import (
	"context"
	"time"

	"github.com/ribora/amqp"
	"github.com/ribora/amqp/model"
	"github.com/ribora/failure"
	"github.com/ribora/hilton"
	"github.com/ribora/session"
	"github.com/ribora/whisk/scan"
)

const instrumentationName = "github.com/ribora/amqp"

// NewTracing extracts incoming W3C trace context from AMQP headers, starts a
// CONSUMER-kind span, injects provider into ctx, and mirrors traceparent into
// session. Records messaging.process.duration and messaging.client.consumed.messages (per-message,
// meaningful when the consumer batches via pageSize>1).
func NewTracing(provider hilton.Provider) *tracingMW {
	meter := provider.Meter(instrumentationName)
	return &tracingMW{
		provider: provider,
		tracer:   provider.Tracer(instrumentationName),
		duration: meter.Histogram("messaging.process.duration",
			hilton.WithDescription("Duration of AMQP message processing"),
			hilton.WithUnit("s"),
			hilton.WithSecondsBoundaries(),
		),
		messages: meter.Counter("messaging.client.consumed.messages", hilton.WithDescription("Number of AMQP messages consumed by the client")),
		active: meter.UpDownCounter("messaging.process.active",
			hilton.WithDescription("Number of AMQP messages being processed"),
			hilton.WithUnit("1"),
		),
	}
}

type tracingMW struct {
	provider hilton.Provider
	tracer   hilton.Tracer
	duration hilton.Histogram
	messages hilton.Counter
	active   hilton.UpDownCounter
}

const otherErrorType = "_OTHER"

func errorType(err error) string {
	if err == nil {
		return ""
	}
	if fe, ok := failure.FromError(err); ok {
		return fe.Type()
	}
	return otherErrorType
}

func withContext(ctx context.Context, src scan.Source) scan.Source {
	if s, ok := src.(amqp.Source); ok {
		return s.WithContext(ctx)
	}
	return src
}

// Only the last message becomes the parent, so the rest of a batch reach the
// span as links — otherwise their traces lose the connection entirely.
func batchLinks(msgs []scan.Source) []hilton.Link {
	if len(msgs) < 2 {
		return nil
	}
	links := make([]hilton.Link, 0, len(msgs)-1)
	for i, msg := range msgs[:len(msgs)-1] {
		sc, ok := hilton.ParseTraceparent(msg.Lookup("header", model.TraceparentHeader))
		if !ok {
			continue
		}
		links = append(links, hilton.Link{
			SpanContext: sc,
			Attributes:  []hilton.Attribute{hilton.Attr("messaging.batch.index", i)},
		})
	}
	return links
}

func (t *tracingMW) Wrap(next func(msgs []scan.Source) error) func(msgs []scan.Source) error {
	return func(msgs []scan.Source) error {
		if len(msgs) == 0 {
			return next(msgs)
		}

		ctx := hilton.ContextWithProvider(msgs[len(msgs)-1].Context(), t.provider)
		if headers := amqp.HeadersFromContext(ctx); headers != nil {
			ctx = t.provider.Propagator().Extract(ctx, amqp.TableCarrier(headers))
		}

		routingKey := amqp.RoutingKey(msgs[len(msgs)-1])

		ctx, span := t.tracer.Start(ctx, "amqp process",
			hilton.WithSpanKind(hilton.SpanKindConsumer),
			hilton.WithAttributes(
				hilton.Attr("messaging.system", "rabbitmq"),
				hilton.Attr("messaging.rabbitmq.destination.routing_key", routingKey),
				hilton.Attr("messaging.operation.type", "process"),
				hilton.Attr("messaging.batch.message_count", len(msgs)),
			),
			hilton.WithLinks(batchLinks(msgs)...),
		)
		if tp := hilton.FormatTraceparent(span.SpanContext()); tp != "" {
			ctx = session.WithTraceparent(ctx, tp)
		}

		for i := range msgs {
			msgs[i] = withContext(ctx, msgs[i])
		}

		activeAttrs := []hilton.Attribute{
			hilton.Attr("messaging.system", "rabbitmq"),
			hilton.Attr("messaging.operation.type", "process"),
		}
		t.active.Add(ctx, int64(len(msgs)), activeAttrs...)
		defer t.active.Add(ctx, -int64(len(msgs)), activeAttrs...) // deferred so a panic still unwinds it

		start := time.Now()
		err := next(msgs)
		took := time.Since(start).Seconds()

		attrs := []hilton.Attribute{
			hilton.Attr("messaging.system", "rabbitmq"),
			hilton.Attr("messaging.operation.type", "process"),
		}
		if err != nil {
			span.RecordError(err)
			span.SetStatus(hilton.StatusError, err.Error())
			attrs = append(attrs, hilton.Attr("error.type", errorType(err)))
		}
		span.End()

		t.duration.Record(ctx, took, attrs...)
		t.messages.Add(ctx, int64(len(msgs)), attrs...)

		return err
	}
}
