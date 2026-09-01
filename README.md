# gorib/amqp

RabbitMQ for gorib services: a consumer that hands batches of messages to one
handler, a publisher that reuses its connection, and an interceptor chain
around both. Session and W3C trace context travel in the message headers, so a
delivery is a continuation of the HTTP request that caused it.

For the service side — declaring handlers, binding them to routing keys,
running the consumer as a command — see `docs/tools/scribus.md` in
`gitlab.com/gorib/waffle`. This file documents the package itself.

| Path | What's in it |
|---|---|
| `.` | consumer, publisher, transport, `Source`, header carrier |
| `interceptor` | tracing, logging and recovery middlewares for the consumer |
| `model` | header names and queue-argument builder |

## Consumer

```go
c, err := amqp.NewConsumer(dsn, "orders", "orders.pricing",
	amqp.WithConsumerPageSize(50),
	amqp.WithConsumerFlushInterval(2*time.Second),
)
_ = c.Bind("order.created")
_ = c.Bind("order.updated")
_ = c.SetHandler(func(msgs []amqp.Source) error { … })
err = c.Consume(ctx)
```

Defaults: `topic` exchange, page size 1, flush interval 5s, failsafe timeout
300s. Both the exchange and the queue are declared durable.

`Bind` is required — `Consume` refuses to start without at least one routing
key, because a queue bound to nothing silently receives nothing. Keys are
de-duplicated locally; `""` is rejected, so fan-out has to be spelled `#`.
Options that take a duration reject anything under a second.

### Batching

The loop accumulates deliveries until one of three things happens: the batch
reaches `pageSize`, the flush ticker fires, or the consumer is stopping. With
`pageSize == 1` the ticker is ignored — a single message is dispatched as soon
as it arrives, and the flush interval only matters for batches.

`Qos` is set only when the page size is above 1; at 1 the broker's default
prefetch applies.

### Acknowledgement

The batch is acknowledged through its **last** delivery, with `multiple` set
when the page size is above 1 — one `Ack` settles everything the channel has
delivered so far. On a handler error the same delivery is `Nack`ed with
`requeue: true`, followed by a one-second pause before the loop continues.

Two consequences to design around:

- the batch is all-or-nothing. A handler that fails on the fifth of fifty
  messages returns all fifty to the queue, and the first four will be seen
  again. Handlers must be idempotent.
- a permanently failing message requeues forever. Bound it at the broker with
  `DeliveryLimit` and a dead-letter exchange rather than in the handler.

### Reconnection

`Consume` reconnects on a timer: every `failsafeTimeout` the connection is torn
down and rebuilt, flushing whatever is in the batch first. This is a liveness
guard, not error handling — it recovers a connection that has gone quiet
without reporting an error. `Close` stops the loop for good (idempotent, via
`sync.Once`), and `Consume` returns nil.

### Queue arguments

`WithConsumerQueueArguments` takes a `QueueConfig` (alias of `model.Queue`) and
turns it into the broker's `x-` arguments:

```go
amqp.WithConsumerQueueArguments(amqp.QueueConfig{
	Type:                 "quorum",
	DeadLetterExchange:   "orders.dlx",
	DeadLetterRoutingKey: "order.failed",
	DeliveryLimit:        ptr(int32(5)),
	MessageTTL:           ptr(time.Hour),
})
```

Durations are emitted as milliseconds, `Type` defaults to `classic`, and
`CustomArgs` is merged last so it can override anything. Setting
`DeadLetterExchange` also makes the consumer declare that exchange — a DLX that
does not exist would make the broker drop dead-lettered messages instead of
routing them.

## Source

A delivery reaches the handler as a `Source`, which is a `whisk/scan.Source`
plus the AMQP specifics:

```go
type Source interface {
	scan.Source // Body() []byte, Context() context.Context, Lookup, LookupAll

	RoutingKey() string
	Exchange() string
	Header(key string) string

	WithContext(ctx context.Context) Source
}
```

That is what lets a generated binder read an AMQP message the same way it reads
an HTTP request. The scopes `whisk` can address are `header`, `routing-key`
and `exchange`; a non-string header value reads as empty.

`WithContext` returns a copy — interceptors replace the sources in the slice
rather than mutating them.

## Interceptors

The consumer itself knows nothing about middleware: it calls one handler. The
chain is assembled by whoever wires it (waffle's scribus tool collects them by
DI tag), sorted by `Order()` ascending, smaller being further out.

| Middleware | Order | What |
|---|---|---|
| `interceptor.NewTracing` | 100 | extracts trace context from headers, starts a `CONSUMER` span, puts the provider in ctx, records `messaging.process.duration`, `messaging.client.consumed.messages` and `messaging.process.active` |
| `interceptor.NewLog` | 400 | puts the logger in ctx, traces every message body in, logs a handler error out |
| `interceptor.NewRecover` | 500 | `failure.Catch` around the handler, so a panic becomes an error the consumer can nack |

Recovery is innermost on purpose: the panic has to become an error before
tracing and logging see the result, otherwise the span would end unrecorded
and the process would die.

Batch-aware by design — the tracing span covers the whole batch and counts it
as `len(msgs)` messages, and the active-calls counter unwinds in a `defer`, so
a panic cannot leak the gauge.

## Publisher

```go
p, _ := amqp.NewPublisher(dsn, "orders")
err := p.Publish(ctx, "order.created", event)
```

The transport is created on first publish and kept; if a publish fails the
transport is closed and dropped, so the next call redials. The exchange is
declared when the connection is established.

`document` is JSON-marshalled unless it is an `amqp.EncodedMessage`, which is
written through untouched — for a payload that is already encoded, or encoded
by something other than `encoding/json`.

The span is `PRODUCER` kind, and its traceparent is mirrored back into the
session before the headers are built, so the consumer downstream becomes a
child of the publish rather than of the incoming request. Session fields go out
as `User-Agent`, `Accept-Language`, `Source-Ip`, `traceparent`, `tracestate`
(see `model`), with the correlation id in AMQP's own `CorrelationId` property.

## What it does not do

- No publisher confirms, no mandatory/immediate routing, no returns handling.
  A publish that the broker cannot route is silently lost.
- No per-message acknowledgement — the unit of settlement is the batch.
- No topology beyond one exchange, one queue and its bindings per consumer.
  Overlapping routing keys are the caller's problem (scribus rejects them).
- No reconnect on connection failure, only on the timer; an error from
  `Consume` propagates and the caller decides.
- `ContentType` is always `text/plain`, even for JSON bodies.

## Tests

| File | What it pins down |
|---|---|
| `consumer_test.go` | batching and flush behaviour, ack/nack, binding validation, reconnect |
| `publisher_test.go` | payload encoding, session headers, transport reuse and drop-on-error |
| `transport_test.go` | declare/bind/qos calls against a mocked channel |
