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
Consumer options that take a duration reject anything under a second.

### Batching

A page is for work that is cheaper in bulk — one multi-row insert, one bulk
index request, one aggregated write. Raising `pageSize` to fetch fifty messages
and then loop over them one at a time is the wrong use: the page settles as a
unit, so one bad message requeues the other forty-nine, and every message waits
for the page to fill or the ticker to fire before it is touched. If the work is
per-message, leave the page size at 1.

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

It also reconnects when the broker closes the delivery channel, which is how
amqp091 reports a dropped connection. The pending batch is dropped rather than
flushed: those deliveries are redelivered on the new channel, and handling them
first would double-process them.

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

Batch-aware by design — the tracing span covers the whole batch, counts it as
`len(msgs)` messages and records that count as `messaging.batch.message_count`,
and the active-calls counter unwinds in a `defer`, so a panic cannot leak the
gauge.

A batch has many causes and a span has one parent, so the parent comes from the
last message and every other message that carries a `traceparent` is attached as
a span link tagged `messaging.batch.index`. With `pageSize == 1` there are no
links and the span looks exactly as it did before.

## Publisher

```go
p, _ := amqp.NewPublisher(dsn, "orders")
err := p.Publish(ctx, "order.created", event)
```

The transport is created on first publish and kept; if a publish fails the
transport is closed and dropped, so the next call redials. The channel is put
into confirm mode and the exchange is declared when the connection is
established — a transport whose setup fails is closed rather than cached.

`Publish` returns once the broker has confirmed the publishing, not once the
frame is on the socket. A negative confirmation is `ErrNotConfirmed`, and a wait
that runs out returns the context's error wrapped as `rabbit confirm:`. Both drop
the transport, the same way a publish error does, because the confirmation is
bound to the channel: one that stopped answering will not answer the next publish
either. `ErrNotConfirming` means the channel was never put into confirm mode —
amqp091 signals that by returning a nil confirmation and no error, which would
otherwise read as success.

The wait is bounded by `WithConfirmTimeout`, five seconds by default, or by the
caller's context when that expires first. Zero disables the publisher's own bound
and leaves only the context. Some bound is needed: on a connection that dies
without closing, nothing resolves the confirmation until the heartbeat expires.

`document` is JSON-marshalled unless it is an `amqp.EncodedMessage`, which is
written through untouched — for a payload that is already encoded, or encoded
by something other than `encoding/json`.

The span is `PRODUCER` kind, and its traceparent goes onto the publish
context before the headers are built, so the consumer downstream becomes a
child of the publish rather than of the incoming request. That context is local
to the call, so the caller's own next outgoing call is still parented to the
request. Session fields go out as `User-Agent`, `Accept-Language`, `Source-Ip`,
and the trace context as `traceparent`, `tracestate` (see `model`), with the
correlation id in AMQP's own `CorrelationId` property.

## What it does not do

- No mandatory/immediate routing and no returns handling. A confirm says the
  broker accepted the publishing, not that it reached a queue: an unroutable
  message is confirmed and dropped, and nothing here notices.
- Settlement is per page, not per message: `Ack`/`Nack` carry AMQP's `multiple`
  flag as soon as a page holds more than one message. The default page size is 1,
  so per-message settlement is what you get until `WithConsumerPageSize` raises it.
- A failed acknowledgement is not reported: `Ack`/`Nack` errors are discarded, so
  a settlement the broker refused looks exactly like one it accepted.
- No topology beyond one exchange, one queue and its bindings per consumer.
  Overlapping routing keys are the caller's problem (scribus rejects them).
- No reconnect on an error from `Consume`; that propagates and the caller
  decides. The loop reconnects only on the timer and on a closed delivery
  channel.
- `ContentType` is always `text/plain`, even for JSON bodies.

## Tests

| File | What it pins down |
|---|---|
| `consumer_test.go` | batching and flush behaviour, ack/nack, binding validation, reconnect on the timer and on a closed channel |
| `publisher_test.go` | payload encoding, session headers, transport reuse, drop-on-error, and the confirm wait |
| `transport_test.go` | declare/bind/qos calls against a mocked channel, and confirm mode |
| `delivery_test.go` | the scopes a `Source` answers, `WithContext` copying rather than mutating, and the publisher recorder |
| `interceptor/tracing_test.go` | the `error.type` attribute on `messaging.process.duration`: a `failure` type, `panic` through `NewRecover`, and the `_OTHER` fallback |
