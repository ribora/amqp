package model_test

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ribora/amqp/model"
)

// The table is declared to the broker, where a wrong or extra argument fails
// the declaration against an existing queue, so the exact key set matters.
func TestQueueTableOnAZeroValue(t *testing.T) {
	assert.Equal(
		t,
		amqp.Table{model.QueueType: model.QueueTypeClassic},
		model.Queue{}.Table(),
	)
}

func TestQueueTableKeepsAnExplicitType(t *testing.T) {
	assert.Equal(
		t,
		amqp.Table{model.QueueType: "quorum"},
		model.Queue{Type: "quorum"}.Table(),
	)
}

// Table maps fields, it does not validate them: "lazy" is meaningless on a
// quorum queue and the broker, not this code, is what rejects the combination.
func TestQueueTableWithEveryFieldSet(t *testing.T) {
	q := model.Queue{
		Type:                 "quorum",
		MessageTTL:           new(30 * time.Second),
		AutoExpire:           new(2 * time.Hour),
		MaxPriority:          new(int32(5)),
		MaxLength:            new(int32(1000)),
		MaxLengthBytes:       new(int32(1 << 20)),
		Overflow:             "reject-publish",
		DeadLetterExchange:   "orders.dlx",
		DeadLetterRoutingKey: "orders.dead",
		LazyMode:             true,
		DeliveryLimit:        new(int32(3)),
		InitialGroupSize:     new(int32(3)),
		CustomArgs:           amqp.Table{"x-single-active-consumer": true},
	}

	assert.Equal(
		t,
		amqp.Table{
			model.QueueType:                 "quorum",
			model.QueueMessageTTL:           int32(30_000),
			model.QueueExpires:              int32(7_200_000),
			model.QueueMaxPriority:          int32(5),
			model.QueueMaxLength:            int32(1000),
			model.QueueMaxBytes:             int32(1 << 20),
			model.QueueOverflow:             "reject-publish",
			model.QueueDeadLetterExchange:   "orders.dlx",
			model.QueueDeadLetterRoutingKey: "orders.dead",
			model.QueueMode:                 model.QueueModeLazy,
			model.QueueDeliveryLimit:        int32(3),
			model.QueueInitialGroupSize:     int32(3),
			"x-single-active-consumer":      true,
		},
		q.Table(),
	)
}

// A nil pointer means "unset", so the key is absent rather than zero — the
// broker's own default for that argument is what should apply.
func TestQueueTableOmitsNilPointers(t *testing.T) {
	table := model.Queue{Type: "classic"}.Table()

	for _, key := range []string{
		model.QueueMessageTTL,
		model.QueueExpires,
		model.QueueMaxPriority,
		model.QueueMaxLength,
		model.QueueMaxBytes,
		model.QueueOverflow,
		model.QueueDeadLetterExchange,
		model.QueueDeadLetterRoutingKey,
		model.QueueMode,
		model.QueueDeliveryLimit,
		model.QueueInitialGroupSize,
	} {
		assert.NotContains(t, table, key)
	}
}

// A pointer to zero is the point of the pointer: x-max-length 0 and "no limit"
// are different declarations.
func TestQueueTableKeepsAPointerToZero(t *testing.T) {
	table := model.Queue{
		MaxLength:     new(int32(0)),
		DeliveryLimit: new(int32(0)),
		MessageTTL:    new(time.Duration(0)),
	}.Table()

	assert.Equal(t, int32(0), table[model.QueueMaxLength])
	assert.Equal(t, int32(0), table[model.QueueDeliveryLimit])
	assert.Equal(t, int32(0), table[model.QueueMessageTTL])
}

// The routing key is nested under the exchange: on its own it would name a
// destination nothing routes to, so it is dropped rather than declared.
func TestQueueTableDropsADeadLetterRoutingKeyWithoutAnExchange(t *testing.T) {
	table := model.Queue{DeadLetterRoutingKey: "orders.dead"}.Table()

	assert.NotContains(t, table, model.QueueDeadLetterRoutingKey)
	assert.NotContains(t, table, model.QueueDeadLetterExchange)
}

func TestQueueTableAllowsADeadLetterExchangeWithoutARoutingKey(t *testing.T) {
	table := model.Queue{DeadLetterExchange: "orders.dlx"}.Table()

	assert.Equal(t, "orders.dlx", table[model.QueueDeadLetterExchange])
	assert.NotContains(t, table, model.QueueDeadLetterRoutingKey)
}

// AMQP argument tables carry no duration type, so a Duration becomes int32
// milliseconds.
func TestQueueTableConvertsDurationsToMilliseconds(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want int32
	}{
		{"Seconds", 90 * time.Second, 90_000},
		{"Milliseconds", 250 * time.Millisecond, 250},
		{"SubMillisecondTruncates", 1500 * time.Microsecond, 1},
		{"BelowAMillisecondBecomesZero", 999 * time.Microsecond, 0},
	}

	for _, c := range cases {
		t.Run(
			c.name,
			func(t *testing.T) {
				table := model.Queue{MessageTTL: &c.in, AutoExpire: &c.in}.Table()
				assert.Equal(t, c.want, table[model.QueueMessageTTL])
				assert.Equal(t, c.want, table[model.QueueExpires])
			},
		)
	}
}

// int32 milliseconds tops out near 24.8 days and the conversion does not check,
// so a longer TTL wraps. Pinned because the wrap is silent: the queue is
// declared with a negative argument instead of the one that was asked for.
func TestQueueTableWrapsADurationBeyondInt32(t *testing.T) {
	table := model.Queue{MessageTTL: new(25 * 24 * time.Hour)}.Table()

	ttl, ok := table[model.QueueMessageTTL].(int32)
	require.True(t, ok)
	assert.Negative(t, ttl)
}

// CustomArgs is merged last, which makes it the escape hatch for an argument
// this struct has no field for — including overriding one it does.
func TestQueueTableCustomArgsWinOverComputedKeys(t *testing.T) {
	table := model.Queue{
		Type:      "classic",
		MaxLength: new(int32(10)),
		CustomArgs: amqp.Table{
			model.QueueType:      "quorum",
			model.QueueMaxLength: int32(99),
			"x-consumer-timeout": int32(1000),
		},
	}.Table()

	assert.Equal(t, "quorum", table[model.QueueType])
	assert.Equal(t, int32(99), table[model.QueueMaxLength])
	assert.Equal(t, int32(1000), table["x-consumer-timeout"])
}

// The consumer hands the result straight to the broker and may reuse the
// config, so Table must build a fresh map and never alias CustomArgs.
func TestQueueTableReturnsAFreshMapEachCall(t *testing.T) {
	custom := amqp.Table{"x-consumer-timeout": int32(1000)}
	q := model.Queue{CustomArgs: custom}

	first := q.Table()
	first[model.QueueType] = "tampered"
	first["x-consumer-timeout"] = int32(7)

	second := q.Table()
	assert.Equal(t, model.QueueTypeClassic, second[model.QueueType])
	assert.Equal(t, int32(1000), second["x-consumer-timeout"])
	assert.Equal(t, amqp.Table{"x-consumer-timeout": int32(1000)}, custom)
}

// MasterLocator has no key in the table and no constant to put it under, so
// setting it changes nothing. Pinned so that wiring it up is a deliberate
// change to what gets declared.
func TestQueueTableIgnoresMasterLocator(t *testing.T) {
	assert.Equal(
		t,
		model.Queue{}.Table(),
		model.Queue{MasterLocator: "min-masters"}.Table(),
	)
}
