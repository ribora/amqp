package amqp

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ Source = Delivery{}.Source(context.Background())

func TestDeliveryAnswersTheAmqpScopes(t *testing.T) {
	src := Delivery{
		RoutingKey: "contracts.updated",
		Exchange:   "sherpa",
		Headers:    map[string]string{"x-request-id": "abc"},
		Body:       []byte(`{"id":1}`),
	}.Source(t.Context())

	assert.Equal(t, "contracts.updated", src.Lookup("routing-key", ""))
	assert.Equal(t, "sherpa", src.Lookup("exchange", ""))
	assert.Equal(t, "abc", src.Lookup("header", "x-request-id"))
	assert.Equal(t, []string{"abc"}, src.LookupAll("header", "x-request-id"))
}

func TestDeliveryReportsRoutingKeyAndExchange(t *testing.T) {
	src := Delivery{RoutingKey: "contracts.created", Exchange: "sherpa"}.Source(t.Context())

	assert.Equal(t, "contracts.created", src.RoutingKey())
	assert.Equal(t, "sherpa", src.Exchange())
}

func TestDeliveryReturnsNothingForAnUnknownScope(t *testing.T) {
	src := Delivery{RoutingKey: "k"}.Source(t.Context())

	assert.Empty(t, src.Lookup("topic", "k"))
	assert.Nil(t, src.LookupAll("topic", "k"))
	assert.Empty(t, src.Header("absent"))
}

func TestWithContextDoesNotTouchTheOriginal(t *testing.T) {
	type key struct{}
	src := Delivery{Body: []byte("x")}.Source(t.Context())

	derived := src.WithContext(context.WithValue(t.Context(), key{}, "v"))

	assert.Equal(t, "v", derived.Context().Value(key{}))
	assert.Nil(t, src.Context().Value(key{}))
	assert.Equal(t, []byte("x"), derived.Body())
}

func TestPublisherRecorderKeepsKeyAndPayload(t *testing.T) {
	r := NewPublisherRecorder()

	require.NoError(t, r.Publish(t.Context(), "guards.granted", map[string]any{"pin": "11111"}))
	require.NoError(t, r.Publish(t.Context(), "guards.revoked", nil))

	published := r.Published()
	require.Len(t, published, 2)
	assert.Equal(t, "guards.granted", published[0].Key)
	assert.Equal(t, map[string]any{"pin": "11111"}, published[0].Document)
	assert.Equal(t, "guards.revoked", published[1].Key)
}

func TestWithKeyFiltersOutTheRest(t *testing.T) {
	r := NewPublisherRecorder()
	require.NoError(t, r.Publish(t.Context(), "guards.granted", 1))
	require.NoError(t, r.Publish(t.Context(), "guards.revoked", 2))
	require.NoError(t, r.Publish(t.Context(), "guards.granted", 3))

	assert.Len(t, r.WithKey("guards.granted"), 2)
	assert.Empty(t, r.WithKey("guards.expired"))
}

func TestFailWithKeepsFailing(t *testing.T) {
	broken := errors.New("broker is down")
	r := NewPublisherRecorder()
	r.FailWith(broken)

	assert.ErrorIs(t, r.Publish(t.Context(), "guards.granted", 1), broken)
	assert.ErrorIs(t, r.Publish(t.Context(), "guards.granted", 1), broken)
	assert.Empty(t, r.Published(), "a refused publish is not a publish")
}

func TestResetClearsBothHistoryAndFailure(t *testing.T) {
	r := NewPublisherRecorder()
	r.FailWith(errors.New("broker is down"))
	_ = r.Publish(t.Context(), "guards.granted", 1)

	r.Reset()

	require.NoError(t, r.Publish(t.Context(), "guards.granted", 2))
	assert.Len(t, r.Published(), 1)
}

func TestPublishedIsACopy(t *testing.T) {
	r := NewPublisherRecorder()
	require.NoError(t, r.Publish(t.Context(), "first", nil))

	snapshot := r.Published()
	require.NoError(t, r.Publish(t.Context(), "second", nil))

	assert.Len(t, snapshot, 1)
}
