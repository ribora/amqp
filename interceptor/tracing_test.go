package interceptor

import (
	"context"
	"errors"
	"testing"

	"github.com/ribora/amqp"
	"github.com/ribora/failure"
	"github.com/ribora/hilton"
	"github.com/ribora/whisk/scan"
)

const processDuration = "messaging.process.duration"

var errNotFound = failure.New("resource not found")

type source struct{ ctx context.Context }

func (s source) Context() context.Context { return s.ctx }
func (s source) Body() []byte             { return nil }
func (s source) RoutingKey() string       { return "orders.created" }
func (s source) Exchange() string         { return "orders" }
func (s source) Header(string) string     { return "" }

func (s source) Lookup(scope, _ string) string {
	switch scope {
	case "routing-key":
		return s.RoutingKey()
	case "exchange":
		return s.Exchange()
	}
	return ""
}

func (s source) LookupAll(scope, key string) []string {
	if v := s.Lookup(scope, key); v != "" {
		return []string{v}
	}
	return nil
}

func (s source) WithContext(ctx context.Context) amqp.Source {
	return source{ctx: ctx}
}

func tracedErrorType(t *testing.T, handlerErr error, panicWith any) any {
	t.Helper()

	rec := hilton.Recorder()
	handler := func([]scan.Source) error {
		if panicWith != nil {
			panic(panicWith)
		}
		return handlerErr
	}
	if panicWith != nil {
		handler = NewRecover().Wrap(handler)
	}

	_ = NewTracing(rec).Wrap(handler)([]scan.Source{source{ctx: context.Background()}})

	for _, m := range rec.Measurements() {
		if m.Instrument != processDuration {
			continue
		}
		for _, a := range m.Attributes {
			if a.Key() == "error.type" {
				return a.Value()
			}
		}
		return nil
	}
	t.Fatalf("%s was never recorded", processDuration)
	return nil
}

func TestErrorType(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		panicWith any
		want      any
	}{
		{"success carries nothing", nil, nil, nil},
		{"a failure carries its type", failure.Wrap("%w: id={id}", errNotFound, 7), nil, "resource_not_found"},
		{"a panic through Recover", nil, "boom", "panic"},
		{"anything else falls back", errors.New("boom"), nil, otherErrorType},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := tracedErrorType(t, c.err, c.panicWith); got != c.want {
				t.Errorf("error.type = %v, want %v", got, c.want)
			}
		})
	}
}
