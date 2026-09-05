package amqp

import "context"

// In this package and not a test one because newSource is unexported: without
// it a test has no way to build a Source at all.
type Delivery struct {
	RoutingKey string
	Exchange   string
	Headers    map[string]string
	Body       []byte
}

func (d Delivery) Source(ctx context.Context) Source {
	return &handmade{ctx: ctx, d: d}
}

type handmade struct {
	ctx context.Context
	d   Delivery
}

func (s *handmade) Body() []byte             { return s.d.Body }
func (s *handmade) Context() context.Context { return s.ctx }
func (s *handmade) RoutingKey() string       { return s.d.RoutingKey }
func (s *handmade) Exchange() string         { return s.d.Exchange }

func (s *handmade) Header(key string) string { return s.d.Headers[key] }

func (s *handmade) WithContext(ctx context.Context) Source {
	cp := *s
	cp.ctx = ctx
	return &cp
}

func (s *handmade) Lookup(scope, key string) string {
	return lookup(s, scope, key)
}

func (s *handmade) LookupAll(scope, key string) []string {
	return lookupAll(s, scope, key)
}
