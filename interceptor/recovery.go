package interceptor

import (
	"context"

	"gitlab.com/gorib/failure"
)

func NewRecover() *recoverMW {
	return &recoverMW{}
}

type recoverMW struct{}

func (recoverMW) Name() string { return "recover" }
func (recoverMW) Order() int   { return 500 }

func (recoverMW) Wrap(next func(ctx context.Context, keys []string, documents [][]byte) error) func(ctx context.Context, keys []string, documents [][]byte) error {
	return func(ctx context.Context, keys []string, documents [][]byte) error {
		return failure.Catch(func() error { return next(ctx, keys, documents) })
	}
}
