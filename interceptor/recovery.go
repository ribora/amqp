package interceptor

import (
	"github.com/ribora/failure"
	"github.com/ribora/whisk/scan"
)

func NewRecover() *recoverMW {
	return &recoverMW{}
}

type recoverMW struct{}

func (recoverMW) Wrap(next func(msgs []scan.Source) error) func(msgs []scan.Source) error {
	return func(msgs []scan.Source) error {
		return failure.Catch(func() error { return next(msgs) })
	}
}
