package interceptor

import (
	"gitlab.com/gorib/failure"
	"gitlab.com/gorib/whisk/scan"
)

func NewRecover() *recoverMW {
	return &recoverMW{}
}

type recoverMW struct{}

func (recoverMW) Name() string { return "recover" }
func (recoverMW) Order() int   { return 500 }

func (recoverMW) Wrap(next func(msgs []scan.Source) error) func(msgs []scan.Source) error {
	return func(msgs []scan.Source) error {
		return failure.Catch(func() error { return next(msgs) })
	}
}
