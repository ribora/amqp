package interceptor

import (
	"gitlab.com/gorib/amqp"
	"gitlab.com/gorib/failure"
)

func NewRecover() *recoverMW {
	return &recoverMW{}
}

type recoverMW struct{}

func (recoverMW) Name() string { return "recover" }
func (recoverMW) Order() int   { return 500 }

func (recoverMW) Wrap(next func(msgs []amqp.Source) error) func(msgs []amqp.Source) error {
	return func(msgs []amqp.Source) error {
		return failure.Catch(func() error { return next(msgs) })
	}
}
