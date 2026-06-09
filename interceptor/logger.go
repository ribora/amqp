package interceptor

import (
	"fmt"

	"gitlab.com/gorib/amqp"
	"gitlab.com/gorib/pry"
)

func NewLog(logger pry.Logger) *logMW {
	return &logMW{logger: logger}
}

type logMW struct {
	logger pry.Logger
}

func (logMW) Name() string { return "log" }
func (logMW) Order() int   { return 400 }

func (l *logMW) Wrap(next func(msgs []amqp.Source) error) func(msgs []amqp.Source) error {
	return func(msgs []amqp.Source) error {
		for i := range msgs {
			ctx := pry.ContextWithLogger(msgs[i].Context(), l.logger)
			msgs[i] = msgs[i].WithContext(ctx)
			l.logger.Trace(fmt.Sprintf("%s >> %s", msgs[i].RoutingKey(), string(msgs[i].Body())), pry.Ctx(ctx))
		}

		err := next(msgs)
		if err != nil && len(msgs) > 0 {
			l.logger.Error(err, pry.Ctx(msgs[len(msgs)-1].Context()))
		}
		return err
	}
}
