package interceptor

import (
	"fmt"

	"gitlab.com/gorib/amqp"
	"gitlab.com/gorib/pry"
	"gitlab.com/gorib/whisk/scan"
)

func NewLog(logger pry.Logger) *logMW {
	return &logMW{logger: logger}
}

type logMW struct {
	logger pry.Logger
}

func (l *logMW) Wrap(next func(msgs []scan.Source) error) func(msgs []scan.Source) error {
	return func(msgs []scan.Source) error {
		for i := range msgs {
			ctx := pry.ContextWithLogger(msgs[i].Context(), l.logger)
			msgs[i] = withContext(ctx, msgs[i])
			l.logger.Trace(fmt.Sprintf("%s >> %s", amqp.RoutingKey(msgs[i]), string(msgs[i].Body())), pry.Ctx(ctx))
		}

		err := next(msgs)
		if err != nil && len(msgs) > 0 {
			l.logger.Error(err, pry.Ctx(msgs[len(msgs)-1].Context()))
		}
		return err
	}
}
