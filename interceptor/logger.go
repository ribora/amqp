package interceptor

import (
	"context"
	"fmt"

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

func (l *logMW) Wrap(next func(ctx context.Context, keys []string, documents [][]byte) error) func(ctx context.Context, keys []string, documents [][]byte) error {
	return func(ctx context.Context, keys []string, documents [][]byte) error {
		ctx = pry.ContextWithLogger(ctx, l.logger)

		for i, key := range keys {
			l.logger.Trace(fmt.Sprintf("%s >> %s", key, string(documents[i])), pry.Ctx(ctx))
		}

		err := next(ctx, keys, documents)
		if err != nil {
			l.logger.Error(err, pry.Ctx(ctx))
		}
		return err
	}
}
