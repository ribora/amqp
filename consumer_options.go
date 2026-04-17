package amqp

import (
	"time"

	"gitlab.com/gorib/amqp/model"
	"gitlab.com/gorib/pry"
)

type QueueConfig = model.Queue

type ConsumerOptions func(*consumer) error

func WithConsumerLogger(logger pry.Logger) ConsumerOptions {
	return func(c *consumer) error {
		c.logger = logger
		return nil
	}
}

func WithConsumerFailsafeTimeout(timeout time.Duration) ConsumerOptions {
	return func(c *consumer) error {
		c.failsafeTimeout = timeout
		return nil
	}
}

func WithConsumerExchangeKind(kind string) ConsumerOptions {
	return func(c *consumer) error {
		return c.SetExchangeKind(kind)
	}
}

func WithConsumerPageSize(size uint16) ConsumerOptions {
	return func(c *consumer) error {
		return c.SetPageSize(size)
	}
}

func WithConsumerFlushInterval(interval time.Duration) ConsumerOptions {
	return func(c *consumer) error {
		return c.SetFlushInterval(interval)
	}
}

func WithConsumerQueueArguments(queue QueueConfig) ConsumerOptions {
	return func(c *consumer) error {
		c.dlxExchange = queue.DeadLetterExchange
		return c.setQueueArgs(queue.Table())
	}
}
