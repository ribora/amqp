package model

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	QueueType                 = "x-queue-type"
	QueueInitialGroupSize     = "x-quorum-initial-group-size"
	QueueMessageTTL           = "x-message-ttl"
	QueueExpires              = "x-expires"
	QueueMaxPriority          = "x-max-priority"
	QueueMaxLength            = "x-max-length"
	QueueMaxBytes             = "x-max-length-bytes"
	QueueOverflow             = "x-overflow"
	QueueDeadLetterExchange   = "x-dead-letter-exchange"
	QueueDeadLetterRoutingKey = "x-dead-letter-routing-key"
	QueueMode                 = "x-queue-mode"
	QueueDeliveryLimit        = "x-delivery-limit"

	QueueTypeClassic = "classic"
	QueueModeLazy    = "lazy"
)

type Queue struct {
	Type string

	MessageTTL  *time.Duration
	AutoExpire  *time.Duration
	MaxPriority *int32

	MaxLength      *int32
	MaxLengthBytes *int32
	Overflow       string

	DeadLetterExchange   string
	DeadLetterRoutingKey string

	LazyMode      bool
	MasterLocator string

	DeliveryLimit    *int32
	InitialGroupSize *int32

	CustomArgs amqp.Table
}

func (q Queue) Table() amqp.Table {
	args := make(amqp.Table)

	if q.Type != "" {
		args[QueueType] = q.Type
	} else {
		args[QueueType] = QueueTypeClassic
	}

	if q.MessageTTL != nil {
		args[QueueMessageTTL] = int32(q.MessageTTL.Milliseconds())
	}
	if q.AutoExpire != nil {
		args[QueueExpires] = int32(q.AutoExpire.Milliseconds())
	}
	if q.MaxPriority != nil {
		args[QueueMaxPriority] = *q.MaxPriority
	}
	if q.MaxLength != nil {
		args[QueueMaxLength] = *q.MaxLength
	}
	if q.MaxLengthBytes != nil {
		args[QueueMaxBytes] = *q.MaxLengthBytes
	}
	if q.Overflow != "" {
		args[QueueOverflow] = q.Overflow
	}
	if q.DeadLetterExchange != "" {
		args[QueueDeadLetterExchange] = q.DeadLetterExchange
		if q.DeadLetterRoutingKey != "" {
			args[QueueDeadLetterRoutingKey] = q.DeadLetterRoutingKey
		}
	}
	if q.LazyMode {
		args[QueueMode] = QueueModeLazy
	}
	if q.DeliveryLimit != nil {
		args[QueueDeliveryLimit] = *q.DeliveryLimit
	}
	if q.InitialGroupSize != nil {
		args[QueueInitialGroupSize] = *q.InitialGroupSize
	}

	for k, v := range q.CustomArgs {
		args[k] = v
	}

	return args
}
