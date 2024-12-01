package redis_mq

import (
	"context"
	"redis_mq/log"
)

type DeadLetterQueue interface {
	Deliver(ctx context.Context, msg *MsgEntity) error
}

type DeadLetterQueueLogger struct {}

func NewDeadLetterQueueLogger() *DeadLetterQueueLogger {
	return &DeadLetterQueueLogger{}
}

func (queue *DeadLetterQueueLogger) Deliver(ctx context.Context, msg *MsgEntity) error {
	log.ErrorContextf(ctx, "msg fail exceed try limit, msg id: %s", msg.MsgID)
	return nil
}