package redis_mq_test

import (
	"context"
	"redis_mq/redis_mq"
	"testing"
	"time"
)

const (
	groupID    = "test_group_id"
	consumerID = "test_consumer_id"
)

type DemoDeadLetterQueue struct {
	do func(msg *redis_mq.MsgEntity)
}

func NewDemoDeadLetterQueue(do func(msg *redis_mq.MsgEntity)) *DemoDeadLetterQueue {
	return &DemoDeadLetterQueue{do: do}
}

func (queue *DemoDeadLetterQueue) Deliver(ctx context.Context, msg *redis_mq.MsgEntity) error {
	queue.do(msg)
	return nil
}

func TestConsumer(t *testing.T) {
	client := redis_mq.NewClient(network, address, password)

	callbackFunc := func(ctx context.Context, msg *redis_mq.MsgEntity) error {
		t.Logf("receive msg, msg id: %s, msg key: %s, msg val: %s", msg.MsgID, msg.Key, msg.Val)
		return nil
	}

	demoDeadLetterQueue := NewDemoDeadLetterQueue(func(msg *redis_mq.MsgEntity) {
		t.Logf("receive dead letter, msg id: %s, msg key: %s, msg val: %s", msg.MsgID, msg.Key, msg.Val)
	})

	consumer, err := redis_mq.NewConsumer(client, topic, groupID, consumerID, callbackFunc, redis_mq.WithMaxRetryLimit(2), redis_mq.WithConsumeTimeout(2*time.Second), redis_mq.WithDeadLetterQueue(demoDeadLetterQueue))
	if err != nil {
		t.Error(err)
		return
	}
	defer consumer.Stop()

	<-time.After(10 * time.Second)
}
