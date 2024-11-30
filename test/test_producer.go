package redis_mq_test

import (
	"context"
	"redis_mq"
	"testing"
)

const (
	network  = "tcp"
	address  = "Please enter redis address"
	password = "Please enter redis password"
	topic    = "test_topic"
)

func test_producer(t *testing.T) {
	client := redis_mq.NewClient(network, address, password)
	producer := redis_mq.NewProducer(client, redis_mq.WithMaxMsgLen(10))
	ctx := context.Background()
	msgID, err := producer.SendMsg(ctx, topic, "test_key", "test_val")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(msgID)
}
