package redis_mq

import (
	"context"
)

type Producer struct {
	client *Client
	opts   *ProducerOptions
}

func NewProducer(client *Client, opts ...ProducerOption) *Producer {
	producer := Producer{
		client: client,
		opts:   &ProducerOptions{},
	}

	for _, opt := range opts {
		opt(producer.opts)
	}

	repairProducer(producer.opts)

	return &producer
}

// send message to redis topic
func (producer *Producer) SendMsg(ctx context.Context, topic, key, val string) (string, error) {
	return producer.client.XAdd(ctx, topic, producer.opts.maxMsgLen, key, val)
}
