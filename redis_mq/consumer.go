package redis_mq

import (
	"context"
	"errors"
)

type Consumer struct {
	topic      string
	groupID    string
	consumerID string

	client *Client
	opts   *ConsumerOptions

	ctx       context.Context
	canceller context.CancelFunc

	callbackFunc MsgCallback // callback when received a message
	failedCnts   map[MsgEntity]int
}

func (consumer *Consumer) checkParams() error {
	if consumer.callbackFunc == nil {
		return errors.New("callback function can not be empty")
	}
	if consumer.client == nil {
		return errors.New("redis client can not be empty")
	}
	if consumer.topic == "" || consumer.consumerID == "" || consumer.groupID == "" {
		return errors.New("topic | consumerID | groupID can not be empty")
	}
	return nil
}

func NewConsumer(client *Client, topic, groupID, consumerID string, callbackFunc MsgCallback, opts ...ConsumerOption) (*Consumer, error) {
	ctx, canceller := context.WithCancel(context.Background())
	consumer := Consumer{
		topic:      topic,
		groupID:    groupID,
		consumerID: consumerID,

		client: client,
		opts:   &ConsumerOptions{},

		ctx:       ctx,
		canceller: canceller,

		callbackFunc: callbackFunc,
		failedCnts:   make(map[MsgEntity]int),
	}

	if err := consumer.checkParams(); err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(consumer.opts)
	}

	repairConsumer(consumer.opts)

	go consumer.run()
	return &consumer, nil
}
