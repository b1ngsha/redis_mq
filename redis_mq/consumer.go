package redis_mq

import (
	"context"
	"errors"
	"redis_mq/log"
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

type MsgCallback func(ctx context.Context, msg *MsgEntity) error

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

func (consumer *Consumer) receive() ([]*MsgEntity, error) {
	msgs, err := consumer.client.XReadGroup(consumer.ctx, consumer.groupID, consumer.consumerID, consumer.topic, int(consumer.opts.consumeTimeout.Milliseconds()))
	if err != nil && !errors.Is(err, ErrNoMsg) {
		return nil, err
	}
	return msgs, nil
}

func (consumer *Consumer) receivePending() ([]*MsgEntity, error) {
	pendingMsgs, err := consumer.client.XReadGroupPending(consumer.ctx, consumer.groupID, consumer.consumerID, consumer.topic)
	if err != nil && !errors.Is(err, ErrNoMsg) {
		return nil, err
	}
	return pendingMsgs, nil
}

func (consumer *Consumer) handleMsgs(ctx context.Context, msgs []*MsgEntity) {
	for _, msg := range msgs {
		if err := consumer.callbackFunc(ctx, msg); err != nil {
			consumer.failedCnts[*msg]++
			continue
		}

		if err := consumer.client.XAck(ctx, consumer.topic, consumer.groupID, msg.MsgID); err != nil {
			log.ErrorContextf(ctx, "ack msg failed, msg id: %s, error: %v", msg.MsgID, err)
			continue
		}

		delete(consumer.failedCnts, *msg)
	}
}

func (consumer *Consumer) deliverDeadLetter(ctx context.Context) {
	for msg, failedCnt := range consumer.failedCnts {
		if failedCnt < consumer.opts.maxRetryLimit {
			continue
		}

		if err := consumer.opts.deadLetterQueue.Deliver(ctx, &msg); err != nil {
			log.ErrorContextf(ctx, "dead letter deliver failed, msg id: %s, error: %v", msg.MsgID, err)
		}

		if err := consumer.client.XAck(ctx, consumer.topic, consumer.groupID, msg.MsgID); err != nil {
			log.ErrorContextf(ctx, "msg ack failed, msg id: %s, error: %v", msg.MsgID, err)
			continue
		}

		delete(consumer.failedCnts, msg)
	}
}

func (consumer *Consumer) run() {
	for {
		select {
		case <-consumer.ctx.Done():
			return
		default:
		}

		// receive and handle new messages
		msgs, err := consumer.receive()
		if err != nil {
			log.ErrorContextf(consumer.ctx, "receive msg failed, error: %v", err)
			continue
		}

		timeoutCtx, _ := context.WithTimeout(consumer.ctx, consumer.opts.handleMsgsTimeout)
		consumer.handleMsgs(timeoutCtx, msgs)

		timeoutCtx, _ = context.WithTimeout(consumer.ctx, consumer.opts.deadLetterDeliverTimeout)
		consumer.deliverDeadLetter(timeoutCtx)

		// receive messages which were received but not acked
		pendingMsgs, err := consumer.receivePending()
		if err != nil {
			log.ErrorContextf(consumer.ctx, "pending msg received failed, err: %v", err)
			continue
		}

		timeoutCtx, _ = context.WithTimeout(consumer.ctx, consumer.opts.handleMsgsTimeout)
		consumer.handleMsgs(timeoutCtx, pendingMsgs)
	}
}

func (consumer *Consumer) Stop() {
	consumer.canceller()
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
