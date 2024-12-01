package redis_mq

import "time"

const (
	// client
	DefaultMaxIdle            = 20
	DefaultIdleTimeoutSeconds = 10
	DefaultMaxActive          = 100

	// producer
	DefaultMaxMsgLen = 500

	// consumer
	DefaultConsumeTimeout           = 2 * time.Second
	DefaultMaxRetryLimit            = 3
	DefaultDeadLetterQueue          = NewDeadLetterQueue()
	DefaultDeadLetterDeliverTimeout = time.Second
	DefaultHandleMsgsTimeout        = time.Second
)

type ClientOptions struct {
	network  string
	address  string
	password string

	// optional
	maxIdle            int
	idleTimeoutSeconds int
	maxActive          int
	wait               bool
}

type ClientOption func(client *ClientOptions)

func WithMaxIdle(maxIdle int) ClientOption {
	return func(opts *ClientOptions) {
		opts.maxIdle = maxIdle
	}
}

func WithIdleTimeoutSeconds(idleTimeoutSeconds int) ClientOption {
	return func(opts *ClientOptions) {
		opts.idleTimeoutSeconds = idleTimeoutSeconds
	}
}

func WithMaxActive(maxActive int) ClientOption {
	return func(opts *ClientOptions) {
		opts.maxActive = maxActive
	}
}

func WithWaitMode() ClientOption {
	return func(opts *ClientOptions) {
		opts.wait = true
	}
}

func repairClient(opts *ClientOptions) {
	if opts.maxIdle < 0 {
		opts.maxIdle = DefaultMaxIdle
	}
	if opts.idleTimeoutSeconds < 0 {
		opts.idleTimeoutSeconds = DefaultIdleTimeoutSeconds
	}
	if opts.maxActive < 0 {
		opts.maxActive = DefaultMaxActive
	}
}

type ProducerOptions struct {
	maxMsgLen int // max max message sizes which topic can store, if exceed, the old message will be popped
}

type ProducerOption func(opts *ProducerOptions)

func WithMaxMsgLen(maxMsgLen int) ProducerOption {
	return func(opts *ProducerOptions) {
		opts.maxMsgLen = maxMsgLen
	}
}

func repairProducer(opts *ProducerOptions) {
	if opts.maxMsgLen <= 0 {
		opts.maxMsgLen = DefaultMaxMsgLen
	}
}

type ConsumerOptions struct {
	consumeTimeout           time.Duration // blocking consume every new message timeout
	maxRetryLimit            int           // max retry times for each message, if exceed, the message will be pushed to dead letter queue
	deadLetterQueue          DeadLetterQueue
	deadLetterDeliverTimeout time.Duration
	handleMsgsTimeout        time.Duration
}

type ConsumerOption func(opts *ConsumerOptions)

func WithConsumeTimeout(consumeTimeout time.Duration) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.consumeTimeout = consumeTimeout
	}
}

func WithMaxRetryLimit(maxRetryLimit int) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.maxRetryLimit = maxRetryLimit
	}
}

func WithDeadLetterQueue(deadLetterQueue DeadLetterQueue) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.deadLetterQueue = deadLetterQueue
	}
}

func WithDeadLetterDeliverTimeout(deadLetterDeliverTimeout time.Duration) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.deadLetterDeliverTimeout = deadLetterDeliverTimeout
	}
}

func WithHandleMsgsTimeout(handleMsgsTimeout time.Duration) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.handleMsgsTimeout = handleMsgsTimeout
	}
}

func repairConsumer(opts *ConsumerOptions) {
	if opts.consumeTimeout < 0 {
		opts.consumeTimeout = 2 * time.Second
	}
	if opts.maxRetryLimit < 0 {
		opts.maxRetryLimit = DefaultMaxRetryLimit
	}
	if opts.deadLetterQueue == nil {
		opts.deadLetterQueue = DefaultDeadLetterQueue
	}
	if opts.deadLetterDeliverTimeout <= 0 {
		opts.deadLetterDeliverTimeout = DefaultDeadLetterDeliverTimeout
	}
	if opts.handleMsgsTimeout <= 0 {
		opts.handleMsgsTimeout = DefaultHandleMsgsTimeout
	}
}


