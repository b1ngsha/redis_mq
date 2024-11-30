package redis_mq

const (
	// client
	DefaultMaxIdle            = 20
	DefaultIdleTimeoutSeconds = 10
	DefaultMaxActive          = 100

	// producer
	DefaultMaxMsgLen = 500
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
