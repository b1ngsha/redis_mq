package redis_mq

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
		opts.maxMsgLen = 500
	}
}