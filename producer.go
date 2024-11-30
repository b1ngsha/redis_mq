package redis_mq

type Producer struct {
	client *Client
	opts   *ProducerOptions
}

func NewProducer(client *Client, opts ...ProducerOption) *Producer {
	producer := Producer{
		client: client,
		opts: &ProducerOptions{},
	}

	for _, opt := range opts {
		opt(producer.opts)
	}

	repairProducer(producer.opts)

	return &producer
}

