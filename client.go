package redis_mq

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Client struct {
	opts *ClientOptions
	pool *redis.Pool
}

func NewClient(network, address, password string, opts ...ClientOption) *Client {
	client := Client{
		opts: &ClientOptions{
			network:  network,
			address:  address,
			password: password,
		},
	}

	for _, opt := range opts {
		opt(client.opts)
	}

	repairClientOptions(client.opts)

	pool := client.getRedisPool()
	return &Client{
		pool: pool,
	}
}

func (client *Client) getRedisPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     client.opts.maxIdle,
		IdleTimeout: time.Duration(client.opts.idleTimeoutSeconds) * time.Second,
		Dial: func() (redis.Conn, error) {
			conn, err := client.buildRedisConn()
			if err != nil {
				return nil, err
			}
			return conn, nil
		},
		MaxActive: client.opts.maxActive,
		Wait:      client.opts.wait,
		TestOnBorrow: func(conn redis.Conn, lastUsed time.Time) error {
			_, err := conn.Do("PING")
			return err
		},
	}
}

// get redis conn from pool
func (client *Client) getConn(ctx context.Context) (redis.Conn, error) {
	return client.pool.GetContext(ctx)
}

func (client *Client) buildRedisConn() (redis.Conn, error) {
	if client.opts.address == "" {
		panic("can not get redis address from config")
	}

	var dialOpts []redis.DialOption
	if len(client.opts.password) > 0 {
		dialOpts = append(dialOpts, redis.DialPassword(client.opts.password))
	}

	return redis.DialContext(context.Background(), client.opts.network, client.opts.address, dialOpts...)
}
