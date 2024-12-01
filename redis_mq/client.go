package redis_mq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/demdxx/gocast"
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

	repairClient(client.opts)

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

func (client *Client) XAdd(ctx context.Context, topic string, maxLen int, key, val string) (string, error) {
	if topic == "" {
		return "", errors.New("redis topic can not be empty")
	}

	conn, err := client.getConn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	return redis.String(conn.Do("XADD", topic, "MAXLEN", maxLen, "*", key, val))
}

func (client *Client) XReadGroup(ctx context.Context, groupID, consumerID, topic string, timeoutMiliSeconds int) ([]*MsgEntity, error) {
	return client.xReadGroup(ctx, groupID, consumerID, topic, timeoutMiliSeconds, false)
}

func (client *Client) XReadGroupPending(ctx context.Context, groupID, consumerID, topic string) ([]*MsgEntity, error) {
	return client.xReadGroup(ctx, groupID, consumerID, topic, 0, true)
}

// Two cases are distinguished according to the value of isPending:
// if idPending is true,  it means consume message which was allocated to current consumer but not ack.
// else, it means consume new message which is not consumed by any other consumer.
func (client *Client) xReadGroup(ctx context.Context, groupID, consumerID, topic string, timeoutMiliSeconds int, isPending bool) ([]*MsgEntity, error) {
	if groupID == "" || consumerID == "" || topic == "" {
		return nil, errors.New("redis XREADGROUP groupID, consumerID and topic con not be empty")
	}

	conn, err := client.getConn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var rawReply interface{}
	if isPending {
		rawReply, err = conn.Do("XREADGROUP", "GROUP", groupID, consumerID, "STREAMS", topic, "0-0")
	} else {
		rawReply, err = conn.Do("XREADGROUP", "GROUP", groupID, consumerID, "BLOCK", timeoutMiliSeconds, "STREAMS", topic, ">")
	}
	if err != nil {
		return nil, err
	}
	reply, _ := rawReply.([]interface{})
	if len(reply) == 0 {
		return nil, ErrNoMsg
	}

	replyItem, _ := reply[0].([]interface{})
	if len(replyItem) != 2 {
		return nil, errors.New("invalid msg format")
	}

	var msgs []*MsgEntity
	rawMsgs, _ := replyItem[1].([]interface{})
	for _, rawMsg := range rawMsgs {
		msg, _ := rawMsg.([]interface{})
		if len(msg) != 2 {
			return nil, errors.New("invalid msg format")
		}
		msgID := gocast.ToString(msg[0])
		msgBody, _ := msg[1].([]interface{})
		if len(msgBody) != 2 {
			return nil, errors.New("invalid msg format")
		}
		msgKey := gocast.ToString(msgBody[0])
		msgVal := gocast.ToString(msgBody[1])
		msgs = append(msgs, &MsgEntity{
			MsgID: msgID,
			Key:   msgKey,
			Val:   msgVal,
		})
	}

	return msgs, nil
}

func (client *Client) XAck(ctx context.Context, topic, groupID, msgID string) error {
	if topic == "" || groupID == "" || msgID == "" {
		return errors.New("redis XACK topic, groupID and msgID con not be empty")
	}

	conn, err := client.pool.GetContext(ctx)
	if err != nil {
		return err
	}

	resp, err := redis.Int64(conn.Do("XACK", topic, groupID, msgID))
	if err != nil {
		return err
	}
	if resp != 1 {
		return fmt.Errorf("invalid resp: %d", resp)
	}
	return nil
}
