package redis_mq

import "errors"

type MsgEntity struct {
	MsgID string
	Key   string
	Val   string
}

var ErrNoMsg = errors.New("no message received")