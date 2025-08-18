// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCeleryBroker is celery broker for redis
type RedisCeleryBroker struct {
	client       *redis.Client
	ExchangeName string
}

// NewRedisBroker creates new RedisCeleryBroker with given redis connection pool
func NewRedisBroker(client *redis.Client, exchange string) *RedisCeleryBroker {
	if exchange == "" {
		exchange = defaultExchange
	}

	return &RedisCeleryBroker{
		client:       client,
		ExchangeName: exchange,
	}
}

// NewRedisCeleryBroker creates new RedisCeleryBroker based on given uri
func NewRedisCeleryBroker(uri string, exchange string) (*RedisCeleryBroker, error) {
	client, err := NewRedis(uri, 0, 0, 0)
	if err != nil {
		return nil, err
	}

	return NewRedisBroker(client, exchange), nil
}

// SendCeleryMessage sends CeleryMessage to redis queue
func (cb *RedisCeleryBroker) SendCeleryMessage(
	ctx context.Context, message *CeleryMessage,
) error {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	cmd := cb.client.LPush(ctx, cb.ExchangeName, jsonBytes)
	return cmd.Err()
}

// GetCeleryMessage retrieves celery message from redis queue
func (cb *RedisCeleryBroker) GetCeleryMessage(
	ctx context.Context,
) (*CeleryMessage, error) {
	result, err := cb.client.BRPop(ctx, time.Second, cb.ExchangeName).Result()
	if err != nil {
		// 处理超时或错误
		if err == redis.Nil {
			return nil, fmt.Errorf("null message received from redis")
		}

		return nil, err
	}

	if len(result) != 2 || result[0] != cb.ExchangeName {
		return nil, fmt.Errorf("not a celery message: %v", result[0])
	}

	var message CeleryMessage
	if err := json.Unmarshal([]byte(result[1]), &message); err != nil {
		return nil, err
	}
	return &message, nil
}

// GetTaskMessage retrieves task message from redis queue
func (cb *RedisCeleryBroker) GetTaskMessage(
	ctx context.Context,
) (*TaskMessage, error) {
	celeryMessage, err := cb.GetCeleryMessage(ctx)
	if err != nil {
		return nil, err
	}
	return celeryMessage.GetTaskMessage(), nil
}

// NewRedisPool creates pool of redis connections from given connection string
//
// Deprecated: newRedisPool exists for historical compatibility
// and should not be used. Pool should be initialized outside of gocelery package.
func NewRedis(
	redisUrl string,
	idleTimeout time.Duration,
	maxActive, maxIdle int,
) (*redis.Client, error) {
	redisOpts, err := redis.ParseURL(redisUrl)
	if err != nil {
		return nil, fmt.Errorf(
			"parse redis uri '%s' fail, err '%w'",
			redisUrl, err,
		)
	} else {
		if idleTimeout == 0 {
			idleTimeout = 240 * time.Second
		}
		if maxActive == 0 {
			maxActive = 3
		}

		redisOpts.ConnMaxIdleTime = idleTimeout
		redisOpts.MaxActiveConns = maxActive
		redisOpts.MaxIdleConns = maxIdle
	}

	return redis.NewClient(redisOpts), nil
}
