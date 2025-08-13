// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCeleryBackend is celery backend for redis
type RedisCeleryBackend struct {
	client *redis.Client
}

// NewRedisBackend creates new RedisCeleryBackend with given redis pool.
// RedisCeleryBackend can be initialized manually as well.
func NewRedisBackend(client *redis.Client) *RedisCeleryBackend {
	return &RedisCeleryBackend{
		client: client,
	}
}

// NewRedisCeleryBackend creates new RedisCeleryBackend
func NewRedisCeleryBackend(uri string) (*RedisCeleryBackend, error) {
	client, err := NewRedis(uri, 0, 0, 0)
	if err != nil {
		return nil, err
	}

	return NewRedisBackend(client), nil
}

func resultKey(taskID string) string {
	return fmt.Sprintf("celery-task-meta-%s", taskID)
}

// GetResult queries redis backend to get asynchronous result
func (cb *RedisCeleryBackend) GetResult(ctx context.Context, taskID string) (*ResultMessage, error) {

	var (
		resultMessage ResultMessage
		key           = resultKey(taskID)
	)

	err := cb.client.Get(ctx, key).Scan(&resultMessage)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("result not available")
		}

		return nil, err
	}

	return &resultMessage, nil
}

// SetResult pushes result back into redis backend
func (cb *RedisCeleryBackend) SetResult(
	ctx context.Context, taskID string, result *ResultMessage,
) error {
	resBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}

	cmd := cb.client.SetEx(ctx, resultKey(taskID), resBytes, 86400*time.Second)
	return cmd.Err()
}
