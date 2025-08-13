// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// CeleryClient provides API for sending celery tasks
type CeleryClient struct {
	broker  CeleryBroker
	backend CeleryBackend
}

// CeleryBroker is interface for celery broker database
type CeleryBroker interface {
	SendCeleryMessage(context.Context, *CeleryMessage) error
}

// CeleryBackend is interface for celery backend database
type CeleryBackend interface {
	GetResult(context.Context, string) (*ResultMessage, error) // must be non-blocking
}

// NewCeleryClient creates new celery client
func NewCeleryClient(broker CeleryBroker, backend CeleryBackend) *CeleryClient {
	return &CeleryClient{
		broker,
		backend,
	}
}

// Delay gets asynchronous result
func (cc *CeleryClient) Delay(
	ctx context.Context, task string, args ...any,
) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Args = args
	return cc.delay(ctx, celeryTask)
}

// DelayKwargs gets asynchronous results with argument map
func (cc *CeleryClient) DelayKwargs(
	ctx context.Context, task string, args map[string]any,
) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Kwargs = args
	return cc.delay(ctx, celeryTask)
}

func (cc *CeleryClient) delay(
	ctx context.Context, task *TaskMessage,
) (*AsyncResult, error) {
	defer releaseTaskMessage(task)
	encodedMessage, err := task.Encode()
	if err != nil {
		return nil, err
	}
	celeryMessage := getCeleryMessage(encodedMessage)
	defer releaseCeleryMessage(celeryMessage)
	err = cc.broker.SendCeleryMessage(ctx, celeryMessage)
	if err != nil {
		return nil, err
	}
	return &AsyncResult{
		TaskID:  task.ID,
		backend: cc.backend,
	}, nil
}

// AsyncResult represents pending result
type AsyncResult struct {
	TaskID  string
	backend CeleryBackend
	result  *ResultMessage
}

// Get gets actual result from backend
// It blocks for period of time set by timeout and returns error if unavailable
func (ar *AsyncResult) Get(
	ctx context.Context, timeout time.Duration,
) (interface{}, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	ctx, fn := context.WithTimeout(ctx, timeout)
	defer fn()

	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, fmt.Errorf(
					"getting result timeout (%s) for %s, err '%w'", timeout, ar.TaskID, err,
				)
			} else if errors.Is(err, context.Canceled) {
				return nil, fmt.Errorf(
					"getting result canceled for %s, err '%w'", ar.TaskID, err,
				)
			} else {
				return nil, ctx.Err()
			}
		case <-ticker.C:
			val, err := ar.AsyncGet(ctx)
			if err != nil {
				time.Sleep(time.Millisecond * 100)
				continue
			}
			return val, nil
		}
	}
}

// AsyncGet gets actual result from backend and returns nil if not available
func (ar *AsyncResult) AsyncGet(ctx context.Context) (interface{}, error) {
	if ar.result != nil {
		return ar.result.Result, nil
	}
	val, err := ar.backend.GetResult(ctx, ar.TaskID)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, err
	}
	if val.Status != "SUCCESS" {
		return nil, fmt.Errorf("error response status %v", val)
	}
	ar.result = val
	return val.Result, nil
}

// Ready checks if actual result is ready
func (ar *AsyncResult) Ready(ctx context.Context) (bool, error) {
	if ar.result != nil {
		return true, nil
	}
	val, err := ar.backend.GetResult(ctx, ar.TaskID)
	if err != nil {
		return false, err
	}
	ar.result = val
	return (val != nil), nil
}
