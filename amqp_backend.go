// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// AMQPCeleryBackend CeleryBackend for AMQP
type AMQPCeleryBackend struct {
	channel    *amqp.Channel
	Connection *amqp.Connection
	Host       string
}

func NewCeleryBackend(host string) (CeleryBackend, error) {
	if strings.HasPrefix(host, "amqp://") {
		return NewAMQPCeleryBackend(host)
	} else if strings.HasPrefix(host, "redis://") {
		return NewRedisCeleryBackend(host)
	} else {
		return nil, fmt.Errorf("unsupport schema '%s'", host)
	}
}

// NewAMQPCeleryBackend creates new AMQPCeleryBackend
func NewAMQPCeleryBackend(host string) (*AMQPCeleryBackend, error) {
	conn, channel, err := NewAMQPConnection(host)
	if err != nil {
		return nil, err
	}

	return &AMQPCeleryBackend{
		channel:    channel,
		Connection: conn,
		Host:       host,
	}, nil
}

// Reconnect reconnects to AMQP server
func (b *AMQPCeleryBackend) Reconnect(context.Context) error {
	_ = b.channel.Close()
	_ = b.Connection.Close()

	conn, channel, err := NewAMQPConnection(b.Host)
	if err != nil {
		return err
	}

	b.channel = channel
	b.Connection = conn

	return nil
}

func parseAndRetry(
	ctx context.Context,
	key string,
	fn func() error,
	reconnector interface {
		Reconnect(ctx context.Context) error
	},
) (err error) {
	var (
		retryDuration time.Duration = 500 * time.Millisecond
	)

	for {
		reconnectFlag := false
		if err == nil {
			err = fn()
		}

		if err != nil {
			var berr *amqp.Error
			if errors.As(err, &berr) {
				switch berr.Code {
				case amqp.NotFound:
					// 重试， 无需重连
					log.Printf("amqp_backend: %s fail, err '%s', retry '%s'", key, err, retryDuration)
					err = nil
				case amqp.ChannelError:
					log.Printf("amqp_backend: %s fail, err '%s'", key, err)
					// 继续重连
					fallthrough
				case amqp.ConnectionForced:
					log.Printf("amqp_backend: %s fail, err '%s'", key, err)
					// 继续重连
					fallthrough
				case amqp.FrameError:
					// 继续重连
					reconnectFlag = true
				default:
					log.Printf("amqp_backend: %s fail, err '%s'", key, err)
					return err
				}
			} else if errStr := err.Error(); strings.Contains(errStr, "connect: connection refused") ||
				strings.Contains(errStr, ": write: broken pipe") {
				// 继续重连
				reconnectFlag = true
			} else {
				log.Printf("amqp_backend: %s fail, err '%s'", key, err)
				return err
			}
		} else {
			break
		}

		if reconnectFlag {
			if err = reconnector.Reconnect(ctx); err != nil {
				// 休眠等待再重连
				log.Printf("amqp_backend: %s Reconnect fail, err '%s', retry '%s'", key, err, retryDuration)
			} else {
				continue
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDuration):
			// 继续重试
			retryDuration *= 2
			if retryDuration > time.Minute {
				retryDuration = time.Minute
			}
		}
	}

	return nil
}

// GetResult retrieves result from AMQP queue
func (b *AMQPCeleryBackend) GetResult(
	ctx context.Context, taskID string,
) (*ResultMessage, error) {
	var (
		queueName = strings.Replace(taskID, "-", "", -1)
		result    <-chan amqp.Delivery
		err       error
	)

	if err = parseAndRetry(ctx, "ConsumeWithContext", func() error {
		// open channel temporarily
		result, err = b.channel.ConsumeWithContext(
			ctx, queueName, "", false, false, false, false, nil,
		)

		return err
	}, b); err != nil {
		return nil, err
	} else if result == nil {
		return nil, errors.New("ConsumeWithContext return empty")
	}

	var resultMessage ResultMessage

	delivery := <-result
	deliveryAck(delivery)
	if err := json.Unmarshal(delivery.Body, &resultMessage); err != nil {
		return nil, err
	}
	return &resultMessage, nil
}
