// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const defaultExchange = "celery"

// AMQPCeleryBroker is RedisBroker for AMQP
type AMQPCeleryBroker struct {
	channel      *amqp.Channel
	Connection   *amqp.Connection
	Host         string
	ExchangeName string
}

// NewAMQPConnection creates new AMQP channel
func NewAMQPConnection(
	host string,
) (*amqp.Connection, *amqp.Channel, error) {
	uri, err := amqp.ParseURI(host)
	if err != nil {
		return nil, nil, err
	}

	config := amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, 5*time.Second)
		},
	}

	connection, err := amqp.DialConfig(host, config)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"dial '%s:%d%s' fail, err '%w'",
			uri.Host, uri.Port, uri.Vhost, err,
		)
	}

	channel, err := connection.Channel()
	if err != nil {
		connection.Close()
		return nil, nil, fmt.Errorf(
			"get channel fail, url '%s:%d%s', err '%w'",
			uri.Host, uri.Port, uri.Vhost, err,
		)
	}

	return connection, channel, nil
}

func NewCeleryBroker(
	ctx context.Context, host, exchange string,
) (CeleryBroker, error) {
	if strings.HasPrefix(host, "amqp://") {
		return NewAMQPCeleryBroker(ctx, host, exchange)
	} else if strings.HasPrefix(host, "redis://") {
		return NewRedisCeleryBroker(host, exchange)
	} else {
		return nil, fmt.Errorf("unsupport schema '%s'", host)
	}
}

// NewAMQPCeleryBroker creates new AMQPCeleryBroker
func NewAMQPCeleryBroker(
	ctx context.Context, host, exchange string,
) (*AMQPCeleryBroker, error) {
	conn, channel, err := NewAMQPConnection(host)
	if err != nil {
		return nil, err
	}

	if exchange == "" {
		exchange = defaultExchange
	}

	return &AMQPCeleryBroker{
		channel:      channel,
		Connection:   conn,
		Host:         host,
		ExchangeName: exchange,
	}, nil
}

func (b *AMQPCeleryBroker) Reconnect(context.Context) error {
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

// SendCeleryMessage sends CeleryMessage to broker
func (b *AMQPCeleryBroker) SendCeleryMessage(
	ctx context.Context, message *CeleryMessage,
) error {
	var (
		taskMessage = message.GetTaskMessage()
		err         error
	)

	resBytes, err := json.Marshal(taskMessage)
	if err != nil {
		return err
	}

	publishMessage := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         resBytes,
	}

	return parseAndRetry(
		ctx, "PublishWithContext", func() error {
			return b.channel.PublishWithContext(
				ctx,
				b.ExchangeName,
				b.ExchangeName,
				false,
				false,
				publishMessage,
			)
		}, b,
	)
}
