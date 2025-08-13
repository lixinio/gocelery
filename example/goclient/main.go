// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/lixinio/gocelery"
)

func WaitSignal(stop chan struct{}) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	close(stop)
}

func watchSignal(cancel context.CancelFunc) {
	stop := make(chan struct{})
	WaitSignal(stop)

	<-stop
	cancel()
}

// Run Celery Worker First!
// celery -A worker worker --loglevel=debug --without-heartbeat --without-mingle
func main() {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel() // 确保最终会取消（即使未捕获到信号）

	// 2. 启动一个依赖上下文的示例任务（模拟业务逻辑）
	go watchSignal(cancel)

	url := "redis://localhost:6379/0"
	if len(os.Args) > 1 {
		url = "amqp://guest:guest@localhost:5672/"
	}

	broker, err := gocelery.NewCeleryBroker(ctx, url, "")
	if err != nil {
		panic(err)
	}

	backend, err := gocelery.NewCeleryBackend(url)
	if err != nil {
		panic(err)
	}

	// initialize celery client
	cli := gocelery.NewCeleryClient(broker, backend)

	// prepare arguments
	taskName := "worker.add"

	for i := 0; i < 50; i++ {
		argA := i + 1
		argB := rand.Intn(10)

		log.Printf("start invoke %d + %d", argA, argB)

		// run task
		asyncResult, err := cli.Delay(ctx, taskName, argA, argB)
		if err != nil {
			panic(err)
		}

		log.Printf("start invoke %d + %d success, task %s", argA, argB, asyncResult.TaskID)

		time.Sleep(time.Second)

		// get results from backend with timeout
		res, err := asyncResult.Get(ctx, 10*time.Second)
		if err != nil {
			panic(err)
		}

		log.Printf(
			"%d + %d = result: %+v of type %+v",
			argA, argB, res, reflect.TypeOf(res),
		)
	}
}
