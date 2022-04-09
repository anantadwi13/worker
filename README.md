# workers

[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/anantadwi13/workers/Test%20and%20coverage)](https://github.com/anantadwi13/workers/actions)
[![godoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat)](https://pkg.go.dev/github.com/anantadwi13/workers)
[![GitHub](https://img.shields.io/github/license/anantadwi13/workers)](https://raw.githubusercontent.com/anantadwi13/workers/master/LICENSE)
[![codecov](https://codecov.io/gh/anantadwi13/workers/branch/master/graph/badge.svg?token=QPUSUNBVPE)](https://codecov.io/gh/anantadwi13/workers)


Golang Worker

## Installation
```shell
go get github.com/anantadwi13/workers@latest
```

## Example
```go
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anantadwi13/workers"
)

func main() {
	shutdownSign := make(chan os.Signal)
	signal.Notify(shutdownSign, syscall.SIGINT, syscall.SIGKILL)

	log.Println("creating a worker instance")
	worker, err := workers.NewWorkerPool(workers.WorkerPoolConfig{
		QueueSize:       1,
		WorkerSize:      2,
		ShutdownTimeout: 1 * time.Second,
	})
	if err != nil {
		log.Fatalln("error:", err)
	}

	log.Println("starting up the worker")
	err = worker.Start()
	if err != nil {
		log.Fatalln("error:", err)
	}

	// job scheduling
	go func(w workers.Worker) {
		for i := 0; true; i++ {
			log.Println(i, "scheduled")

			job := workers.NewJobSimple(func(ctx context.Context, jobId string, params ...interface{}) {
				select {
				case <-time.After(2 * time.Second):
					log.Println(params[0], "successfully done")
				case <-ctx.Done():
					log.Println(params[0], "context deadline")
				}
			}, i)

			err := w.Push(job)
			if err != nil {
				log.Println("error:", i, err)
				if errors.Is(err, workers.ErrWorkerNotRunning) {
					return
				}
			}

			time.Sleep(1 * time.Second)
		}
	}(worker)

	// wait shutdown signal
	<-shutdownSign

	log.Println("shutting down the worker")
	err = worker.Shutdown()
	if err != nil {
		panic(err)
	}
	log.Println("the worker is successfully stopped")
}
```