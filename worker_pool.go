package worker

import (
	"context"
	"runtime"
	"sync"
	"time"
)

type WorkerPoolConfig struct {
	QueueSize  int
	WorkerSize int

	// timeout in seconds
	JobTimeout      int
	ShutdownTimeout int
}

type workerPool struct {
	jobQueue        chan Job
	workerSize      int
	jobTimeout      int
	shutdownTimeout int

	workerCancellationLock sync.Mutex
	workerCancellationFunc []func()
}

func NewWorkerPool(config WorkerPoolConfig) (Worker, error) {
	if config.QueueSize <= 0 {
		config.QueueSize = 1024
	}
	if config.WorkerSize <= 0 {
		config.WorkerSize = runtime.NumCPU()
	}

	if config.JobTimeout < 0 {
		config.JobTimeout = 0
	}

	if config.ShutdownTimeout < 0 {
		config.ShutdownTimeout = 0
	}

	return &workerPool{
		jobQueue:        make(chan Job, config.QueueSize),
		workerSize:      config.WorkerSize,
		jobTimeout:      config.JobTimeout,
		shutdownTimeout: config.ShutdownTimeout,
	}, nil
}

func (w *workerPool) Start() error {
	w.initWorker()
	return nil
}

func (w *workerPool) Shutdown() error {
	w.stopWorker()
	return nil
}

func (w *workerPool) GetJobTimeout() int {
	return w.jobTimeout
}

func (w *workerPool) GetShutdownTimeout() int {
	return w.shutdownTimeout
}

func (w *workerPool) Push(job Job) error {
	w.jobQueue <- job
	return nil
}

func (w *workerPool) PushAndWait(job Job) error {
	//TODO implement me
	panic("implement me")
}

func (w *workerPool) initWorker() {
	w.workerCancellationLock.Lock()
	defer w.workerCancellationLock.Unlock()

	for i := 0; i < w.workerSize; i++ {
		cancelChan := make(chan IsCanceled)
		cancelFunc := func() {
			close(cancelChan)
		}

		go w.jobDispatcher(cancelChan)

		w.workerCancellationFunc = append(w.workerCancellationFunc, cancelFunc)
	}
}

func (w *workerPool) stopWorker() {
	w.workerCancellationLock.Lock()
	defer w.workerCancellationLock.Unlock()

	for _, f := range w.workerCancellationFunc {
		f()
	}
	w.workerCancellationFunc = nil
}

func (w *workerPool) jobDispatcher(cancelChan chan IsCanceled) {
	for {
		select {
		case job := <-w.jobQueue:
			done := make(chan struct{})

			go func() {
				var (
					ctx        = context.Background()
					cancelFunc context.CancelFunc
				)
				if w.jobTimeout > 0 {
					ctx, cancelFunc = context.WithTimeout(ctx, time.Duration(w.jobTimeout)*time.Second)
				}

				job.Do(ctx)

				if cancelFunc != nil {
					cancelFunc()
				}
				close(done)
			}()

			select {
			case <-done:
				// the job is done
			case <-cancelChan:
				// on graceful shutdown
				ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(w.shutdownTimeout)*time.Second)

				job.Cancel(ctx)

				cancelFunc()
				return
			}
		case <-cancelChan:
			return
		}
	}
}
