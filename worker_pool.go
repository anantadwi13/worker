package workers

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"
)

type WorkerPoolConfig struct {
	QueueSize  int
	WorkerSize int

	// 0 for infinity time
	JobTimeout      time.Duration
	ShutdownTimeout time.Duration
}

type workerPool struct {
	jobQueue        chan Job
	workerSize      int
	jobTimeout      time.Duration
	shutdownTimeout time.Duration

	status     Status
	statusLock sync.RWMutex

	wgShutdown sync.WaitGroup

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
		status:          StatusCreated,
	}, nil
}

func (w *workerPool) Start() error {
	if w.Status() != StatusCreated {
		return errors.New("worker has been running or already stopped")
	}
	w.initWorker()
	return nil
}

func (w *workerPool) Shutdown() error {
	if w.Status() != StatusRunning {
		return errors.New("worker is not running")
	}
	w.stopWorker()
	return nil
}

func (w *workerPool) Status() Status {
	w.statusLock.RLock()
	defer w.statusLock.RUnlock()
	return w.status
}

func (w *workerPool) setStatus(status Status) {
	w.statusLock.Lock()
	defer w.statusLock.Unlock()
	w.status = status
}

func (w *workerPool) GetJobTimeout() time.Duration {
	return w.jobTimeout
}

func (w *workerPool) GetShutdownTimeout() time.Duration {
	return w.shutdownTimeout
}

func (w *workerPool) Push(job Job) error {
	if w.Status() != StatusRunning {
		return errors.New("worker is not running")
	}
	if job == nil {
		return errors.New("job is nil")
	}
	w.jobQueue <- job
	return nil
}

func (w *workerPool) PushAndWait(job Job) error {
	if w.Status() != StatusRunning {
		return errors.New("worker is not running")
	}
	if job == nil {
		return errors.New("job is nil")
	}
	w.jobQueue <- job
	<-job.Done()
	return nil
}

func (w *workerPool) initWorker() {
	w.workerCancellationLock.Lock()
	defer w.workerCancellationLock.Unlock()

	for i := 0; i < w.workerSize; i++ {
		w.wgShutdown.Add(1)
		cancelChan := make(chan ChanSignal)
		cancelFunc := func() {
			close(cancelChan)
		}

		go w.jobDispatcher(cancelChan)

		w.workerCancellationFunc = append(w.workerCancellationFunc, cancelFunc)
	}
	w.setStatus(StatusRunning)
}

func (w *workerPool) stopWorker() {
	w.workerCancellationLock.Lock()
	defer w.workerCancellationLock.Unlock()

	w.setStatus(StatusStopped)

	for _, f := range w.workerCancellationFunc {
		f()
	}
	w.workerCancellationFunc = nil

	close(w.jobQueue)

	w.wgShutdown.Wait()
}

func (w *workerPool) jobDispatcher(cancelChan chan ChanSignal) {
	defer func() {
		for job := range w.jobQueue {
			ctx, cancel := context.WithTimeout(context.Background(), 0)
			job.Do(ctx)
			cancel()
		}
		w.wgShutdown.Done()
	}()

	for {
		select {
		case job := <-w.jobQueue:
			if job == nil {
				continue
			}

			done := make(chan ChanSignal)

			go func() {
				var (
					ctx        = context.Background()
					cancelFunc context.CancelFunc
				)
				if w.jobTimeout > 0 {
					ctx, cancelFunc = context.WithTimeout(ctx, w.jobTimeout)
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
				if w.shutdownTimeout > 0 {
					select {
					case <-done:
					case <-time.After(w.shutdownTimeout):
						ctx, cancel := context.WithTimeout(context.Background(), 0)
						job.Cancel(ctx)
						cancel()
					}
				} else {
					<-done
				}
				return
			}
		case <-cancelChan:
			return
		}
	}
}
