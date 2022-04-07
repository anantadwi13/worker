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

	wgShutdown   sync.WaitGroup
	shutdownChan chan ChanSignal
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
		shutdownChan:    make(chan ChanSignal),
	}, nil
}

func (w *workerPool) Start() error {
	w.statusLock.Lock()
	defer w.statusLock.Unlock()

	if w.status != StatusCreated {
		return errors.New("worker has been running or already stopped")
	}
	w.status = StatusRunning

	for i := 0; i < w.workerSize; i++ {
		w.wgShutdown.Add(1)
		go w.looper()
	}
	return nil
}

func (w *workerPool) Shutdown() error {
	w.statusLock.Lock()
	defer w.statusLock.Unlock()

	if w.status != StatusRunning {
		return errors.New("worker is not running")
	}
	w.status = StatusStopped

	close(w.shutdownChan)

	w.wgShutdown.Wait()
	return nil
}

func (w *workerPool) Status() Status {
	w.statusLock.RLock()
	defer w.statusLock.RUnlock()
	return w.status
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
	select {
	case <-w.shutdownChan:
		return errors.New("worker has been stopped")
	case w.jobQueue <- job:
	}
	return nil
}

func (w *workerPool) PushAndWait(job Job) error {
	if w.Status() != StatusRunning {
		return errors.New("worker is not running")
	}
	if job == nil {
		return errors.New("job is nil")
	}
	select {
	case <-w.shutdownChan:
		return errors.New("worker has been stopped")
	case w.jobQueue <- job:
	}
	<-job.Done()
	return nil
}

func (w *workerPool) looper() {
	defer w.wgShutdown.Done()

	var jobTimeout *time.Duration

	if w.jobTimeout > 0 {
		jobTimeout = ptrDuration(w.jobTimeout)
	}

	defer func() {
		for {
			select {
			case job := <-w.jobQueue:
				if w.shutdownTimeout > 0 {
					// force stop by setting timeout to 0
					go w.jobDispatcher(job, nil, false, ptrDuration(0))
				} else {
					// run the remaining jobs
					w.jobDispatcher(job, nil, false, jobTimeout)
				}
			default:
				return
			}
		}
	}()

	for {
		var (
			job Job
			ok  bool
		)

		select {
		case <-w.shutdownChan:
			return
		case job, ok = <-w.jobQueue:
		}

		if !ok {
			break
		}
		if job == nil {
			continue
		}

		done := make(chan ChanSignal)

		go w.jobDispatcher(job, done, false, jobTimeout)

		select {
		case <-done:
			// the job is done
		case <-w.shutdownChan:
			// on graceful shutdown
			if w.shutdownTimeout > 0 {
				select {
				case <-done:
				case <-time.After(w.shutdownTimeout):
					w.jobDispatcher(job, nil, true, ptrDuration(0))
				}
			} else {
				<-done
			}
		}
	}
}

func (w *workerPool) jobDispatcher(job Job, done chan ChanSignal, isCancellation bool, timeout *time.Duration) {
	var (
		ctx        = context.Background()
		cancelFunc context.CancelFunc
	)
	if timeout != nil {
		ctx, cancelFunc = context.WithTimeout(ctx, *timeout)
	}

	if isCancellation {
		job.Cancel(ctx)
	} else {
		job.Do(ctx)
	}

	if cancelFunc != nil {
		cancelFunc()
	}
	if done != nil {
		close(done)
	}
}
