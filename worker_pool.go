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
	queueSize       int
	jobTimeout      time.Duration
	shutdownTimeout time.Duration

	status     Status
	statusLock sync.RWMutex

	lifecycleLock sync.Mutex

	wgShutdown   sync.WaitGroup
	shutdownChan chan ChanSignal
}

var (
	defaultQueueSize  = 1024
	defaultWorkerSize = runtime.NumCPU()
)

func NewWorkerPool(config WorkerPoolConfig) (Worker, error) {
	if config.QueueSize <= 0 {
		config.QueueSize = defaultQueueSize
	}
	if config.WorkerSize <= 0 {
		config.WorkerSize = defaultWorkerSize
	}

	if config.JobTimeout < 0 {
		config.JobTimeout = 0
	}

	if config.ShutdownTimeout < 0 {
		config.ShutdownTimeout = 0
	}

	return &workerPool{
		queueSize:       config.QueueSize,
		jobQueue:        make(chan Job, config.QueueSize),
		workerSize:      config.WorkerSize,
		jobTimeout:      config.JobTimeout,
		shutdownTimeout: config.ShutdownTimeout,
		status:          StatusCreated,
		shutdownChan:    make(chan ChanSignal),
	}, nil
}

func (w *workerPool) Start() error {
	w.lifecycleLock.Lock()
	defer w.lifecycleLock.Unlock()

	w.statusLock.Lock()
	if w.status != StatusCreated {
		w.statusLock.Unlock()
		return errors.New("worker has been running or already stopped")
	}
	w.status = StatusRunning
	w.statusLock.Unlock()

	for i := 0; i < w.workerSize; i++ {
		w.wgShutdown.Add(1)
		go w.looper(i)
	}
	return nil
}

func (w *workerPool) Shutdown() error {
	w.lifecycleLock.Lock()
	defer w.lifecycleLock.Unlock()

	w.statusLock.Lock()
	if w.status != StatusRunning {
		w.statusLock.Unlock()
		return errors.New("worker is not running")
	}
	w.status = StatusStopped
	w.statusLock.Unlock()

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

func (w *workerPool) looper(workerId int) {
	defer w.wgShutdown.Done()

	var (
		jobTimeout   *time.Duration
		jobQueuePrio = make(chan Job, w.queueSize)
	)

	if w.jobTimeout > 0 {
		jobTimeout = ptrDuration(w.jobTimeout)
	}

	defer w.jobDeferDispatcher(w.jobQueue, jobTimeout)
	defer w.jobDeferDispatcher(jobQueuePrio, jobTimeout)

	for {
		var job Job

		select {
		case <-w.shutdownChan:
			return
		case job = <-w.jobQueue:
		}

		if w.Status() != StatusRunning {
			// in case there is problem with shutdownChan
			jobQueuePrio <- job
			return
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
func (w *workerPool) jobDeferDispatcher(jobQueue chan Job, jobTimeout *time.Duration) {
	for {
		select {
		case job := <-jobQueue:
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
}
