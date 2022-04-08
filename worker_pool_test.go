package workers

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type JobWrapper struct {
	Name        string
	Wait        time.Duration
	PushAndWait bool
	Job         Job
	WantErr     bool
}

func generateDummyJob(timeout time.Duration, number *int32) Job {
	return NewJobSimple(func(isCanceled chan ChanSignal, params ...interface{}) {
		if len(params) != 1 {
			return
		}

		num, ok := params[0].(*int32)
		if !ok {
			return
		}

		select {
		case <-time.After(timeout):
			atomic.AddInt32(num, 1)
		case <-isCanceled:
		}
	}, number)
}

func TestWorkerPoolValidation(t *testing.T) {
	worker, err := NewWorkerPool(WorkerPoolConfig{
		QueueSize:       -1,
		WorkerSize:      -1,
		JobTimeout:      -1,
		ShutdownTimeout: -1,
	})
	assert.NoError(t, err)
	assert.NotNil(t, worker)

	wp, ok := worker.(*workerPool)
	assert.True(t, ok)
	assert.EqualValues(t, defaultQueueSize, wp.queueSize)
	assert.EqualValues(t, defaultWorkerSize, wp.workerSize)
}

func TestWorkerPoolMainFlow(t *testing.T) {
	worker, err := NewWorkerPool(WorkerPoolConfig{})
	assert.NoError(t, err)

	assert.Equal(t, time.Duration(0), worker.GetJobTimeout())
	assert.Equal(t, time.Duration(0), worker.GetShutdownTimeout())

	value := 0

	job := NewJobSimple(func(isCanceled chan ChanSignal, params ...interface{}) {
		if len(params) != 1 {
			return
		}

		val, ok := params[0].(*int)
		assert.True(t, ok)
		*val++
	}, &value)

	assert.NotEmpty(t, job.Id())

	err = worker.Push(job)
	assert.Error(t, err)

	err = worker.PushAndWait(job)
	assert.Error(t, err)

	err = worker.Shutdown()
	assert.Error(t, err)

	assert.Equal(t, StatusCreated, worker.Status())

	// start the worker
	err = worker.Start()
	assert.NoError(t, err)

	assert.Equal(t, StatusRunning, worker.Status())

	err = worker.Start()
	assert.Error(t, err)

	err = worker.Push(job)
	assert.NoError(t, err)

	<-job.Done()

	err = worker.PushAndWait(job)
	assert.NoError(t, err)

	// the job will not be executed twice since it's already running or done
	assert.Equal(t, 1, value)

	// shutdown the worker
	err = worker.Shutdown()
	assert.NoError(t, err)

	assert.Equal(t, StatusStopped, worker.Status())

	err = worker.Start()
	assert.Error(t, err)

	err = worker.Shutdown()
	assert.Error(t, err)
}

func TestWorkerPoolConsistency1(t *testing.T) {
	worker, err := NewWorkerPool(WorkerPoolConfig{
		QueueSize:       1,
		WorkerSize:      2,
		ShutdownTimeout: 10 * time.Millisecond,
	})
	assert.NoError(t, err)

	err = worker.Start()
	assert.NoError(t, err)

	/*
		INTERVAL 10 ms
		|a|c|d|d|f|f|f|f|f|f| | | -> worker 1
		|b|b|b|e|g|g|g|g|g|g| | | -> worker 2
		| | | | | |h|h|h|h|h| | | -> queue 1
		| | | | | | |i|i|i|i| | | -> out of queue
		| | | | | | | |j|j|j| | | -> out of queue
		| | | | | | | |k|k|k| | | -> out of queue
		| | | | | | | | |x| | | | -> shutdown trigger
	*/

	var (
		data = int32(0)

		jobs = []*JobWrapper{
			{"nil1", 0 * time.Millisecond, true, nil, true},
			{"nil2", 0 * time.Millisecond, false, nil, true},
			{"a", 0 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), false},
			{"b", 0 * time.Millisecond, false, generateDummyJob(30*time.Millisecond, &data), false},
			{"c", 10 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), false},
			{"d", 20 * time.Millisecond, false, generateDummyJob(20*time.Millisecond, &data), false},
			{"e", 30 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), false},
			{"f", 40 * time.Millisecond, false, generateDummyJob(60*time.Millisecond, &data), false},
			{"g", 40 * time.Millisecond, false, generateDummyJob(60*time.Millisecond, &data), false},
			{"h", 50 * time.Millisecond, true, generateDummyJob(20*time.Millisecond, &data), false},
			{"i", 60 * time.Millisecond, true, generateDummyJob(10*time.Millisecond, &data), true},
			{"j", 70 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), true},
			{"k", 70 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), true},
		}
	)

	go func() {
		time.Sleep(80 * time.Millisecond)
		err = worker.Shutdown()
		assert.NoError(t, err)
	}()

	for _, job := range jobs {
		go func(job *JobWrapper) {
			time.Sleep(job.Wait)

			var err error

			if job.PushAndWait {
				err = worker.PushAndWait(job.Job)
			} else {
				err = worker.Push(job.Job)
			}
			if job.WantErr {
				assert.Error(t, err, "%+v", job)
				return
			}
			assert.NoError(t, err, "%+v", job)
		}(job)
	}

	time.Sleep(120 * time.Millisecond)
	assert.Equal(t, int32(5), atomic.LoadInt32(&data))
}

func TestWorkerPoolConsistency2(t *testing.T) {
	worker, err := NewWorkerPool(WorkerPoolConfig{
		QueueSize:       2,
		WorkerSize:      2,
		ShutdownTimeout: 10 * time.Millisecond,
	})
	assert.NoError(t, err)

	err = worker.Start()
	assert.NoError(t, err)

	/*
		INTERVAL 10 ms
		|a|c|d|d|f|f|f|f|f|f| | | -> worker 1
		|b|b|b|e|g|g|g|g|g|g| | | -> worker 2
		| | | | | |h|h|h|h|h| | | -> queue 1
		| | | | | | |i|i|i|i| | | -> queue 2
		| | | | | | | |j|j|j| | | -> out of queue
		| | | | | | | |k|k|k| | | -> out of queue
		| | | | | | | | |x| | | | -> shutdown trigger
	*/

	var (
		data = int32(0)

		jobs = []*JobWrapper{
			{"a", 0 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), false},
			{"b", 0 * time.Millisecond, false, generateDummyJob(30*time.Millisecond, &data), false},
			{"c", 10 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), false},
			{"d", 20 * time.Millisecond, false, generateDummyJob(20*time.Millisecond, &data), false},
			{"e", 30 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), false},
			{"f", 40 * time.Millisecond, false, generateDummyJob(60*time.Millisecond, &data), false},
			{"g", 40 * time.Millisecond, false, generateDummyJob(60*time.Millisecond, &data), false},
			{"h", 50 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), false},
			{"i", 60 * time.Millisecond, true, generateDummyJob(10*time.Millisecond, &data), false},
			{"j", 70 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), true},
			{"k", 70 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), true},
		}
	)

	go func() {
		time.Sleep(80 * time.Millisecond)
		err = worker.Shutdown()
		assert.NoError(t, err)
	}()

	for _, job := range jobs {
		go func(job *JobWrapper) {
			time.Sleep(job.Wait)

			var err error

			if sj, ok := job.Job.(*simpleJob); ok {
				sj.id = job.Name
			}

			if job.PushAndWait {
				err = worker.PushAndWait(job.Job)
			} else {
				err = worker.Push(job.Job)
			}
			if job.WantErr {
				assert.Error(t, err, "%+v", job)
				return
			}
			assert.NoError(t, err, "%+v", job)
		}(job)
	}

	time.Sleep(120 * time.Millisecond)
	assert.Equal(t, int32(5), atomic.LoadInt32(&data))
}

func TestWorkerPoolConsistency3(t *testing.T) {
	worker, err := NewWorkerPool(WorkerPoolConfig{
		QueueSize:  2,
		WorkerSize: 2,
	})
	assert.NoError(t, err)

	err = worker.Start()
	assert.NoError(t, err)

	/*
		INTERVAL 10 ms
		|a|c|d|d|f|f|f|f|f|f|h| | -> worker 1
		|b|b|b|e|g|g|g|g|g|g|i| | -> worker 2
		| | | | | |h|h|h|h|h| | | -> queue 1
		| | | | | | |i|i|i|i| | | -> queue 2
		| | | | | | | |j|j|j| | | -> out of queue
		| | | | | | | |k|k|k| | | -> out of queue
		| | | | | | | | |x| | | | -> shutdown trigger
	*/

	var (
		data = int32(0)

		jobs = []*JobWrapper{
			{"nil1", 0 * time.Millisecond, true, nil, true},
			{"nil2", 0 * time.Millisecond, false, nil, true},
			{"a", 0 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), false},
			{"b", 0 * time.Millisecond, false, generateDummyJob(30*time.Millisecond, &data), false},
			{"c", 10 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), false},
			{"d", 20 * time.Millisecond, false, generateDummyJob(20*time.Millisecond, &data), false},
			{"e", 30 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), false},
			{"f", 40 * time.Millisecond, false, generateDummyJob(60*time.Millisecond, &data), false},
			{"g", 40 * time.Millisecond, false, generateDummyJob(60*time.Millisecond, &data), false},
			{"h", 50 * time.Millisecond, true, generateDummyJob(10*time.Millisecond, &data), false},
			{"i", 60 * time.Millisecond, true, generateDummyJob(10*time.Millisecond, &data), false},
			{"j", 70 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), true},
			{"k", 70 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), true},
		}
	)

	go func() {
		time.Sleep(80 * time.Millisecond)
		err = worker.Shutdown()
		assert.NoError(t, err)
	}()

	for _, job := range jobs {
		go func(job *JobWrapper) {
			time.Sleep(job.Wait)

			var err error

			if sj, ok := job.Job.(*simpleJob); ok {
				sj.id = job.Name
			}

			if job.PushAndWait {
				err = worker.PushAndWait(job.Job)
			} else {
				err = worker.Push(job.Job)
			}
			if job.WantErr {
				assert.Error(t, err, "%+v", job)
				return
			}
			assert.NoError(t, err, "%+v", job)
		}(job)
	}

	time.Sleep(120 * time.Millisecond)
	assert.Equal(t, int32(9), atomic.LoadInt32(&data))
}

func TestWorkerPoolConsistency4(t *testing.T) {
	worker, err := NewWorkerPool(WorkerPoolConfig{
		QueueSize:  1,
		WorkerSize: 2,
		JobTimeout: 70 * time.Millisecond,
	})
	assert.NoError(t, err)

	err = worker.Start()
	assert.NoError(t, err)

	/*
		INTERVAL 10 ms
		|a|c|d|d|f|f|f|f|f|f|f|f| | -> worker 1
		|b|b|b|e|g|g|g|g|g|g|h|h| | -> worker 2
		| | | | | |h|h|h|h|h| | | | -> queue 1
		| | | | | | |i|i|i|i| | | | -> out of queue
		| | | | | | | |j|j|j| | | | -> out of queue
		| | | | | | | |k|k|k| | | | -> out of queue
		| | | | | | | | |x| | | | | -> shutdown trigger

		f job timeout > 70 ms, then it should not be counted
	*/

	var (
		data = int32(0)

		jobs = []*JobWrapper{
			{"nil1", 0 * time.Millisecond, true, nil, true},
			{"nil2", 0 * time.Millisecond, false, nil, true},
			{"a", 0 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), false},
			{"b", 0 * time.Millisecond, false, generateDummyJob(30*time.Millisecond, &data), false},
			{"c", 10 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), false},
			{"d", 20 * time.Millisecond, false, generateDummyJob(20*time.Millisecond, &data), false},
			{"e", 30 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), false},
			{"f", 40 * time.Millisecond, false, generateDummyJob(80*time.Millisecond, &data), false},
			{"g", 40 * time.Millisecond, false, generateDummyJob(60*time.Millisecond, &data), false},
			{"h", 50 * time.Millisecond, true, generateDummyJob(20*time.Millisecond, &data), false},
			{"i", 60 * time.Millisecond, true, generateDummyJob(10*time.Millisecond, &data), true},
			{"j", 70 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), true},
			{"k", 70 * time.Millisecond, false, generateDummyJob(10*time.Millisecond, &data), true},
		}
	)

	go func() {
		time.Sleep(80 * time.Millisecond)
		err = worker.Shutdown()
		assert.NoError(t, err)
	}()

	for _, job := range jobs {
		go func(job *JobWrapper) {
			time.Sleep(job.Wait)

			var err error

			if job.PushAndWait {
				err = worker.PushAndWait(job.Job)
			} else {
				err = worker.Push(job.Job)
			}
			if job.WantErr {
				assert.Error(t, err, "%+v", job)
				return
			}
			assert.NoError(t, err, "%+v", job)
		}(job)
	}

	time.Sleep(130 * time.Millisecond)
	assert.Equal(t, int32(7), atomic.LoadInt32(&data))
}
