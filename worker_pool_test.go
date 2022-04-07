package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWorkerPoolMainFlow(t *testing.T) {
	worker, err := NewWorkerPool(WorkerPoolConfig{})
	assert.NoError(t, err)

	value := 0

	job := NewJobSimple(func(isCanceled chan ChanSignal, requestData interface{}, result chan *Result) {
		val, ok := requestData.(*int)
		assert.True(t, ok)
		*val++
	}, &value, nil)

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
