package workers

import (
	"context"
	"sync"
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

type JobConfig struct {
	Id               string
	ExecutedAt       int
	Duration         int
	PushAndWait      bool
	SuccessExecution bool
	ErrScheduling    bool
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

	job := NewJobSimple(func(ctx context.Context, jobId string, params ...interface{}) {
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

func TestWorkerPoolConsistency(t *testing.T) {
	type Test struct {
		Name         string
		WorkerConfig WorkerPoolConfig
		JobConfigs   []*JobConfig
		TimeUnit     time.Duration
		ShutdownAt   int
	}

	tests := []Test{
		{
			Name: "test 1",
			WorkerConfig: WorkerPoolConfig{
				QueueSize:       1,
				WorkerSize:      2,
				ShutdownTimeout: 2,
			},
			JobConfigs: []*JobConfig{
				/*
					INTERVAL 10 ms
					|a|c|d|d|f|f|f|f|f|h|h| | -> worker 1
					|b|b|b|e|g|g|g|g|g|g|g| | -> worker 2
					| | | | | |h|h|h|h| | | | -> queue 1
					| | | | | | |i|i|i| | | | -> out of queue
					| | | | | | | |j|j| | | | -> out of queue
					| | | | | | | |k|k| | | | -> out of queue
					| | | | | | | | |x|x| | | -> shutdown trigger
				*/
				{"a", 0, 1, false, true, false},
				{"b", 0, 3, false, true, false},
				{"c", 1, 1, false, true, false},
				{"d", 2, 2, false, true, false},
				{"e", 3, 1, false, true, false},
				{"f", 4, 5, false, true, false},
				{"g", 4, 7, false, false, false},
				{"h", 5, 2, true, false, false},
				{"i", 6, 1, true, false, true},
				{"j", 7, 1, false, false, true},
				{"k", 7, 1, false, false, true},
			},
			TimeUnit:   10 * time.Millisecond,
			ShutdownAt: 8,
		},
		{
			Name: "test 2",
			WorkerConfig: WorkerPoolConfig{
				QueueSize:       2,
				WorkerSize:      2,
				ShutdownTimeout: 1,
			},
			JobConfigs: []*JobConfig{
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
				{"a", 0, 1, false, true, false},
				{"b", 0, 3, false, true, false},
				{"c", 1, 1, false, true, false},
				{"d", 2, 2, false, true, false},
				{"e", 3, 1, false, true, false},
				{"f", 4, 6, false, false, false},
				{"g", 4, 6, false, false, false},
				{"h", 5, 1, false, false, false},
				{"i", 6, 1, true, false, false},
				{"j", 7, 1, false, false, true},
				{"k", 7, 1, false, false, true},
			},
			TimeUnit:   10 * time.Millisecond,
			ShutdownAt: 8,
		},
		{
			Name: "test 3 without shutdown timeout",
			WorkerConfig: WorkerPoolConfig{
				QueueSize:  2,
				WorkerSize: 2,
			},
			JobConfigs: []*JobConfig{
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
				{"a", 0, 1, false, true, false},
				{"b", 0, 3, false, true, false},
				{"c", 1, 1, false, true, false},
				{"d", 2, 2, false, true, false},
				{"e", 3, 1, false, true, false},
				{"f", 4, 6, false, true, false},
				{"g", 4, 6, false, true, false},
				{"h", 5, 1, true, true, false},
				{"i", 6, 1, true, true, false},
				{"j", 7, 1, false, false, true},
				{"k", 7, 1, false, false, true},
			},
			TimeUnit:   10 * time.Millisecond,
			ShutdownAt: 8,
		},
		{
			Name: "test 4 with job timeout",
			WorkerConfig: WorkerPoolConfig{
				QueueSize:  1,
				WorkerSize: 2,
				JobTimeout: 7,
			},
			JobConfigs: []*JobConfig{
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
				{"a", 0, 1, false, true, false},
				{"b", 0, 3, false, true, false},
				{"c", 1, 1, false, true, false},
				{"d", 2, 2, false, true, false},
				{"e", 3, 1, false, true, false},
				{"f", 4, 8, false, false, false},
				{"g", 4, 6, false, true, false},
				{"h", 5, 2, true, true, false},
				{"i", 6, 1, true, false, true},
				{"j", 7, 1, false, false, true},
				{"k", 7, 1, false, false, true},
			},
			TimeUnit:   10 * time.Millisecond,
			ShutdownAt: 8,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			test.WorkerConfig.JobTimeout *= test.TimeUnit
			test.WorkerConfig.ShutdownTimeout *= test.TimeUnit

			worker, err := NewWorkerPool(test.WorkerConfig)
			assert.NoError(tt, err)

			var (
				actualSuccessJob   = &sync.Map{}
				expectedSuccessJob = make(map[string]bool, len(test.JobConfigs))
				jobWrappers        []*JobWrapper
			)

			for _, config := range test.JobConfigs {
				job := NewJobSimple(func(ctx context.Context, jobId string, params ...interface{}) {
					if len(params) != 2 {
						return
					}

					jobDuration, ok := params[0].(time.Duration)
					if !ok {
						return
					}
					successJob, ok := params[1].(*sync.Map)
					if !ok {
						return
					}

					select {
					case <-time.After(jobDuration):
						successJob.Store(jobId, true)
					case <-ctx.Done():
						successJob.Store(jobId, false)
					}
				}, time.Duration(config.Duration)*test.TimeUnit, actualSuccessJob)

				if sj, ok := job.(*simpleJob); ok {
					sj.id = config.Id
				}

				jobWrappers = append(jobWrappers, &JobWrapper{
					Name:        config.Id,
					Wait:        time.Duration(config.ExecutedAt) * test.TimeUnit,
					PushAndWait: config.PushAndWait,
					Job:         job,
					WantErr:     config.ErrScheduling,
				})
				expectedSuccessJob[config.Id] = config.SuccessExecution
			}

			jobWrappers = append(jobWrappers, &JobWrapper{
				Name:        "nil job 1",
				Wait:        time.Duration(0) * test.TimeUnit,
				PushAndWait: false,
				Job:         nil,
				WantErr:     true,
			})
			jobWrappers = append(jobWrappers, &JobWrapper{
				Name:        "nil job 2",
				Wait:        time.Duration(0) * test.TimeUnit,
				PushAndWait: true,
				Job:         nil,
				WantErr:     true,
			})

			err = worker.Start()
			assert.NoError(tt, err)

			go func() {
				time.Sleep(time.Duration(test.ShutdownAt) * test.TimeUnit)
				err = worker.Shutdown()
				assert.NoError(tt, err)
			}()

			// job scheduling

			for _, jobWrapper := range jobWrappers {
				go func(jobWrapper *JobWrapper) {
					time.Sleep(jobWrapper.Wait)

					var err error

					if jobWrapper.PushAndWait {
						err = worker.PushAndWait(jobWrapper.Job)
					} else {
						err = worker.Push(jobWrapper.Job)
					}

					if jobWrapper.WantErr {
						assert.Error(tt, err, "%+v", jobWrapper)
						return
					}
					assert.NoError(tt, err, "%+v", jobWrapper)
				}(jobWrapper)
			}

			<-worker.Done()

			// validation

			successCount := 0
			actualSuccessJob.Range(func(key, value interface{}) bool {
				if value.(bool) {
					successCount++
				}
				assert.EqualValues(tt, expectedSuccessJob[key.(string)], value, "jobId %s", key)
				return true
			})
			tt.Log("success count", successCount)
		})
	}
}
