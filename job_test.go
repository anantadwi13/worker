package workers

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSimpleJobNormal(t *testing.T) {
	// test normal flow

	var (
		data = int32(0)
		ctx  = context.Background()
	)

	job := NewJobSimple(func(ctx context.Context, params ...interface{}) {
		if len(params) != 1 {
			return
		}

		select {
		case <-time.After(10 * time.Millisecond):
			atomic.AddInt32(params[0].(*int32), 1)
		case <-ctx.Done():
		}
	}, &data)

	assert.NotEmpty(t, job.Id())
	assert.Equal(t, StatusCreated, job.Status())

	job.Cancel(ctx)
	assert.Equal(t, StatusCreated, job.Status())

	go func() {
		job.Do(ctx)
	}()
	go func() {
		time.Sleep(5 * time.Millisecond)
		assert.Equal(t, StatusRunning, job.Status())
	}()

	go func() {
		time.Sleep(15 * time.Millisecond)
		assert.Equal(t, StatusStopped, job.Status())
	}()

	time.Sleep(20 * time.Millisecond)
	assert.EqualValues(t, 1, atomic.LoadInt32(&data))
}

func TestSimpleJobCancellation(t *testing.T) {
	// test job cancellation

	var (
		data = int32(0)
		ctx  = context.Background()
	)

	job := NewJobSimple(func(ctx context.Context, params ...interface{}) {
		if len(params) != 1 {
			return
		}

		select {
		case <-time.After(20 * time.Millisecond):
			atomic.AddInt32(params[0].(*int32), 1)
		case <-ctx.Done():
		}
	}, &data)

	assert.NotEmpty(t, job.Id())
	assert.Equal(t, StatusCreated, job.Status())

	job.Cancel(ctx)
	assert.Equal(t, StatusCreated, job.Status())

	go func() {
		job.Do(ctx)
	}()
	go func() {
		time.Sleep(5 * time.Millisecond)
		assert.Equal(t, StatusRunning, job.Status())
	}()

	go func() {
		time.Sleep(25 * time.Millisecond)
		assert.Equal(t, StatusStopped, job.Status())
	}()

	go func() {
		time.Sleep(15 * time.Millisecond)
		assert.Equal(t, StatusStopped, job.Status())
	}()

	go func() {
		time.Sleep(7 * time.Millisecond)
		assert.Equal(t, StatusRunning, job.Status())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()
		job.Cancel(ctx)
		time.Sleep(1 * time.Millisecond)
		assert.Equal(t, StatusStopped, job.Status())
	}()

	time.Sleep(30 * time.Millisecond)
	assert.EqualValues(t, 0, atomic.LoadInt32(&data))
}

func TestSimpleJobDeadline(t *testing.T) {
	// test job deadline

	var (
		data = int32(0)
		ctx  = context.Background()
	)

	job := NewJobSimple(func(ctx context.Context, params ...interface{}) {
		if len(params) != 1 {
			return
		}

		select {
		case <-time.After(20 * time.Millisecond):
			atomic.AddInt32(params[0].(*int32), 1)
		case <-ctx.Done():
		}
	}, &data)

	assert.NotEmpty(t, job.Id())
	assert.Equal(t, StatusCreated, job.Status())

	job.Cancel(ctx)
	assert.Equal(t, StatusCreated, job.Status())

	go func() {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
		job.Do(ctx)
	}()
	go func() {
		time.Sleep(5 * time.Millisecond)
		assert.Equal(t, StatusRunning, job.Status())
	}()

	go func() {
		time.Sleep(15 * time.Millisecond)
		assert.Equal(t, StatusStopped, job.Status())
	}()

	time.Sleep(20 * time.Millisecond)
	assert.EqualValues(t, 0, atomic.LoadInt32(&data))
}
