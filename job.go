package workers

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

type simpleJob struct {
	id      string
	jobFunc SimpleJobFunc
	params  []interface{}

	status     Status
	statusLock sync.RWMutex
	doneChan   chan ChanSignal

	ctxJobFunc        context.Context
	cancelJobFuncLock sync.RWMutex
	cancelJobFunc     context.CancelFunc
}

type SimpleJobFunc func(ctx context.Context, params ...interface{})

func NewJobSimple(
	jobFunc SimpleJobFunc, params ...interface{},
) Job {
	return &simpleJob{
		id:       uuid.NewString(),
		jobFunc:  jobFunc,
		params:   params,
		status:   StatusCreated,
		doneChan: make(chan ChanSignal),
	}
}

func (s *simpleJob) Id() string {
	return s.id
}

func (s *simpleJob) Do(ctx context.Context) {
	if s.Status() != StatusCreated {
		return
	}

	s.setStatus(StatusRunning)
	defer s.setStatus(StatusStopped)

	s.cancelJobFuncLock.Lock()
	s.ctxJobFunc, s.cancelJobFunc = context.WithCancel(ctx)
	s.cancelJobFuncLock.Unlock()

	defer func() {
		s.cancelJobFuncLock.RLock()
		if s.cancelJobFunc != nil {
			s.cancelJobFunc()
		}
		s.cancelJobFuncLock.RUnlock()
	}()

	go func(ctx context.Context) {
		s.jobFunc(ctx, s.params...)
		close(s.doneChan)
	}(s.ctxJobFunc)

	select {
	case <-s.doneChan:
	case <-ctx.Done():
	}
}

func (s *simpleJob) Cancel(ctx context.Context) {
	if s.Status() != StatusRunning {
		return
	}

	select {
	case <-s.doneChan:
	case <-ctx.Done():
		s.cancelJobFuncLock.RLock()
		if s.cancelJobFunc != nil {
			s.cancelJobFunc()
		}
		s.cancelJobFuncLock.RUnlock()
	}
}

func (s *simpleJob) Status() Status {
	s.statusLock.RLock()
	defer s.statusLock.RUnlock()
	return s.status
}

func (s *simpleJob) Done() chan ChanSignal {
	return s.doneChan
}

func (s *simpleJob) setStatus(status Status) {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()
	s.status = status
}
