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
	cancelChan chan ChanSignal
}

type SimpleJobFunc func(isCanceled chan ChanSignal, params ...interface{})

func NewJobSimple(
	jobFunc SimpleJobFunc, params ...interface{},
) Job {
	return &simpleJob{
		id:         uuid.NewString(),
		jobFunc:    jobFunc,
		params:     params,
		status:     StatusCreated,
		cancelChan: make(chan ChanSignal, 2),
		doneChan:   make(chan ChanSignal),
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

	go func() {
		s.jobFunc(s.cancelChan, s.params...)
		close(s.doneChan)
	}()

	select {
	case <-s.doneChan:
	case <-ctx.Done():
		s.cancelChan <- ChanSignal{}
	}
}

func (s *simpleJob) Cancel(ctx context.Context) {
	if s.Status() != StatusRunning {
		return
	}

	select {
	case <-s.doneChan:
	case <-ctx.Done():
		s.cancelChan <- ChanSignal{}
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
