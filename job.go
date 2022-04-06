package worker

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

type simpleJob struct {
	id         string
	jobFunc    JobFunc
	resultChan chan *Result

	cancelChan chan IsCanceled
	wg         sync.WaitGroup
}

type Result struct {
	Data  interface{}
	Error error
}

type JobFunc func(isCanceled chan IsCanceled, result chan *Result)

func NewJobSimple(
	jobFunc JobFunc, result chan *Result,
) Job {
	return &simpleJob{
		id:         uuid.NewString(),
		jobFunc:    jobFunc,
		resultChan: result,
		cancelChan: make(chan IsCanceled, 2),
	}
}

func (s *simpleJob) Id() string {
	return s.id
}

func (s *simpleJob) Do(ctx context.Context) {
	done := make(chan struct{})

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		s.jobFunc(s.cancelChan, s.resultChan)
		close(done)
	}()

	select {
	case <-done:
		s.wg.Wait()
	case <-ctx.Done():
		s.cancelChan <- struct{}{}
	}
}

func (s *simpleJob) Cancel(ctx context.Context) {
	done := make(chan struct{})

	go func() {
		s.cancelChan <- struct{}{}
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}
