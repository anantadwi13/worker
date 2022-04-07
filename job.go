package workers

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

type simpleJob struct {
	id          string
	jobFunc     JobFunc
	requestData interface{}
	resultChan  chan *Result

	status     Status
	statusLock sync.RWMutex
	doneChan   chan ChanSignal
	cancelChan chan ChanSignal
	wg         sync.WaitGroup
}

type Result struct {
	Data  interface{}
	Error error
}

type JobFunc func(isCanceled chan ChanSignal, requestData interface{}, result chan *Result)

func NewJobSimple(
	jobFunc JobFunc, requestData interface{}, result chan *Result,
) Job {
	return &simpleJob{
		id:          uuid.NewString(),
		jobFunc:     jobFunc,
		requestData: requestData,
		resultChan:  result,
		status:      StatusCreated,
		cancelChan:  make(chan ChanSignal, 2),
		doneChan:    make(chan ChanSignal),
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

	done := make(chan ChanSignal)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		s.jobFunc(s.cancelChan, s.requestData, s.resultChan)
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		s.cancelChan <- ChanSignal{}
	}

	s.wg.Wait()
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

	s.wg.Wait()
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
	if s.status == StatusStopped {
		close(s.doneChan)
	}
}
