package workers

import (
	"context"
	"time"
)

type ChanSignal struct{}
type Status int

const (
	StatusCreated Status = iota
	StatusRunning
	StatusStopped
)

type Job interface {
	Id() string
	Status() Status
	Done() chan ChanSignal

	// Func below should be called by worker

	// Do should be blocking the process until the job is finished or canceled. ctx contains a job timeout
	Do(ctx context.Context)
	// Cancel should be blocking the process until the job is gracefully canceled. ctx contains a cancellation deadline
	Cancel(ctx context.Context)
}

type Worker interface {
	Start() error
	Shutdown() error
	Status() Status

	GetJobTimeout() time.Duration
	GetShutdownTimeout() time.Duration

	Push(job Job) error
	PushAndWait(job Job) error
}
