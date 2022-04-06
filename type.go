package worker

import "context"

type Job interface {
	Id() string
	// ctx contains a job timeout
	Do(ctx context.Context)
	// Cancel will block the process until the job is gracefully canceled. ctx contains a cancellation deadline
	Cancel(ctx context.Context)
}

type Worker interface {
	Start() error
	Shutdown() error

	// GetJobTimeout returns a timeout in seconds
	GetJobTimeout() int
	// GetShutdownTimeout returns a timeout in seconds
	GetShutdownTimeout() int

	Push(job Job) error
	PushAndWait(job Job) error
}

type IsCanceled struct{}
