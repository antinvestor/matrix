package queueutil

import (
	"context"
	"github.com/pitabwire/frame"
)

type worker[T any] struct {
	service *frame.Service
}

func (w *worker[T]) Submit(ctx context.Context, job frame.Job[T]) {
	err := frame.SubmitJob(ctx, w.service, job)
	if err != nil {
		_ = job.WriteError(ctx, err)
		job.Close()
	}
}

// NewWorkManager creates a new internal Queue worker
func NewWorkManager[T any](service *frame.Service) WorkPoolManager[T] {
	return &worker[T]{
		service: service,
	}
}
