package queueutil

import (
	"context"
	"fmt"

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

func NewWorkManagerWithContext[T any](ctx context.Context) (WorkPoolManager[T], error) {

	svc := frame.Svc(ctx)
	if svc == nil {
		return nil, fmt.Errorf("supplied context does not have service")
	}

	return NewWorkManager[T](svc), nil
}
