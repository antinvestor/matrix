package queueutil

import (
	"context"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame"
)

type QueueManager interface {
	WorkPookManager
	RegisterPublisher(ctx context.Context, opts *config.QueueOptions) error
	GetPublisher(ref string) (frame.Publisher, error)

	DiscardPublisher(ctx context.Context, ref string) error

	EnsurePublisherOk(ctx context.Context, opts *config.QueueOptions) error
	Publish(ctx context.Context, reference string, payload any, headers ...map[string]string) error
	GetSubscriber(ref string) (frame.Subscriber, error)

	DiscardSubscriber(ctx context.Context, ref string) error
	RegisterSubscriber(ctx context.Context, opts *config.QueueOptions, handler ...frame.SubscribeWorker) error
}

type WorkPookManager interface {
	Submit(ctx context.Context, job frame.Job)
}
