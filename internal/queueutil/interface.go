package queueutil

import (
	"context"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame"
)

type QueueManager interface {
	RegisterPublisher(ctx context.Context, opts *config.QueueOptions) error
	GetPublisher(ref string) (frame.Publisher, error)
	Publish(ctx context.Context, reference string, payload any, headers ...map[string]string) error
	GetSubscriber(ref string) (frame.Subscriber, error)
	RegisterSubscriber(ctx context.Context, opts *config.QueueOptions, handler ...frame.SubscribeWorker) error
}
