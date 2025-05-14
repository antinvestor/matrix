package queueutil

import (
	"context"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame"
)

type QueueManager interface {
	RegisterPublisher(ctx context.Context, opts *config.QueueOptions) error
	RegisterSubscriber(ctx context.Context, opts *config.QueueOptions, handler frame.SubscribeWorker) error

	Publish(ctx context.Context, reference string, payload any) error
}
