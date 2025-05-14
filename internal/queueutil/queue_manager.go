package queueutil

import (
	"context"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame"
)

type queues struct {
	service *frame.Service
}

func (c *queues) RegisterPublisher(ctx context.Context, opts *config.QueueOptions) error {
	return c.service.AddPublisher(ctx, opts.Reference, string(opts.ConnectionString))
}
func (c *queues) RegisterSubscriber(_ context.Context, opts *config.QueueOptions, handler frame.SubscribeWorker) error {
	dbOpts := frame.RegisterSubscriber(opts.Reference, string(opts.ConnectionString), opts.Concurrency, handler)
	c.service.Init(dbOpts)
	return nil
}

func (c *queues) Publish(ctx context.Context, reference string, payload any) error {
	return c.service.Publish(ctx, reference, payload)
}

// NewQueueManager ensures a default internal Queue exists
func NewQueueManager(service *frame.Service) QueueManager {
	return &queues{
		service: service,
	}
}
