package queueutil

import (
	"context"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame"
	"regexp"
)

type queues struct {
	service *frame.Service
}

func (c *queues) RegisterPublisher(ctx context.Context, opts *config.QueueOptions) error {
	return c.service.AddPublisher(ctx, opts.Ref(), string(opts.DS))
}

func (c *queues) GetPublisher(ref string) (frame.Publisher, error) {
	return c.service.GetPublisher(ref)
}

func (c *queues) EnsurePublisherOk(ctx context.Context, opts *config.QueueOptions) error {

	_, err := c.GetPublisher(opts.Ref())
	if err != nil {
		return c.RegisterPublisher(ctx, opts)
	}
	return nil

}

func (c *queues) Publish(ctx context.Context, reference string, payload any, headers ...map[string]string) error {
	return c.service.Publish(ctx, reference, payload, headers...)
}

func (c *queues) RegisterSubscriber(ctx context.Context, opts *config.QueueOptions, optHandler ...frame.SubscribeWorker) error {
	return c.service.AddSubscriber(ctx, opts.Ref(), string(opts.DS), optHandler...)
}

func (c *queues) GetSubscriber(ref string) (frame.Subscriber, error) {
	return c.service.GetSubscriber(ref)
}

// NewQueueManager ensures a default internal Queue exists
func NewQueueManager(service *frame.Service) QueueManager {
	return &queues{
		service: service,
	}
}

var safeCharacters = regexp.MustCompile("[^A-Za-z0-9$]+")

func Tokenise(str string) string {
	return safeCharacters.ReplaceAllString(str, "_")
}
