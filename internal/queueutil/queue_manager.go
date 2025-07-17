package queueutil

import (
	"context"
	"regexp"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame"
)

type queues struct {
	service *frame.Service
}

func (q *queues) RegisterPublisher(ctx context.Context, opts *config.QueueOptions) error {
	return q.service.AddPublisher(ctx, opts.Ref(), string(opts.DSrc()))
}

func (q *queues) GetPublisher(ref string) (frame.Publisher, error) {
	return q.service.GetPublisher(ref)
}

func (q *queues) DiscardPublisher(ctx context.Context, ref string) error {
	return q.service.DiscardPublisher(ctx, ref)
}

func (q *queues) GetOrCreatePublisher(ctx context.Context, opts *config.QueueOptions) (frame.Publisher, error) {
	publisher, err := q.GetPublisher(opts.Ref())
	if err == nil {
		return publisher, nil
	}
	err = q.RegisterPublisher(ctx, opts)
	if err != nil {
		return nil, err
	}

	return q.GetPublisher(opts.Ref())
}

func (q *queues) Publish(ctx context.Context, reference string, payload any, headers ...map[string]string) error {

	err := q.service.Publish(ctx, reference, payload, headers...)
	if err != nil {
		return err
	}
	return nil
}

func (q *queues) RegisterSubscriber(ctx context.Context, opts *config.QueueOptions, optHandler ...frame.SubscribeWorker) error {
	return q.service.AddSubscriber(ctx, opts.Ref(), string(opts.DSrc()), optHandler...)
}

func (q *queues) GetSubscriber(ref string) (frame.Subscriber, error) {
	return q.service.GetSubscriber(ref)
}

func (q *queues) GetOrCreateSubscriber(ctx context.Context, opts *config.QueueOptions) (frame.Subscriber, error) {
	subscriber, err := q.GetSubscriber(opts.Ref())
	if err == nil {
		return subscriber, nil
	}
	err = q.RegisterSubscriber(ctx, opts)
	if err != nil {
		return nil, err
	}

	return q.GetSubscriber(opts.Ref())
}

func (q *queues) DiscardSubscriber(ctx context.Context, ref string) error {
	return q.service.DiscardSubscriber(ctx, ref)
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
