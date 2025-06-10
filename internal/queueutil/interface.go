package queueutil

import (
	"context"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame"
)

const (
	UserID        = "user_id"
	RoomID        = "room_id"
	EventID       = "event_id"
	RoomEventType = "output_room_event_type"

	AppServiceIDToken = "appservice_id_token"
)

type QueueManager interface {
	RegisterPublisher(ctx context.Context, opts *config.QueueOptions) error
	GetPublisher(ref string) (frame.Publisher, error)

	DiscardPublisher(ctx context.Context, ref string) error

	EnsurePublisherOk(ctx context.Context, opts *config.QueueOptions) error
	Publish(ctx context.Context, reference string, payload any, headers ...map[string]string) error
	GetSubscriber(ref string) (frame.Subscriber, error)

	DiscardSubscriber(ctx context.Context, ref string) error
	RegisterSubscriber(ctx context.Context, opts *config.QueueOptions, handler ...frame.SubscribeWorker) error
}
