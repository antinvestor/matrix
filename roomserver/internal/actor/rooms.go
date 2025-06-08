package actor

import (
	"context"
	"fmt"
	actorV1 "github.com/antinvestor/matrix/apis/actor/v1"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame"
	"time"

	"github.com/asynkron/protoactor-go/actor"
)

var messageQPullDelay = 500 * time.Millisecond

// RoomActor is an actor that processes messages for a specific room
type RoomActor struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	roomID     string
	qm         queueutil.QueueManager

	handlerFunc  HandlerFunc
	subscription frame.Subscriber
}

// NewRoomActor creates a new room actor
func NewRoomActor(ctx context.Context, qm queueutil.QueueManager, handlerFunc HandlerFunc) *RoomActor {

	ictx, cancel := context.WithCancel(ctx)

	return &RoomActor{
		ctx:         ictx,
		cancelFunc:  cancel,
		qm:          qm,
		handlerFunc: handlerFunc,
	}
}

func (ra *RoomActor) ActorName() string {
	return ra.roomID
}

// Receive handles messages sent to the actor
func (ra *RoomActor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actor.Started:
		ra.onStarted(ctx)
	case *actor.Stopping:
		ra.onStopping(ctx)
	case *actor.Stopped:
		ra.onStopped(ctx)
	case *actorV1.SetupRoomRequest:
		ra.onSetupActor(ctx, msg)
	case *actorV1.QRequestWork:
		// Instead of immediately processing, just trigger production work
		ctx.Send(ctx.Self(), &actorV1.QProduceWork{RoomId: ra.roomID})

	case *actorV1.QProduceWork:
		// Process the next message and determine whether there are more to process
		hasMoreMessages, err := ra.processNextMessage(ra.ctx)

		// Handle potential errors
		if err != nil {
			ctx.Logger().With("error", err).Error("failed to process next message")
			ctx.Stop(ctx.Self())
			return
		}

		// If there are more messages, continue processing immediately
		if hasMoreMessages {
			// Process next message immediately without delay
			ctx.Send(ctx.Self(), &actorV1.QProduceWork{RoomId: ra.roomID})
		} else {
			ctx.Stop(ctx.Self())
		}
	}
}

func (ra *RoomActor) onStarted(ctx actor.Context) {

}

func (ra *RoomActor) onStopping(_ actor.Context) {

	// First cancel any ongoing operations
	ra.cancelFunc()

	// Then properly close the subscription with timeout
	if ra.subscription != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ra.subscription.Stop(closeCtx)
	}

}

func (ra *RoomActor) onStopped(actx actor.Context) {
	// Notify the parent that this actor has stopped
	actx.Send(actx.Parent(), &actorV1.ActorStopped{RoomId: ra.roomID})
}

func (ra *RoomActor) createQueueOpts(qUri string) *config.QueueOptions {
	nueOpts := config.QueueOptions{
		Prefix:     RoomActorQPrefix,
		QReference: ra.ActorName(),
		DS:         config.DataSource(qUri),
	}
	return &nueOpts
}

func (ra *RoomActor) onSetupActor(ctx actor.Context, msg *actorV1.SetupRoomRequest) {
	var err error

	defer func() {
		message := "Setup"
		if err != nil {
			message = err.Error()
		}
		ctx.Respond(&actorV1.SetupRoomResponse{Success: err == nil, Message: message})
	}()
	if ra.roomID == "" {
		ra.roomID = msg.GetRoomId()
	}
	if ra.roomID != msg.GetRoomId() {
		err = fmt.Errorf("room ID mismatch: %s != %s", ra.roomID, msg.GetRoomId())
		return
	}

	err = ra.setupWorker(ctx, msg.GetQueueUri())

	// Start continuous message processing in a goroutine
	//go ra.processNextMessage(ra.ctx)
}

func (ra *RoomActor) setupWorker(ctx actor.Context, qUri string) error {

	opts := ra.createQueueOpts(qUri)

	// Register subscriber for this room
	err := ra.qm.RegisterSubscriber(ra.ctx, opts)
	if err != nil {
		return err
	}

	// Get the subscriber
	ra.subscription, err = ra.qm.GetSubscriber(opts.Ref())
	if err != nil {
		return err
	}

	return nil

}

// processNextMessage pulls a message from the queue and processes it
// returns true if there are more messages available to process immediately
func (ra *RoomActor) processNextMessage(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
		// Check if subscription is active
		if ra.subscription == nil {
			return false, fmt.Errorf("subscription not initialized for room %s", ra.roomID)
		}

		msg, err := ra.subscription.Receive(ctx)
		if err != nil {
			return false, err
		}

		// Check if the message is nil
		if msg == nil {
			return false, nil
		}

		// Process the message
		err = ra.handlerFunc(ctx, msg.Metadata, msg.Body)
		if err != nil {
			msg.Nack()
			return true, err
		}

		msg.Ack()

		return true, nil
	}
}

type RoomBehavior struct {
	isActive bool
}

func (m *RoomBehavior) MailboxStarted() {
}

func (m *RoomBehavior) MessagePosted(msg any) {
}

func (m *RoomBehavior) MessageReceived(msg any) {

}

func (m *RoomBehavior) MailboxEmpty() {}
