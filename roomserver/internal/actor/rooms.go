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

// RoomActor is an actor that processes messages for a specific room
type RoomActor struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	roomID     string
	qm         queueutil.QueueManager

	handlerFunc  HandlerFunc
	subscription frame.Subscriber

	lastActivity time.Time
	idleTimeout  time.Duration
}

// NewRoomActor creates a new room actor
func NewRoomActor(ctx context.Context, qm queueutil.QueueManager, handlerFunc HandlerFunc, idleTimeout time.Duration) *RoomActor {

	ictx, cancel := context.WithCancel(ctx)

	return &RoomActor{
		ctx:         ictx,
		cancelFunc:  cancel,
		qm:          qm,
		handlerFunc: handlerFunc,
		idleTimeout: idleTimeout,
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
		ctx.Send(ctx.Self(), &actorV1.QProduceWork{RoomId: ra.roomID})

	case *actorV1.QProduceWork:
		err := ra.processNextMessage(ra.ctx)
		if err != nil {
			ctx.Logger().With(err).Error("failed to process next message")
			return
		}
		if time.Since(ra.lastActivity) >= ra.idleTimeout {
			ctx.Stop(ctx.Self())
		} else {
			ctx.Send(ctx.Self(), &actorV1.QRequestWork{RoomId: ra.roomID})
		}
	}
}

func (ra *RoomActor) onStarted(ctx actor.Context) {
	ra.lastActivity = time.Now()
}

func (ra *RoomActor) onStopping(_ actor.Context) {

	ra.cancelFunc()

	// Close the subscription if it exists
	if ra.subscription != nil {
		_ = ra.subscription.Stop(ra.ctx)
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

// processNextMessage continuously pulls messages from the queue and processes them
func (ra *RoomActor) processNextMessage(ctx context.Context) error {

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Attempt to receive a message from the subscription
		msg, err := ra.subscription.Receive(ctx)
		if err != nil {
			return err
		}

		// Process the message
		err = ra.handlerFunc(ctx, msg.Metadata, msg.Body)
		if err != nil {
			// Log error or take appropriate action - we're not logging as requested
			msg.Nack()
			return err
		}

		msg.Ack()
		return nil

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
