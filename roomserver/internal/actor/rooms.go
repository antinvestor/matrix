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
	ctx               context.Context
	cancelFunc        context.CancelFunc
	roomID            string
	qm                queueutil.QueueManager
	subscription      frame.Subscriber
	worker            frame.SubscribeWorker
	lastActivity      time.Time
	idleTimeout       time.Duration
	idleCheckInterval time.Duration
	idleCheckTimer    *time.Timer
}

// NewRoomActor creates a new room actor
func NewRoomActor(ctx context.Context, qm queueutil.QueueManager, worker frame.SubscribeWorker, idleTimeout time.Duration) *RoomActor {

	ictx, cancel := context.WithCancel(ctx)

	return &RoomActor{
		ctx:               ictx,
		cancelFunc:        cancel,
		qm:                qm,
		worker:            worker,
		idleTimeout:       idleTimeout,
		idleCheckInterval: idleTimeout / 4, // Check 4 times during the idle timeout period
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
	case *actorV1.CheckIdleTimeout:
		ra.onCheckIdleTimeout(ctx)
	default:
		// Handle cluster PID assignment
	}
}

func (ra *RoomActor) onStarted(ctx actor.Context) {
	ra.lastActivity = time.Now()
	// Start the idle timer
	ra.startIdleTimer(ctx)
}

func (ra *RoomActor) onStopping(_ actor.Context) {

	ra.cancelFunc()

	// Stop the idle check timer
	if ra.idleCheckTimer != nil {
		ra.idleCheckTimer.Stop()
	}

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

	// Skip if already subscribed
	if ra.subscription != nil {
		return
	}

	opts := ra.createQueueOpts(msg.GetQueueUri())

	// Register subscriber for this room
	err = ra.qm.RegisterSubscriber(ra.ctx, opts)
	if err != nil {
		return
	}

	// Get the subscriber
	ra.subscription, err = ra.qm.GetSubscriber(opts.Ref())
	if err != nil {
		return
	}

	// Start continuous message processing in a goroutine
	go ra.internallyHandleRoomEvents(ra.ctx)
}

// internallyHandleRoomEvents continuously pulls messages from the queue and processes them
func (ra *RoomActor) internallyHandleRoomEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Actor is stopping, exit the goroutine
			return
		default:
			// Attempt to receive a message from the subscription
			msg, err := ra.subscription.Receive(ctx)
			if err != nil {
				// If there's an error receiving, wait a bit before trying again
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Process the message
			err = ra.worker.Handle(ctx, msg.Metadata, msg.Body)
			if err != nil {
				// Log error or take appropriate action - we're not logging as requested
				msg.Nack()
				return
			}

			msg.Ack()
			// Update last activity timestamp
			ra.lastActivity = time.Now()

		}
	}
}

func (ra *RoomActor) onCheckIdleTimeout(ctx actor.Context) {
	// If we're actively processing a message, don't check for idle timeout
	if ra.subscription != nil && !ra.subscription.Idle() {
		ra.startIdleTimer(ctx)
		return
	}

	// If we've been idle for too long, stop the actor
	if time.Since(ra.lastActivity) >= ra.idleTimeout {
		ctx.Stop(ctx.Self())
		return
	}

	// Not idle yet, restart the timer
	ra.startIdleTimer(ctx)
}

func (ra *RoomActor) startIdleTimer(ctx actor.Context) {
	if ra.idleCheckTimer != nil {
		ra.idleCheckTimer.Stop()
	}
	ra.idleCheckTimer = time.AfterFunc(ra.idleCheckInterval, func() {
		ctx.Send(ctx.Self(), &actorV1.CheckIdleTimeout{
			RoomId: ra.roomID,
		})
	})
}
