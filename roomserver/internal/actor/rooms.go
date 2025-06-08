package actor

import (
	"context"
	"errors"
	"fmt"
	actorV1 "github.com/antinvestor/matrix/apis/actor/v1"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/scheduler"
	"github.com/pitabwire/frame"
	"time"

	"github.com/asynkron/protoactor-go/actor"
)

const messageQPullDelay = 500 * time.Millisecond

// RoomActor is an actor that processes messages for a specific room
type RoomActor struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	roomID     string

	qm queueutil.QueueManager

	handlerFunc  HandlerFunc
	subscription frame.Subscriber

	cluster   *cluster.Cluster
	scheduler *scheduler.TimerScheduler
	cli       *actorV1.RoomEventProcessorGrainClient

	svc *frame.Service

	// Flag to track if job processing is active
	jobProcessing bool
}

// NewRoomActor creates a new room actor
func NewRoomActor(ctx context.Context, cluster *cluster.Cluster, svc *frame.Service, qm queueutil.QueueManager, handlerFunc HandlerFunc) *RoomActor {

	ictx, cancel := context.WithCancel(ctx)

	return &RoomActor{
		ctx:         ictx,
		cancelFunc:  cancel,
		cluster:     cluster,
		qm:          qm,
		handlerFunc: handlerFunc,
		svc:         svc,
	}
}

func (ra *RoomActor) ActorName() string {
	return ra.roomID
}

func (ra *RoomActor) onSetupActor(gctx actor.Context, req *actorV1.SetupRoomRequest) error {

	if ra.roomID == "" {
		ra.roomID = req.GetRoomId()
	}
	if ra.roomID != req.GetRoomId() {
		return fmt.Errorf("room ID mismatch: %s != %s", ra.roomID, req.GetRoomId())
	}

	ra.cli = actorV1.GetRoomEventProcessorGrainClient(ra.cluster, ra.roomID)

	roomOpts := &config.QueueOptions{
		QReference: req.GetQueueRef(),
		Prefix:     req.GetQueuePfx(),
		DS:         config.DataSource(req.GetQueueUri()),
	}

	return ra.setupSubscriber(gctx, roomOpts)
}

func (ra *RoomActor) setupSubscriber(gctx actor.Context, opts *config.QueueOptions) error {

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

func (ra *RoomActor) Init(ctx cluster.GrainContext) {

	ctx.Logger().With("room", ra.roomID).Info("--------------- Room actor running             ")

}
func (ra *RoomActor) Terminate(gctx cluster.GrainContext) {
	// First cancel any ongoing operations
	ra.cancelFunc()

	// Then properly close the subscription with timeout
	if ra.subscription != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		_ = ra.subscription.Stop(closeCtx)
	}
	gctx.Logger().With("room", ra.roomID).Info("--------------- Room actor terminating             ")
}

func (ra *RoomActor) ReceiveDefault(gctx cluster.GrainContext) {

	msg := gctx.Message()

	switch msgT := msg.(type) {
	case *actor.ReceiveTimeout:

		gctx.Logger().With("room", ra.roomID).With("message", msg).Info("--------------- ReceiveTimeout             ")
		gctx.Poison(gctx.Self())

	case *actorV1.WorkRequest:
		// Skip if already processing a job
		if ra.jobProcessing {
			return
		}

		gctx.Logger().With("room", ra.roomID).With("message", msg).Info("--------------- WorkRequest             ")

		// Mark job as processing
		ra.jobProcessing = true

		work := frame.NewJob(func(ctx context.Context, result frame.JobResultPipe[any]) error {
			defer func() {
				// Reset processing flag when job completes
				ra.jobProcessing = false
			}()

			logger := ra.svc.L(ctx)
			logger.Info("| 2. ---------------------------------------------------  |")
			// Process the next message and determine whether there are more to process
			messegeID, err := ra.processNextMessage(ctx)

			// Handle potential errors
			if err != nil {
				gctx.Logger().With("error", err).Error(" 2. err ----------------------------  failed to process message")
				gctx.Poison(gctx.Self())
				return err
			}

			logger.WithField("messegeID", messegeID).Info("| 3. ---------------------------------------------------  |")

			// If there are more messages, continue processing immediately
			if messegeID != "" {
				// Process next message immediately without delay
				gctx.Send(gctx.Self(), &actorV1.WorkRequest{RoomId: msgT.GetRoomId()})
				gctx.SetReceiveTimeout(actorIdlingTime)
			} else {
				ra.scheduleNextMessagePull(gctx, messageQPullDelay)
			}
			logger.Info("| 4. ---------------------------------------------------  |")
			return nil
		})

		gctx.Logger().Info("| 1. ---------------------------------------------------  |")
		err := frame.SubmitJob(ra.ctx, ra.svc, work)
		if err != nil {
			// Reset processing flag on error
			ra.jobProcessing = false
			gctx.Logger().With("error", err).Error("could not submit message for processing")
			return
		}
		gctx.Logger().Debug("---------------------- message submitted for processing                 ")

	default:

		gctx.Logger().With("room", ra.roomID).With("message", msg).Info("--------------- Unknown            ")
	}
}
func (ra *RoomActor) SetupRoom(req *actorV1.SetupRoomRequest, gctx cluster.GrainContext) (*actorV1.SetupRoomResponse, error) {

	err := ra.onSetupActor(gctx, req)
	if err != nil {
		return nil, err
	}

	ra.scheduler = scheduler.NewTimerScheduler(gctx)
	gctx.SetReceiveTimeout(actorIdlingTime)

	gctx.Logger().With("room", ra.roomID).With("message", req).Info("--------------- SetupRoom              ")

	ra.scheduleNextMessagePull(gctx, 0*time.Millisecond)

	return &actorV1.SetupRoomResponse{Message: "ok"}, nil
}

// processNextMessage pulls a message from the queue and processes it
// returns message ID if a message was processed, empty string if no message was available
func (ra *RoomActor) processNextMessage(ctx context.Context) (string, error) {

	// Check if subscription is active
	if ra.subscription == nil {
		return "", fmt.Errorf("subscription not initialized for room %s", ra.roomID)
	}

	// Create a context with timeout to prevent blocking indefinitely
	receiveCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	msg, err := ra.subscription.Receive(receiveCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			// This is not an error, just means no message was available within the timeout
			return "", nil
		}
		return "", err
	}

	// Check if the message is nil
	if msg == nil {
		return "", nil
	}

	// Process the message
	err = ra.handlerFunc(ctx, msg.Metadata, msg.Body)
	if err != nil {
		msg.Nack()
		return msg.LoggableID, err
	}
	msg.Ack()

	return msg.LoggableID, nil
}

func (ra *RoomActor) scheduleNextMessagePull(ctx cluster.GrainContext, delay time.Duration) {

	if delay == 0 {
		ctx.Request(ctx.Self(), &actorV1.WorkRequest{RoomId: ra.roomID})
	}

	ra.scheduler.RequestOnce(delay, ctx.Self(), &actorV1.WorkRequest{RoomId: ra.roomID})
}
