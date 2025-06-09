package actor

import (
	"context"
	"fmt"
	actorV1 "github.com/antinvestor/matrix/apis/actor/v1"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/pitabwire/frame"
	"sync/atomic"
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
	publisher    frame.Publisher

	cluster *cluster.Cluster
	cli     *actorV1.RoomEventProcessorGrainClient

	svc *frame.Service

	// Flag to track if job processing is active
	jobProcessing atomic.Bool

	// Flag to track if setup is in progress
	setupInProgress atomic.Bool
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

func (ra *RoomActor) onSetupActor(gctx actor.Context, req *actorV1.SetupRequest) error {

	if ra.roomID == "" {
		ra.roomID = req.GetRoomId()
	}
	if ra.roomID != req.GetRoomId() {
		return fmt.Errorf("room ID mismatch: %s != %s", ra.roomID, req.GetRoomId())
	}

	ra.cli = actorV1.GetRoomEventProcessorGrainClient(ra.cluster, ra.roomID)

	err := ra.setupSubscriber(gctx, req)
	if err != nil {
		return err
	}

	return ra.setupPublisher(gctx, req)
}

func (ra *RoomActor) setupSubscriber(_ actor.Context, req *actorV1.SetupRequest) error {

	opts := &config.QueueOptions{
		QReference: req.GetQRef(),
		Prefix:     req.GetQPrefix(),
		DS:         config.DataSource(req.GetQSubsUri()),
	}

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

func (ra *RoomActor) setupPublisher(_ actor.Context, req *actorV1.SetupRequest) error {

	opts := &config.QueueOptions{
		QReference: req.GetQRef(),
		Prefix:     req.GetQPrefix(),
		DS:         config.DataSource(req.GetQPubUri()),
	}
	// Register subscriber for this room
	err := ra.qm.RegisterPublisher(ra.ctx, opts)
	if err != nil {
		return err
	}

	// Get the subscriber
	ra.publisher, err = ra.qm.GetPublisher(opts.Ref())
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
}

func (ra *RoomActor) ReceiveDefault(gctx cluster.GrainContext) {

	msg := gctx.Message()

	switch msgT := msg.(type) {
	case *actor.ReceiveTimeout:

		gctx.Poison(gctx.Self())

	case *actorV1.WorkRequest:
		// Skip if already processing a job
		if !ra.jobProcessing.CompareAndSwap(false, true) {
			return
		}

		// Process the next message and determine whether there are more to process
		err := ra.nextEvent(gctx, msgT)
		if err != nil {
			// Reset processing flag on error
			ra.jobProcessing.Swap(false)
			gctx.Logger().With("error", err).Error("----------------------- could not submit message for processing")
			return
		}

	default:

		gctx.Logger().With("room", ra.roomID).With("message", msg).Info("--------------- Unknown            ")
	}
}
func (ra *RoomActor) Setup(req *actorV1.SetupRequest, gctx cluster.GrainContext) (*actorV1.SetupResponse, error) {

	// Ensure only one setup can run at a time
	if !ra.setupInProgress.CompareAndSwap(false, true) {

		return &actorV1.SetupResponse{Success: false, Message: "setup already in progress"}, nil
	}

	// Use defer to ensure the flag is reset even if an error occurs
	defer ra.setupInProgress.Store(false)

	if ra.publisher != nil {
		return &actorV1.SetupResponse{Success: true, Message: "ok"}, nil
	}

	err := ra.onSetupActor(gctx, req)
	if err != nil {
		return &actorV1.SetupResponse{Success: false, Message: err.Error()}, nil
	}

	gctx.SetReceiveTimeout(actorIdlingTime)

	gctx.Logger().With("room", ra.roomID).With("message", req).Debug("--------------- SetupRoom              ")

	gctx.Request(gctx.Self(), &actorV1.WorkRequest{RoomId: ra.roomID})

	return &actorV1.SetupResponse{Success: true, Message: "ok"}, nil
}

func (ra *RoomActor) Publish(req *actorV1.PublishRequest, gctx cluster.GrainContext) (*actorV1.PublishResponse, error) {

	// Check if publisher is active
	if ra.publisher == nil {
		return nil, fmt.Errorf("publisher not initialized for room %s", req.GetRoomId())
	}

	err := ra.publisher.Publish(ra.ctx, req.GetPayload(), req.GetMetadata())
	if err != nil {
		return nil, err
	}

	return &actorV1.PublishResponse{Success: true, Message: "ok"}, nil
}

// nextEvent pulls a message from the queue and processes it
// returns message ID if a message was processed, empty string if no message was available
func (ra *RoomActor) nextEvent(gctx cluster.GrainContext, req *actorV1.WorkRequest) error {

	work := frame.NewJob(func(ctx context.Context, result frame.JobResultPipe[any]) error {
		defer func() {
			// Reset processing flag when job completes
			ra.jobProcessing.Swap(false)
		}()

		l := ra.svc.L(ctx)

		// Check if subscription is active
		if ra.subscription == nil {
			l.Error(" err ----------------------------  no subscription initialized for room")
			return fmt.Errorf("subscription not initialized for room %s", req.GetRoomId())
		}

		requestCtx, cancelFn := context.WithTimeout(ctx, messageQPullDelay)
		msg, err := ra.subscription.Receive(requestCtx)
		cancelFn()
		if err != nil {
			gctx.Request(gctx.Self(), &actorV1.WorkRequest{RoomId: ra.roomID})
			return err
		}

		// Process the message
		err = ra.handlerFunc(ctx, msg.Metadata, msg.Body)
		if err != nil {
			l.WithError(err).Error(" 2. err ----------------------------  failed to process event")
			msg.Nack()

			return err
		}
		msg.Ack()

		// Process next message immediately without delay
		gctx.Request(gctx.Self(), &actorV1.WorkRequest{RoomId: ra.roomID})
		gctx.SetReceiveTimeout(actorIdlingTime)

		return nil
	})

	return frame.SubmitJob(ra.ctx, ra.svc, work)
}
