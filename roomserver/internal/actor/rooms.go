package actor

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/antinvestor/gomatrixserverlib/spec"
	actorV1 "github.com/antinvestor/matrix/apis/actor/v1"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/util"
)

// RoomActor is an actor that processes messages for a specific room
type RoomActor struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	roomID     string

	qOpts *config.QueueOptions
	qm    queueutil.QueueManager

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

	log *util.LogEntry
}

// NewRoomActor creates a new room actor
func NewRoomActor(ctx context.Context, cluster *cluster.Cluster, svc *frame.Service, opts *config.QueueOptions, qm queueutil.QueueManager, handlerFunc HandlerFunc) *RoomActor {

	ictx, cancel := context.WithCancel(ctx)

	return &RoomActor{
		ctx:        ictx,
		cancelFunc: cancel,
		cluster:    cluster,

		qOpts: opts,
		qm:    qm,

		handlerFunc: handlerFunc,
		svc:         svc,
		log:         util.Log(ictx),
	}
}

func (ra *RoomActor) setupSubscriber(actx actor.Context, roomID *spec.RoomID) error {

	opts, err := RoomifyQOpts(ra.ctx, ra.qOpts, roomID, true)
	if err != nil {
		return err
	}

	// Register subscriber for this room
	err = ra.qm.RegisterSubscriber(ra.ctx, opts)
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

func (ra *RoomActor) setupPublisher(actx actor.Context, roomID *spec.RoomID) error {

	opts, err := RoomifyQOpts(ra.ctx, ra.qOpts, roomID, false)
	if err != nil {
		return err
	}
	// Register subscriber for this room
	err = ra.qm.RegisterPublisher(ra.ctx, opts)
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

func (ra *RoomActor) Init(gctx cluster.GrainContext) {

	roomID, err := spec.NewRoomID(gctx.Identity())
	if err != nil {
		ra.log.With("roomID", roomID).With("error", err).Error("--------------- Invalid room ID     ")
		return
	}

	ra.roomID = roomID.String()
	ra.cli = actorV1.GetRoomEventProcessorGrainClient(ra.cluster, roomID.String())
	ra.log = ra.log.With("roomID", roomID.String())
	log := ra.log

	err = ra.setupPublisher(gctx, roomID)
	if err != nil {
		log.With("roomID", roomID).With("error", err).Error(" Failed to setup publisher     ")
		return
	}

	err = ra.setupSubscriber(gctx, roomID)
	if err != nil {
		log.With("roomID", roomID).With("error", err).Error(" Failed to setup subscriber     ")
		return
	}

	gctx.Request(gctx.Self(), &actorV1.WorkRequest{RoomId: ra.roomID})

	log.With("roomID", roomID).Info("--------------- Successfully Setup Room    ")

}
func (ra *RoomActor) Terminate(gctx cluster.GrainContext) {
	var err error
	// First cancel any ongoing operations
	ra.cancelFunc()

	if ra.publisher != nil {
		err = ra.qm.DiscardPublisher(ra.ctx, ra.publisher.Ref())
		if err != nil {
			gctx.Logger().With("error", err).Error("failed to discard publisher")
		}
	}

	// Then properly close the subscription with timeout
	if ra.subscription != nil {
		err = ra.qm.DiscardSubscriber(ra.ctx, ra.subscription.Ref())
		if err != nil {
			gctx.Logger().With("error", err).Error("failed to discard publisher")
		}
	}
}

func (ra *RoomActor) ReceiveDefault(gctx cluster.GrainContext) {

	msg := gctx.Message()

	switch msgT := msg.(type) {
	case *actorV1.StopRoomActor:

		gctx.Poison(gctx.Self())

	case *actorV1.WorkRequest:
		// Skip if already processing a job
		if !ra.jobProcessing.CompareAndSwap(false, true) {
			return
		}

		// Process the next message and determine whether there are more to process
		ra.nextEvent(gctx, msgT)

	default:

		ra.log.With("message", msg).Debug("--------------- Unknown            ")
	}
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
func (ra *RoomActor) nextEvent(gctx cluster.GrainContext, req *actorV1.WorkRequest) {

	log := ra.log

	work := frame.NewJob(func(ctx context.Context, _ frame.JobResultPipe) error {
		defer func() {
			// Reset processing flag when job completes
			ra.jobProcessing.Swap(false)
		}()

		log = ra.log.WithContext(ctx)

		// Check if subscription is active
		if ra.subscription == nil {
			log.Error(" no subscription initialised for room")
			return fmt.Errorf("subscription not initialized for room %s", req.GetRoomId())
		}

		requestCtx, cancelFn := context.WithTimeout(ctx, maximumIdlingTime)
		msg, err := ra.subscription.Receive(requestCtx)
		cancelFn()
		if err != nil {

			if ra.subscription.Idle() {
				gctx.Request(gctx.Self(), &actorV1.StopRoomActor{RoomId: ra.roomID})
				return err
			}

			gctx.Request(gctx.Self(), &actorV1.WorkRequest{RoomId: ra.roomID})
			return err
		}

		// Process the message
		err = ra.handlerFunc(ctx, msg.Metadata, msg.Body)
		if err != nil {
			log.WithError(err).Error(" failed to process event")
			msg.Nack()

			return err
		}
		msg.Ack()

		// Process next message immediately without delay
		gctx.Request(gctx.Self(), &actorV1.WorkRequest{RoomId: ra.roomID})

		return nil
	})

	err := frame.SubmitJob(ra.ctx, ra.svc, work)
	if err != nil {

		log.With("error", err).Error("could not submit message for processing")
		// Reset processing flag on error
		ra.jobProcessing.Swap(false)

	}
}
