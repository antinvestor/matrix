package actorutil

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/antinvestor/gomatrixserverlib/spec"
	actorV1 "github.com/antinvestor/matrix/apis/actor/v1"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/util"
)

// RoomActor is an actor that processes messages for a specific room
type RoomActor struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	roomID *spec.RoomID

	qm queueutil.QueueManager

	processors map[ActorFunctionID]*functionOpt

	actorFunctionID ActorFunctionID

	subscription frame.Subscriber

	cluster *cluster.Cluster
	cli     *actorV1.RoomEventProcessorGrainClient

	svc *frame.Service

	// Flag to track if job processing is active
	jobProcessing atomic.Bool

	// Flag to track if setup is in progress
	setupInProgress atomic.Bool

	log *util.LogEntry
}

func (ra *RoomActor) Progress(_ *actorV1.ProgressRequest, _ cluster.GrainContext) (*actorV1.ProgressResponse, error) {

	if ra.subscription == nil {
		return nil, errors.New("no subscription available")
	}

	var subscriptionState string
	switch ra.subscription.State() {
	case frame.SubscriberStateWaiting:
		subscriptionState = "waiting"
	case frame.SubscriberStateProcessing:
		subscriptionState = "processing"
	case frame.SubscriberStateInError:
		subscriptionState = "error"
	default:
		subscriptionState = "unknown"
	}

	metrics := ra.subscription.Metrics()

	return &actorV1.ProgressResponse{
		Message:        "ok",
		State:          subscriptionState,
		ActiveMessages: metrics.ActiveMessages.Load(),
		LastActivity:   metrics.LastActivity.Load(),
		ProcessingTime: metrics.ProcessingTime.Load(),
		MessageCount:   metrics.MessageCount.Load(),
		ErrorCount:     metrics.ErrorCount.Load(),
	}, nil
}

// NewRoomActor creates a new room actor
func NewRoomActor(ctx context.Context, cluster *cluster.Cluster, svc *frame.Service, qm queueutil.QueueManager, processorMap map[ActorFunctionID]*functionOpt) *RoomActor {

	ictx, cancel := context.WithCancel(ctx)

	return &RoomActor{
		ctx:        ictx,
		cancelFunc: cancel,

		cluster:    cluster,
		processors: processorMap,

		qm: qm,

		svc: svc,
		log: util.Log(ictx),
	}
}

func (ra *RoomActor) setupSubscriber(qOpts *config.QueueOptions, roomID *spec.RoomID) error {

	ctx := ra.ctx
	qm := ra.qm

	opts, err := roomifyQOpts(ctx, qOpts, roomID)
	if err != nil {
		return err
	}

	// Register subscriber for this room
	err = qm.RegisterSubscriber(ctx, opts)
	if err != nil {

		return err
	}

	// Get the subscriber
	ra.subscription, err = qm.GetSubscriber(opts.Ref())
	if err != nil {
		return err
	}

	return nil

}

func (ra *RoomActor) Init(gctx cluster.GrainContext) {

	log := gctx.Logger()

	actorIdentity := gctx.Identity()

	functionID, roomID, err := prefixedIDToRoomID(actorIdentity)
	if err != nil {
		ra.log.With("roomID", roomID).With("error", err).Error(" Invalid room ID  ")
		gctx.Request(gctx.Self(), &actorV1.StopRoomActor{RoomId: actorIdentity})
		return
	}

	ra.actorFunctionID = functionID

	processor, ok := ra.processors[functionID]
	if !ok {
		ra.log.With("roomID", roomID).With("error", err).Error(" Invalid setup, no processor exists ")
		gctx.Request(gctx.Self(), &actorV1.StopRoomActor{RoomId: actorIdentity})
		return
	}

	ra.roomID = roomID
	ra.cli = actorV1.GetRoomEventProcessorGrainClient(ra.cluster, roomID.String())
	ra.log = ra.log.With("roomID", roomID.String())

	err = ra.setupSubscriber(processor.qOpts, roomID)
	if err != nil {
		log.With("roomID", roomID).With("error", err).Error(" Failed to setup subscriber     ")
		return
	}

	gctx.Request(gctx.Self(), &actorV1.WorkRequest{RoomId: roomID.String()})
}
func (ra *RoomActor) Terminate(gctx cluster.GrainContext) {
	var err error
	// First cancel any ongoing operations
	ra.cancelFunc()

	// Then properly close the subscription with timeout
	if ra.subscription != nil {
		err = ra.qm.DiscardSubscriber(ra.ctx, ra.subscription.Ref())
		if err != nil {
			gctx.Logger().With("error", err).Error("failed to discard publisher")
		}
	}
}

func (ra *RoomActor) ReceiveDefault(gctx cluster.GrainContext) {

	log := gctx.Logger()

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
		eventWork := ra.nextEventJob(gctx, msgT)

		err := frame.SubmitJob(ra.ctx, ra.svc, eventWork)
		if err != nil {
			ra.jobProcessing.Swap(false)

			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// Reset processing flag on error
				return
			}

			log.With("error", err).Error("could not submit message for processing")
		}
	}
}

// nextEventJob pulls a message from the queue and processes it
// returns message ID if a message was processed, empty string if no message was available
func (ra *RoomActor) nextEventJob(gctx cluster.GrainContext, req *actorV1.WorkRequest) frame.Job {

	return frame.NewJob(func(ctx context.Context, _ frame.JobResultPipe) error {
		defer func() {
			// Reset processing flag when job completes
			ra.jobProcessing.Swap(false)
		}()

		log := ra.log.WithContext(ctx)

		// Check if subscription is active
		if ra.subscription == nil {
			log.Error(" no subscription initialised for room")
			return fmt.Errorf("subscription not initialised for room %s", req.GetRoomId())
		}

		requestCtx, cancelFn := context.WithTimeout(ctx, maximumIdlingTime)
		msg, err := ra.subscription.Receive(requestCtx)
		cancelFn()
		if err != nil {

			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// Reset processing flag on error

				if ra.subscription.IsIdle() {
					gctx.Request(gctx.Self(), &actorV1.StopRoomActor{RoomId: ra.roomID.String()})
					return nil
				}
			}

			gctx.Request(gctx.Self(), &actorV1.WorkRequest{RoomId: ra.roomID.String()})
			return nil
		}

		// Process the message
		processor := ra.processors[ra.actorFunctionID]
		err = processor.handlerFunc(ctx, msg.Metadata, msg.Body)
		if err != nil {
			log.WithError(err).Error(" failed to process event")
			msg.Nack()

			return nil
		}
		msg.Ack()

		// Process next message immediately without delay
		gctx.Request(gctx.Self(), &actorV1.WorkRequest{RoomId: ra.roomID.String()})

		return nil
	})
}
