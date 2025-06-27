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

// SequentialActor is an actor that processes messages for a specific room
type SequentialActor struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	roomID *spec.RoomID
	userID *spec.UserID

	processors      map[ActorFunctionID]*functionOpt
	actorFunctionID ActorFunctionID

	qm           queueutil.QueueManager
	subscription frame.Subscriber

	cluster *cluster.Cluster
	cli     *actorV1.SequentialProcessorGrainClient

	// Flag to track if job processing is active
	jobProcessing atomic.Bool

	log *util.LogEntry
}

func (ra *SequentialActor) Progress(_ *actorV1.ProgressRequest, _ cluster.GrainContext) (*actorV1.ProgressResponse, error) {

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

// NewSeqActor creates a new room actor
func NewSeqActor(ctx context.Context, cluster *cluster.Cluster, qm queueutil.QueueManager, processorMap map[ActorFunctionID]*functionOpt) *SequentialActor {

	ictx, cancel := context.WithCancel(ctx)

	return &SequentialActor{
		ctx:        ictx,
		cancelFunc: cancel,

		cluster:    cluster,
		processors: processorMap,

		qm: qm,

		log: util.Log(ictx),
	}
}

func (ra *SequentialActor) setupSubscriber(ctx context.Context, qm queueutil.QueueManager, qOpts *config.QueueOptions, encodedIDStr string) error {

	var err error
	opts := idifyQOpts(ctx, qOpts, encodedIDStr)

	// Get the subscriber
	ra.subscription, err = qm.GetOrCreateSubscriber(ctx, opts)
	if err != nil {
		return err
	}

	return nil
}

func (ra *SequentialActor) Init(gctx cluster.GrainContext) {

	log := gctx.Logger()

	actorIdentity := gctx.Identity()

	functionID, internalIDStr, err := decodeClusterIDToFunctionID(actorIdentity)
	if err != nil {
		ra.log.With("internalID", internalIDStr).With("error", err).Error(" Invalid room/user ID  ")
		gctx.Request(gctx.Self(), &actorV1.StopProcessor{Id: actorIdentity})
		return
	}

	ra.actorFunctionID = functionID

	processor, ok := ra.processors[functionID]
	if !ok {
		ra.log.With("internalID", internalIDStr).With("error", err).Error(" Invalid setup, no processor exists ")
		gctx.Request(gctx.Self(), &actorV1.StopProcessor{Id: actorIdentity})
		return
	}

	queueIDStr := ""

	if functionID == ActorFunctionSyncAPIOutputSendToDeviceEvents {
		userID, err0 := decodeStrToUserID(internalIDStr)
		if err0 != nil {
			ra.log.With("internalID", internalIDStr).With("error", err).Error(" Invalid user id, processor couldn't startup ")
			gctx.Request(gctx.Self(), &actorV1.StopProcessor{Id: actorIdentity})
			return
		}
		ra.userID = userID
		ra.log = ra.log.With("userID", userID.String())
		queueIDStr = userID.String()

	} else {
		roomID, err0 := decodeStrToRoomID(internalIDStr)
		if err0 != nil {
			ra.log.With("internalID", internalIDStr).With("error", err).Error(" Invalid setup, no processor exists ")
			gctx.Request(gctx.Self(), &actorV1.StopProcessor{Id: actorIdentity})
			return
		}
		ra.roomID = roomID
		ra.log = ra.log.With("roomID", roomID.String())
		queueIDStr = roomID.String()

	}

	ra.cli = actorV1.GetSequentialProcessorGrainClient(ra.cluster, actorIdentity)

	err = ra.setupSubscriber(ra.ctx, ra.qm, processor.qOpts, internalIDStr)
	if err != nil {
		log.With("internalID", internalIDStr).With("error", err).Error(" Failed to setup subscriber     ")
		return
	}

	gctx.Request(gctx.Self(), &actorV1.WorkRequest{QId: queueIDStr})
}

func (ra *SequentialActor) Terminate(gctx cluster.GrainContext) {
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

func (ra *SequentialActor) ReceiveDefault(gctx cluster.GrainContext) {

	msg := gctx.Message()

	switch msgT := msg.(type) {
	case *actorV1.StopProcessor:

		gctx.Poison(gctx.Self())

	case *actorV1.WorkRequest:
		// Skip if already processing a job
		if !ra.jobProcessing.CompareAndSwap(false, true) {
			return
		}

		// Process the next message and determine whether there are more to process
		eventWork := frame.NewJob(func(ctx context.Context, _ frame.JobResultPipe) error {
			defer func() {
				// Reset processing flag when job completes
				ra.jobProcessing.Swap(false)
				gctx.Request(gctx.Self(), &actorV1.WorkRequest{QId: msgT.GetQId()})
			}()

			err := ra.pullNewMessage(ctx, msgT)

			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// Reset processing flag on error

				if ra.subscription.IsIdle() {
					gctx.Request(gctx.Self(), &actorV1.StopProcessor{Id: gctx.Identity()})
					return nil
				}
			}

			return nil
		})

		ra.qm.Submit(ra.ctx, eventWork)
	}
}

// pullNewMessage pulls a message from the queue and processes it
// returns message ID if a message was processed, empty string if no message was available
func (ra *SequentialActor) pullNewMessage(ctx context.Context, req *actorV1.WorkRequest) error {

	log := ra.log.WithContext(ctx)

	// Check if subscription is active
	if ra.subscription == nil {
		log.Error(" no subscription initialised")
		return fmt.Errorf("subscription not initialised for room/user %s", req.GetQId())
	}

	requestCtx, cancelFn := context.WithTimeout(ctx, maximumIdlingTime)
	msg, err := ra.subscription.Receive(requestCtx)
	cancelFn()
	if err != nil {
		return err
	}

	// Process the message
	processor := ra.processors[ra.actorFunctionID]

	authClaim := frame.ClaimsFromMap(msg.Metadata)
	var authCtx context.Context
	if authClaim != nil {
		authCtx = authClaim.ClaimsToContext(ctx)
	} else {
		authCtx = ctx
	}

	err = processor.handlerFunc(authCtx, msg.Metadata, msg.Body)
	if err != nil {
		log.WithError(err).Error(" failed to process event")
		msg.Nack()

		return nil
	}
	msg.Ack()
	return nil
}
