package actorutil

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	actorV1 "github.com/antinvestor/matrix/apis/actor/v1"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/constants"
)

type ActorFunctionID string

const (
	ActorFunctionRoomServer          = ActorFunctionID("RoomServer")
	ActorFunctionSyncAPIServer       = ActorFunctionID("SyncAPIServer")
	ActorFunctionUserAPIServer       = ActorFunctionID("UserAPIServer")
	ActorFunctionFederationAPIServer = ActorFunctionID("FederationAPIServer")
)

const idPrefixSeparator = "___"

type ActorManager interface {
	EnableFunction(functionID ActorFunctionID, qOpts *config.QueueOptions, handlerFunc HandlerFunc)
	Progress(ctx context.Context, functionID ActorFunctionID, roomID *spec.RoomID) (*actorV1.ProgressResponse, error)
}

type HandlerFunc func(ctx context.Context, metadata map[string]string, message []byte) error

type functionOpt struct {
	qOpts       *config.QueueOptions
	handlerFunc HandlerFunc
}

func prefixRoomIDWithFunc(funcPrefix ActorFunctionID, roomID *spec.RoomID) string {
	return fmt.Sprintf("%s%s%s", funcPrefix, idPrefixSeparator, constants.EncodeRoomID(roomID))
}

func prefixedIDToRoomID(ID string) (ActorFunctionID, *spec.RoomID, error) {

	idParts := strings.Split(ID, idPrefixSeparator)
	if len(idParts) != 2 {
		return "", nil, fmt.Errorf("invalid actor id: %s", ID)
	}

	funcPrefix := ActorFunctionID(idParts[0])
	roomIDStr := idParts[1]

	roomID, err := constants.DecodeRoomID(roomIDStr)
	if err != nil {
		return "", nil, fmt.Errorf("invalid room id: %s", ID)
	}
	return funcPrefix, roomID, nil
}

// If a room consumer is inactive for a while then we will allow NATS
// to clean it up. This stops us from holding onto durable consumers
// indefinitely for rooms that might no longer be active, since they do
// have an interest overhead in the NATS Server. If the room becomes
// active again then we'll recreate the consumer anyway.
const maximumConsumerInactivityThreshold = time.Hour * 24

// An event being processed by a room actor is only allowed 5 seconds maximum.
// If the event takes longer than this then we will assume it has issues and the message will be redelivered.
const maximumProcessingTime = time.Second * 15

// If a room actor is not receiving messages for sometime, we allow it to
// be stopped. This stops us from holding onto room actors indefinitely
// for rooms that might no longer be active, since they do have an
// interest overhead in the NATS Server. If the room becomes active
// again then we'll recreate the actor anyway.
const maximumIdlingTime = time.Minute * 1

func roomifyQOpts(_ context.Context, opts *config.QueueOptions, roomId *spec.RoomID) *config.QueueOptions {

	ds := opts.DS

	encodedRoomID := constants.EncodeRoomID(roomId)

	if ds.IsNats() {

		subject := fmt.Sprintf("%s.%s", ds.GetQuery("subject"), encodedRoomID)
		ds = ds.ExtendQuery("consumer_filter_subject", subject)
		durable := strings.ReplaceAll(fmt.Sprintf("CnsDurable_%s", subject), ".", "_")

		ds = ds.ExtendQuery("consumer_durable_name", durable)
		ds = ds.ExtendQuery("consumer_ack_wait", maximumProcessingTime.String())
		ds = ds.ExtendQuery("consumer_inactive_threshold", maximumConsumerInactivityThreshold.String())
		ds = ds.ExtendQuery("consumer_headers_only", "false")
		ds = ds.ExtendQuery("receive_batch_max_batch_size", "1")
		ds = ds.ExtendQuery("consumer_max_ack_pending", "1")

	} else {
		ds = ds.SuffixPath(encodedRoomID)
	}

	return &config.QueueOptions{
		QReference: fmt.Sprintf("%s%s", opts.QReference, encodedRoomID),
		Prefix:     opts.Prefix,
		DS:         ds,
	}
}
