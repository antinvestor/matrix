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
	ActorFunctionRoomServerInputEvents           = ActorFunctionID("RoomServerInputEvts")
	ActorFunctionSyncAPIOutputRoomEvents         = ActorFunctionID("SyncAPIOutputRoomEvts")
	ActorFunctionSyncAPIOutputSendToDeviceEvents = ActorFunctionID("SyncAPIOutputSendToDeviceEvts")
	ActorFunctionUserAPIOutputRoomEvents         = ActorFunctionID("UserAPIOutputRoomEvts")
	ActorFunctionFederationAPIOutputRoomEvents   = ActorFunctionID("FederationAPIOutputRoomEvts")
)

const idPrefixSeparator = "___"

type ActorManager interface {
	EnableFunction(functionID ActorFunctionID, qOpts *config.QueueOptions, handlerFunc HandlerFunc)
	Progress(ctx context.Context, functionID ActorFunctionID, id any) (*actorV1.ProgressResponse, error)
}

type HandlerFunc func(ctx context.Context, metadata map[string]string, message []byte) error

type functionOpt struct {
	qOpts       *config.QueueOptions
	handlerFunc HandlerFunc
}

func encodeID(id any) (string, error) {

	switch val := id.(type) {
	case *spec.RoomID:
		return constants.EncodeRoomID(val), nil
	case *spec.UserID:
		return constants.EncodeUserID(val), nil
	default:
		return "", fmt.Errorf("invalid id type, only *spec.RoomID/*spec.UserID is allowed")
	}
}

func encodeIDToClusterID(funcPrefix ActorFunctionID, encodedIDStr string) string {
	return fmt.Sprintf("%s%s%s", funcPrefix, idPrefixSeparator, encodedIDStr)
}

func decodeClusterIDToFunctionID(ID string) (ActorFunctionID, string, error) {
	idParts := strings.Split(ID, idPrefixSeparator)
	if len(idParts) != 2 {
		return "", "", fmt.Errorf("invalid actor id: %s", ID)
	}

	funcPrefix := ActorFunctionID(idParts[0])
	specID := idParts[1]
	return funcPrefix, specID, nil
}

func decodeStrToRoomID(encodedID string) (*spec.RoomID, error) {
	roomID, err := constants.DecodeRoomID(encodedID)
	if err != nil {
		return nil, fmt.Errorf("invalid room id: %s", encodedID)
	}
	return roomID, nil
}

func decodeStrToUserID(encodedID string) (*spec.UserID, error) {
	userID, err := constants.DecodeUserID(encodedID)
	if err != nil {
		return nil, fmt.Errorf("invalid room id: %s", encodedID)
	}
	return userID, nil
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

func idifyQOpts(_ context.Context, opts *config.QueueOptions, encodedID string) *config.QueueOptions {

	ds := opts.DS

	if ds.IsNats() {

		subject := fmt.Sprintf("%s.%s", ds.GetQuery("subject"), encodedID)
		ds = ds.ExtendQuery("consumer_filter_subject", subject)
		durable := strings.ReplaceAll(fmt.Sprintf("CnsDurable_%s", subject), ".", "_")

		ds = ds.ExtendQuery("consumer_durable_name", durable)
		ds = ds.ExtendQuery("consumer_ack_wait", maximumProcessingTime.String())
		ds = ds.ExtendQuery("consumer_inactive_threshold", maximumConsumerInactivityThreshold.String())
		ds = ds.ExtendQuery("consumer_headers_only", "false")
		ds = ds.ExtendQuery("receive_batch_max_batch_size", "1")
		ds = ds.ExtendQuery("consumer_max_ack_pending", "1")

	} else {
		ds = ds.SuffixPath(encodedID)
	}

	return &config.QueueOptions{
		QReference: fmt.Sprintf("%s%s", opts.QReference, encodedID),
		Prefix:     opts.Prefix,
		DS:         ds,
	}
}
