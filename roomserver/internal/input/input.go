// Copyright 2017 Vector Creations Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package input contains the code processes new room events
package input

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	fedapi "github.com/antinvestor/matrix/federationapi/api"
	"github.com/antinvestor/matrix/internal/actorutil"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/roomserver/acls"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/internal/query"
	"github.com/antinvestor/matrix/roomserver/producers"
	"github.com/antinvestor/matrix/roomserver/storage"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/constants"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/util"
)

// Inputer is responsible for consuming from the roomserver input
// streams and processing the events. All input events are queued
// into a single NATS stream and the order is preserved strictly.
// The `room_id` message header will contain the room ID which will
// be used to assign the pending event to a per-room worker.
//
// The input API maintains an ephemeral headers-only consumer. It
// will speed through the stream working out which room IDs are
// pending and create durable consumers for them. The durable
// consumer will then be used for each room worker goroutine to
// fetch events one by one and process them. Each room having a
// durable consumer of its own means there is no head-of-line
// blocking between rooms. Filtering ensures that each durable
// consumer only receives events for the room it is interested in.
//
// The ephemeral consumer closely tracks the newest events. The
// per-room durable consumers will only progress through the stream
// as events are processed.
//
//	      A BC *  -> positions of each consumer (* = ephemeral)
//	      ⌄ ⌄⌄ ⌄
//	ABAABCAABCAA  -> newest (letter = subject for each message)
//
// In this example, A is still processing an event but has two
// pending events to process afterwards. Both B and C are caught
// up, so they will do nothing until a new event comes in for B
// or C.
type Inputer struct {
	Cfg                     *config.RoomServer
	DB                      storage.RoomDatabase
	Qm                      queueutil.QueueManager
	Am                      actorutil.ActorManager
	ServerName              spec.ServerName
	SigningIdentity         func(ctx context.Context, roomID spec.RoomID, senderID spec.UserID) (fclient.SigningIdentity, error)
	FSAPI                   fedapi.RoomserverFederationAPI
	RSAPI                   api.RoomserverInternalAPI
	KeyRing                 gomatrixserverlib.JSONVerifier
	ACLs                    *acls.ServerACLs
	OutputProducer          *producers.RoomEventProducer
	Queryer                 *query.Queryer
	UserAPI                 userapi.RoomserverUserAPI
	EnableMetrics           bool
	repliesTopicRef         string
	inputRoomEventsTopicRef string
}

func NewInputer(
	ctx context.Context,
	cfg *config.RoomServer,
	db storage.RoomDatabase,
	qm queueutil.QueueManager,
	am actorutil.ActorManager,
	serverName spec.ServerName,
	signingIdentity func(ctx context.Context, roomID spec.RoomID, senderID spec.UserID) (fclient.SigningIdentity, error),
	fsAPI fedapi.RoomserverFederationAPI,
	rsAPI api.RoomserverInternalAPI,
	keyRing gomatrixserverlib.JSONVerifier,
	acls *acls.ServerACLs,
	outputProducer *producers.RoomEventProducer,
	queryer *query.Queryer,
	userAPI userapi.RoomserverUserAPI,
	enableMetrics bool,
) (*Inputer, error) {
	c := &Inputer{
		Cfg:             cfg,
		DB:              db,
		Qm:              qm,
		ServerName:      serverName,
		SigningIdentity: signingIdentity,
		FSAPI:           fsAPI,
		RSAPI:           rsAPI,
		KeyRing:         keyRing,
		ACLs:            acls,
		OutputProducer:  outputProducer,
		Queryer:         queryer,
		UserAPI:         userAPI,
		EnableMetrics:   enableMetrics,
	}

	am.EnableFunction(actorutil.ActorFunctionRoomServer, &cfg.Queues.InputRoomEvent, c.HandleRoomEvent)
	c.Am = am

	err := c.Qm.EnsurePublisherOk(ctx, &cfg.Queues.InputRoomEvent)
	if err != nil {
		return nil, err
	}
	c.inputRoomEventsTopicRef = cfg.Queues.InputRoomEvent.Ref()

	replyPubOpts, err := replyQOpts(ctx, &cfg.Queues.InputRoomEvent, "", false)
	if err != nil {
		return nil, err
	}

	err = c.Qm.EnsurePublisherOk(ctx, replyPubOpts)
	if err != nil {
		return nil, err
	}
	c.repliesTopicRef = replyPubOpts.Ref()

	inputQOpts := cfg.Queues.InputRoomEvent
	inputQOpts.DS = inputQOpts.DS.RemoveQuery("subject")

	err = c.Qm.RegisterSubscriber(ctx, &inputQOpts, c)
	return c, err
}

func (r *Inputer) Handle(ctx context.Context, metadata map[string]string, _ []byte) error {

	roomId, err := constants.DecodeRoomID(metadata[constants.RoomID])
	if err != nil {
		return err
	}

	_, err = r.Am.Progress(ctx, actorutil.ActorFunctionRoomServer, roomId)
	if err != nil {
		return err
	}

	return nil
}

func replyQOpts(_ context.Context, opts *config.QueueOptions, requestID string, isSubscriber bool) (*config.QueueOptions, error) {

	suffixedReplySubject := fmt.Sprintf("%s_Reply", constants.InputRoomEvent)

	ds := opts.DS
	if ds.IsNats() {

		ds = ds.RemoveQuery("subject", "consumer_headers_only", "consumer_durable_name", constants.QueueHeaderToExtendSubject).
			ExtendQuery("stream_subjects", fmt.Sprintf("%s.*", suffixedReplySubject)).
			ExtendQuery("stream_name", suffixedReplySubject).
			ExtendQuery("stream_storage", "memory")

		if isSubscriber {

			// durable := strings.ReplaceAll(fmt.Sprintf("Durable_%s_%s", suffixedReplySubject, requestID), ".", "_")

			ds = ds.ExtendQuery("consumer_filter_subject", fmt.Sprintf("%s.%s", suffixedReplySubject, requestID))

		} else {
			ds = ds.ExtendQuery("subject", suffixedReplySubject).
				ExtendQuery(constants.QueueHeaderToExtendSubject, constants.SynchronousReplyMsgID)
		}
	} else {
		ds = config.DataSource(fmt.Sprintf("mem://%s", suffixedReplySubject))
	}

	return &config.QueueOptions{
		QReference: fmt.Sprintf("%s_%s", suffixedReplySubject, requestID),
		Prefix:     opts.Prefix,
		DS:         ds,
	}, nil
}

// HandleRoomEvent is called by the worker for the room. It must only be called
// by the actor embedded into the worker.
func (r *Inputer) HandleRoomEvent(ctx context.Context, metadata map[string]string, message []byte) error {

	// Try to unmarshal the input room event. If the JSON unmarshalling
	// fails then we'll terminate the message — this notifies NATS that
	// we are done with the message and never want to see it again.
	var inputRoomEvent api.InputRoomEvent
	err := json.Unmarshal(message, &inputRoomEvent)
	if err != nil {
		return nil
	}

	// Process the room event. If something goes wrong then we'll tell
	// NATS to terminate the message. We'll store the error result as
	// a string, because we might want to return that to the caller if
	// it was a synchronous request.
	var errString string
	err = r.processRoomEvent(
		ctx,
		spec.ServerName(metadata["virtual_host"]),
		&inputRoomEvent,
	)
	if err != nil {

		errString = err.Error()

		var rejectedError types.RejectedError
		switch {
		case errors.As(err, &rejectedError):
			// Don't send events that were rejected to Sentry
			util.Log(ctx).WithError(err).
				WithField("room_id", inputRoomEvent.Event.RoomID()).
				WithField("event_id", inputRoomEvent.Event.EventID()).
				WithField("type", inputRoomEvent.Event.Type()).
				Warn("Roomserver rejected event")

		default:

			util.Log(ctx).WithError(err).
				WithField("room_id", inputRoomEvent.Event.RoomID()).
				WithField("event_id", inputRoomEvent.Event.EventID()).
				WithField("type", inputRoomEvent.Event.Type()).
				Error("Roomserver failed to process event")

		}

		// Even though we failed to process this message (e.g. due to Matrix restarting and receiving a context canceled),
		// the message may already have been queued for redelivery or will be, so this makes sure that we still reprocess the msg
		// after restarting. We only Ack if the context was not yet canceled.

		if ctx.Err() == nil {
			err = nil
		}

	}

	// If it was a synchronous input request then the "sync" field
	// will be present in the message. That means that someone is
	// waiting for a response. The temporary inbox name is present in
	// that field, so send back the error string (if any). If there
	// was no error then we'll return a blank message, which means
	// that everything was OK.
	responseMsgID, ok := metadata[constants.SynchronousReplyMsgID]
	if ok {

		log := util.Log(ctx).
			WithField("error", errString).
			WithField("room_id", inputRoomEvent.Event.RoomID()).
			WithField("event_id", inputRoomEvent.Event.EventID()).
			WithField("type", inputRoomEvent.Event.Type()).
			WithField("sync_reply_msg_id", responseMsgID)

		err0 := r.Qm.Publish(ctx, r.repliesTopicRef, errString, map[string]string{
			constants.SynchronousReplyMsgID: responseMsgID,
		})
		if err0 != nil {
			log.WithError(err0).WithField("room_id", inputRoomEvent.Event.RoomID()).
				WithField("event_id", inputRoomEvent.Event.EventID()).
				WithField("type", inputRoomEvent.Event.Type()).
				WithField("sync_reply_msg_id", responseMsgID).
				Warn("Roomserver failed to respond for sync event")
		}
	}
	return err
}

// queueInputRoomEvents queues events into the roomserver input
// stream in NATS.
func (r *Inputer) queueInputRoomEvents(
	ctx context.Context,
	request *api.InputRoomEventsRequest,
) (replySub frame.Subscriber, err error) {
	// If the request is synchronous then we need to create a
	// temporary inbox to wait for responses on, and then create
	// a subscription to it. If it's asynchronous then we won't
	// bother, so these values will remain empty.

	var replyToOpts *config.QueueOptions
	var requestMsgID string
	if !request.Asynchronous {
		requestMsgID = frame.GenerateID(ctx)
		replyToOpts, err = replyQOpts(ctx, &r.Cfg.Queues.InputRoomEvent, requestMsgID, true)
		if err != nil {
			return nil, err
		}
		err = r.Qm.RegisterSubscriber(ctx, replyToOpts)
		if err != nil {
			return nil, err
		}

		replySub, err = r.Qm.GetSubscriber(replyToOpts.Ref())
		if err != nil {
			return nil, err
		}
		if replySub == nil {
			// This shouldn't ever happen, but it doesn't hurt to check
			// because we can potentially avoid a nil pointer panic later
			// if it did for some reason.
			return nil, fmt.Errorf("expected a subscription to the temporary inbox")
		}
	}

	// For each event, marshal the input room event and then
	// send it into the input queue.
	for _, e := range request.InputRoomEvents {

		roomID := e.Event.RoomID()

		header := map[string]string{
			constants.RoomID: constants.EncodeRoomID(&roomID),
			"virtual_host":   string(request.VirtualHost),
		}

		if replyToOpts != nil {
			header[constants.SynchronousReplyMsgID] = requestMsgID
		}

		err = r.Qm.Publish(ctx, r.inputRoomEventsTopicRef, e, header)
		if err != nil {
			util.Log(ctx).WithError(err).WithField("room_id", roomID).
				WithField("event_id", e.Event.EventID()).
				Error("Roomserver failed to queue async event")
			return nil, fmt.Errorf("r.Qm.Publish: %w", err)
		}
	}
	return
}

// InputRoomEvents implements api.RoomserverInternalAPI
func (r *Inputer) InputRoomEvents(
	ctx context.Context,
	request *api.InputRoomEventsRequest,
	response *api.InputRoomEventsResponse,
) {
	// Queue up the event into the roomserver.
	replySub, err := r.queueInputRoomEvents(ctx, request)
	if err != nil {
		response.ErrMsg = err.Error()
		return
	}

	// If we aren't waiting for synchronous responses then we can
	// give up here, there is nothing further to do.
	if replySub == nil {
		return
	}

	// Otherwise, we'll want to sit and wait for the responses
	// from the roomserver. There will be one response for every
	// input we submitted. The last error value we receive will
	// be the one returned as the error string.
	defer func(replySub frame.Subscriber, ctx context.Context) {
		err = r.Qm.DiscardSubscriber(ctx, replySub.Ref())
		if err != nil {
			util.Log(ctx).WithError(err).Error("Roomserver failed to stop subscriber")
		}
	}(replySub, ctx) // nolint:errcheck

	receivedResponseCount := 0

	// Set up a goroutine to cancel our wait operations if the parent context is cancelled
	for range len(request.InputRoomEvents) {

		select {
		case <-ctx.Done():
			return
		default:

			msg, err0 := replySub.Receive(ctx)
			if err0 != nil {
				util.Log(ctx).WithError(err0).Error("Roomserver failed to receive response")
				response.ErrMsg = err0.Error()
				return
			}

			if msg != nil {
				msg.Ack()
				receivedResponseCount++

				if len(msg.Body) > 0 {
					if errMsg := string(msg.Body); errMsg != "" {
						response.ErrMsg = strings.Join([]string{response.ErrMsg, errMsg}, "\n")
					}
				}
			}

		}
	}
}
