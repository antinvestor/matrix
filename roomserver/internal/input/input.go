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
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	fedapi "github.com/antinvestor/matrix/federationapi/api"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/roomserver/acls"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/internal/actor"
	"github.com/antinvestor/matrix/roomserver/internal/query"
	"github.com/antinvestor/matrix/roomserver/producers"
	"github.com/antinvestor/matrix/roomserver/storage"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/frame"
	"github.com/sirupsen/logrus"
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
	Cfg             *config.RoomServer
	DB              storage.RoomDatabase
	Qm              queueutil.QueueManager
	ServerName      spec.ServerName
	SigningIdentity func(ctx context.Context, roomID spec.RoomID, senderID spec.UserID) (fclient.SigningIdentity, error)
	FSAPI           fedapi.RoomserverFederationAPI
	RSAPI           api.RoomserverInternalAPI
	KeyRing         gomatrixserverlib.JSONVerifier
	ACLs            *acls.ServerACLs
	OutputProducer  *producers.RoomEventProducer
	Queryer         *query.Queryer
	UserAPI         userapi.RoomserverUserAPI
	actorSystem     *actor.RoomActorSystem
	EnableMetrics   bool
}

func NewInputer(
	ctx context.Context,
	cfg *config.RoomServer,
	db storage.RoomDatabase,
	qm queueutil.QueueManager,
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

	c.actorSystem = actor.NewRoomActorSystem(ctx, &cfg.ActorSystem, qm, c.HandleRoomEvent)
	err := c.actorSystem.Start(ctx)
	if err != nil {
		return nil, err
	}

	err = c.Qm.RegisterSubscriber(ctx, &cfg.Queues.InputRoomEvent, c)
	return c, err
}

func (r *Inputer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {

	roomId, err := spec.NewRoomID(metadata["room_id"])
	if err != nil {
		return err
	}

	opt := &r.Cfg.Queues.InputRoomEvent
	roomOpts, err := actor.RoomifyQOpts(ctx, opt, roomId, true)
	if err != nil {
		return err
	}

	err = r.actorSystem.EnsureRoomActorExists(ctx, roomId, roomOpts.DS)
	if err != nil {
		return err
	}

	if !roomOpts.DS.IsNats() {
		err = r.Qm.EnsurePublisherOk(ctx, roomOpts)
		if err != nil {
			return err
		}

		err = r.Qm.Publish(ctx, roomOpts.Ref(), message, metadata)
		if err != nil {
			return err
		}

	}

	return nil
}

func replyQOpts(ctx context.Context, opts *config.QueueOptions) (*config.QueueOptions, error) {

	replyTo := fmt.Sprintf("__ReplyQueue_%s", frame.GenerateID(ctx))

	ds := opts.DS
	if ds.IsNats() {

		ds = ds.RemoveQuery("jetstream", "stream_storage", "stream_retention")
		ds = ds.ExtendQuery("subject", replyTo)

	} else {
		ds = config.DataSource(fmt.Sprintf("mem://%s", replyTo))
	}

	return &config.QueueOptions{
		QReference: fmt.Sprintf("%s%s", opts.QReference, replyTo),
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
		var rejectedError types.RejectedError
		switch {
		case errors.As(err, &rejectedError):
			// Don't send events that were rejected to Sentry
			logrus.WithError(err).WithFields(logrus.Fields{
				"room_id":  inputRoomEvent.Event.RoomID(),
				"event_id": inputRoomEvent.Event.EventID(),
				"type":     inputRoomEvent.Event.Type(),
			}).Warn("Roomserver rejected event")
			err = nil
		default:

			logrus.WithError(err).WithFields(logrus.Fields{
				"room_id":  inputRoomEvent.Event.RoomID(),
				"event_id": inputRoomEvent.Event.EventID(),
				"type":     inputRoomEvent.Event.Type(),
			}).Warn("Roomserver failed to process event")

			// Even though we failed to process this message (e.g. due to Matrix restarting and receiving a context canceled),
			// the message may already have been queued for redelivery or will be, so this makes sure that we still reprocess the msg
			// after restarting. We only Ack if the context was not yet canceled.
			errString = err.Error()
		}

	}

	// If it was a synchronous input request then the "sync" field
	// will be present in the message. That means that someone is
	// waiting for a response. The temporary inbox name is present in
	// that field, so send back the error string (if any). If there
	// was no error then we'll return a blank message, which means
	// that everything was OK.
	replyTo, ok := metadata["sync_uri"]
	if ok {

		replyToOpts := &config.QueueOptions{
			DS:         config.DataSource(replyTo),
			Prefix:     metadata["sync_prefix"],
			QReference: metadata["sync_ref"],
		}

		err0 := r.Qm.EnsurePublisherOk(ctx, replyToOpts)
		if err0 != nil {
			logrus.WithError(err0).WithFields(logrus.Fields{
				"room_id":  inputRoomEvent.Event.RoomID(),
				"event_id": inputRoomEvent.Event.EventID(),
				"type":     inputRoomEvent.Event.Type(),
				"replyTo":  replyTo,
			}).Warn("Publisher to reply to is not available")
			return nil
		}
		err0 = r.Qm.Publish(ctx, replyToOpts.Ref(), []byte(errString))
		if err0 != nil {
			logrus.WithError(err0).WithFields(logrus.Fields{
				"room_id":  inputRoomEvent.Event.RoomID(),
				"event_id": inputRoomEvent.Event.EventID(),
				"type":     inputRoomEvent.Event.Type(),
				"replyTo":  replyTo,
			}).Warn("Roomserver failed to respond for sync event")
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
	if !request.Asynchronous {
		replyToOpts, err = replyQOpts(ctx, &r.Cfg.Queues.InputRoomEvent)
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
			"room_id":      roomID.String(),
			"virtual_host": string(request.VirtualHost),
		}

		if replyToOpts != nil {
			header["sync_prefix"] = replyToOpts.Prefix
			header["sync_ref"] = replyToOpts.Ref()
			header["sync_uri"] = string(replyToOpts.DS)
		}

		roomOpts, err0 := actor.RoomifyQOpts(ctx, &r.Cfg.Queues.InputRoomEvent, &roomID, false)
		if err0 != nil {
			return nil, err0
		}

		err = r.Qm.EnsurePublisherOk(ctx, roomOpts)
		if err != nil {
			return nil, err
		}

		err = r.Qm.Publish(ctx, roomOpts.Ref(), e, header)
		if err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"room_id":  roomID,
				"event_id": e.Event.EventID(),
				"subj_ref": roomOpts.Ref(),
				"subj_uri": roomOpts.DS,
			}).Error("Roomserver failed to queue async event")
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
		err = replySub.Stop(ctx)
		if err != nil {
			logrus.WithError(err).Error("Roomserver failed to stop subscriber")
		}
	}(replySub, ctx) // nolint:errcheck
	for i := 0; i < len(request.InputRoomEvents); i++ {
		msg, err0 := replySub.Receive(ctx)
		if err0 != nil {
			response.ErrMsg = err0.Error()
			return
		}
		msg.Ack()
		if len(msg.Body) > 0 {
			response.ErrMsg = string(msg.Body)
		}

	}
}
