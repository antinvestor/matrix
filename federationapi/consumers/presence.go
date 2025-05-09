// Copyright 2022 The Matrix.org Foundation C.I.C.
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

package consumers

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/queue"
	"github.com/antinvestor/matrix/federationapi/storage"
	fedTypes "github.com/antinvestor/matrix/federationapi/types"
	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/nats-io/nats.go"
	"github.com/pitabwire/util"
	log "github.com/sirupsen/logrus"
)

// OutputReceiptConsumer consumes events that originate in the clientapi.
type OutputPresenceConsumer struct {
	jetstream               nats.JetStreamContext
	durable                 string
	db                      storage.Database
	queues                  *queue.OutgoingQueues
	isLocalServerName       func(spec.ServerName) bool
	rsAPI                   roomserverAPI.FederationRoomserverAPI
	topic                   string
	outboundPresenceEnabled bool
}

// NewOutputPresenceConsumer creates a new OutputPresenceConsumer. Call Start() to begin consuming events.
func NewOutputPresenceConsumer(
	_ context.Context,
	cfg *config.FederationAPI,
	js nats.JetStreamContext,
	queues *queue.OutgoingQueues,
	store storage.Database,
	rsAPI roomserverAPI.FederationRoomserverAPI,
) *OutputPresenceConsumer {
	return &OutputPresenceConsumer{
		jetstream:               js,
		queues:                  queues,
		db:                      store,
		isLocalServerName:       cfg.Matrix.IsLocalServerName,
		durable:                 cfg.Matrix.JetStream.Durable("FederationAPIPresenceConsumer"),
		topic:                   cfg.Matrix.JetStream.Prefixed(jetstream.OutputPresenceEvent),
		outboundPresenceEnabled: cfg.Matrix.Presence.EnableOutbound,
		rsAPI:                   rsAPI,
	}
}

// Start consuming from the clientapi
func (t *OutputPresenceConsumer) Start(ctx context.Context) error {
	if !t.outboundPresenceEnabled {
		return nil
	}
	return jetstream.Consumer(
		ctx, t.jetstream, t.topic, t.durable, 1, t.onMessage,
		nats.DeliverAll(), nats.ManualAck(), nats.HeadersOnly(),
	)
}

// onMessage is called in response to a message received on the presence
// events topic from the client api.
func (t *OutputPresenceConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	// only send presence events which originated from us
	userID := msg.Header.Get(jetstream.UserID)
	_, serverName, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		log.WithError(err).WithField("user_id", userID).Error("failed to extract domain from receipt sender")
		return true
	}
	if !t.isLocalServerName(serverName) {
		return true
	}

	parsedUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		util.GetLogger(ctx).WithError(err).WithField("user_id", userID).Error("invalid user ID")
		return true
	}

	roomIDs, err := t.rsAPI.QueryRoomsForUser(ctx, *parsedUserID, "join")
	if err != nil {
		log.WithError(err).Error("failed to calculate joined rooms for user")
		return true
	}

	roomIDStrs := make([]string, len(roomIDs))
	for i, roomID := range roomIDs {
		roomIDStrs[i] = roomID.String()
	}

	presence := msg.Header.Get("presence")

	ts, err := strconv.Atoi(msg.Header.Get("last_active_ts"))
	if err != nil {
		return true
	}

	// send this presence to all servers who share rooms with this user.
	joined, err := t.db.GetJoinedHostsForRooms(ctx, roomIDStrs, true, true)
	if err != nil {
		log.WithError(err).Error("failed to get joined hosts")
		return true
	}

	if len(joined) == 0 {
		return true
	}

	var statusMsg *string = nil
	if data, ok := msg.Header["status_msg"]; ok && len(data) > 0 {
		status := msg.Header.Get("status_msg")
		statusMsg = &status
	}

	p := types.PresenceInternal{LastActiveTS: spec.Timestamp(ts)}

	content := fedTypes.Presence{
		Push: []fedTypes.PresenceContent{
			{
				CurrentlyActive: p.CurrentlyActive(),
				LastActiveAgo:   p.LastActiveAgo(),
				Presence:        presence,
				StatusMsg:       statusMsg,
				UserID:          userID,
			},
		},
	}

	edu := &gomatrixserverlib.EDU{
		Type:   spec.MPresence,
		Origin: string(serverName),
	}
	if edu.Content, err = json.Marshal(content); err != nil {
		log.WithError(err).Error("failed to marshal EDU JSON")
		return true
	}

	log.Tracef("sending presence EDU to %d servers", len(joined))
	if err = t.queues.SendEDU(ctx, edu, serverName, joined); err != nil {
		log.WithError(err).Error("failed to send EDU")
		return false
	}

	return true
}
