// Copyright 2022 The Global.org Foundation C.I.C.
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
	"github.com/antinvestor/matrix/internal/queueutil"
	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/constants"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/pitabwire/util"
)

// OutputPresenceConsumer consumes events that originate in the clientapi.
type OutputPresenceConsumer struct {
	qm                      queueutil.QueueManager
	db                      storage.Database
	queues                  *queue.OutgoingQueues
	isLocalServerName       func(spec.ServerName) bool
	rsAPI                   roomserverAPI.FederationRoomserverAPI
	outboundPresenceEnabled bool
}

// NewOutputPresenceConsumer creates a new OutputPresenceConsumer. Call Start() to begin consuming events.
func NewOutputPresenceConsumer(
	ctx context.Context,
	cfg *config.FederationAPI,
	qm queueutil.QueueManager,
	queues *queue.OutgoingQueues,
	store storage.Database,
	rsAPI roomserverAPI.FederationRoomserverAPI,
) error {
	c := &OutputPresenceConsumer{
		qm:                      qm,
		queues:                  queues,
		db:                      store,
		isLocalServerName:       cfg.Global.IsLocalServerName,
		outboundPresenceEnabled: cfg.Global.Presence.EnableOutbound,
		rsAPI:                   rsAPI,
	}

	return qm.RegisterSubscriber(ctx, &cfg.Queues.OutputPresenceEvent, c)
}

// Handle onMessage is called in response to a message received on the presence
// events topic from the client api.
func (t *OutputPresenceConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {
	// only send presence events which originated from us
	userID := metadata[constants.UserID]
	_, serverName, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		util.Log(ctx).WithError(err).
			WithField("component", "federation_consumer").
			WithField("user_id", userID).
			Error("failed to extract domain from receipt sender")
		return nil
	}
	if !t.isLocalServerName(serverName) {
		return nil
	}

	parsedUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		util.Log(ctx).WithError(err).
			WithField("component", "federation_consumer").
			WithField("user_id", userID).
			Error("invalid user ID")
		return nil
	}

	roomIDs, err := t.rsAPI.QueryRoomsForUser(ctx, *parsedUserID, "join")
	if err != nil {
		util.Log(ctx).WithError(err).
			WithField("component", "federation_consumer").
			Error("failed to calculate joined rooms for user")
		return nil
	}

	roomIDStrs := make([]string, len(roomIDs))
	for i, roomID := range roomIDs {
		roomIDStrs[i] = roomID.String()
	}

	presence := metadata["presence"]

	ts, err := strconv.Atoi(metadata["last_active_ts"])
	if err != nil {
		return nil
	}

	// send this presence to all servers who share rooms with this user.
	joined, err := t.db.GetJoinedHostsForRooms(ctx, roomIDStrs, true, true)
	if err != nil {
		util.Log(ctx).WithError(err).
			WithField("component", "federation_consumer").
			Error("failed to get joined hosts")
		return nil
	}

	if len(joined) == 0 {
		return nil
	}

	var statusMsg *string = nil
	if data, ok := metadata["status_msg"]; ok && len(data) > 0 {
		status := metadata["status_msg"]
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
		util.Log(ctx).WithError(err).
			WithField("component", "federation_consumer").
			Error("failed to marshal EDU JSON")
		return nil
	}

	util.Log(ctx).
		WithField("component", "federation_consumer").
		WithField("server_count", len(joined)).
		Debug("sending presence EDU to servers")

	if err = t.queues.SendEDU(ctx, edu, serverName, joined); err != nil {
		util.Log(ctx).WithError(err).
			WithField("component", "federation_consumer").
			Error("failed to send EDU")
		return err
	}

	return nil
}
