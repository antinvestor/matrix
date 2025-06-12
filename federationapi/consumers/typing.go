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

	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/pitabwire/util"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/queue"
	"github.com/antinvestor/matrix/federationapi/storage"
	"github.com/antinvestor/matrix/setup/config"
)

// OutputTypingConsumer consumes events that originate in the clientapi.
type OutputTypingConsumer struct {
	qm                queueutil.QueueManager
	db                storage.Database
	queues            *queue.OutgoingQueues
	isLocalServerName func(spec.ServerName) bool
}

// NewOutputTypingConsumer creates a new OutputTypingConsumer. Call Start() to begin consuming typing events.
func NewOutputTypingConsumer(
	ctx context.Context,
	cfg *config.FederationAPI,
	qm queueutil.QueueManager,
	queues *queue.OutgoingQueues,
	store storage.Database,
) error {
	c := &OutputTypingConsumer{
		qm:                qm,
		queues:            queues,
		db:                store,
		isLocalServerName: cfg.Global.IsLocalServerName,
	}

	return qm.RegisterSubscriber(ctx, &cfg.Queues.OutputTypingEvent, c)
}

// Handle is called in response to a message received on the typing
// events topic from the client api.
func (t *OutputTypingConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {
	// Extract the typing event from msg.
	roomID := metadata[queueutil.RoomID]
	userID := metadata[queueutil.UserID]
	typing, err := strconv.ParseBool(metadata["typing"])
	if err != nil {
		util.Log(ctx).WithError(err).Error("EDU output log: typing parse failure")
		return nil
	}

	// only send typing events which originated from us
	_, typingServerName, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("user_id", userID).Error("Failed to extract domain from typing sender")
		return nil
	}
	if !t.isLocalServerName(typingServerName) {
		return nil
	}

	joined, err := t.db.GetJoinedHosts(ctx, roomID)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("room_id", roomID).Error("failed to get joined hosts for room")
		return err
	}

	names := make([]spec.ServerName, len(joined))
	for i := range joined {
		names[i] = joined[i].ServerName
	}

	edu := &gomatrixserverlib.EDU{Type: "m.typing"}
	if edu.Content, err = json.Marshal(map[string]interface{}{
		"room_id": roomID,
		"user_id": userID,
		"typing":  typing,
	}); err != nil {
		util.Log(ctx).WithError(err).Error("failed to marshal EDU JSON")
		return nil
	}
	err = t.queues.SendEDU(ctx, edu, typingServerName, names)
	if err != nil {
		util.Log(ctx).WithError(err).Error("failed to send EDU")
		return err
	}

	return nil
}
