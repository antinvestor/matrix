// Copyright 2025 Ant Investor Ltd.
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

	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/pitabwire/util"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"

	"github.com/antinvestor/matrix/federationapi/queue"
	"github.com/antinvestor/matrix/federationapi/storage"
	"github.com/antinvestor/matrix/federationapi/types"
	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/userapi/api"
)

// KeyChangeConsumer consumes events that originate in key server.
type KeyChangeConsumer struct {
	qm                queueutil.QueueManager
	db                storage.Database
	queues            *queue.OutgoingQueues
	isLocalServerName func(spec.ServerName) bool
	rsAPI             roomserverAPI.FederationRoomserverAPI
}

// NewKeyChangeConsumer creates a new KeyChangeConsumer. Call Start() to begin consuming from key servers.
func NewKeyChangeConsumer(
	ctx context.Context,
	cfg *config.KeyServer,
	qm queueutil.QueueManager,
	queues *queue.OutgoingQueues,
	store storage.Database,
	rsAPI roomserverAPI.FederationRoomserverAPI,
) error {
	c := &KeyChangeConsumer{
		qm:                qm,
		queues:            queues,
		db:                store,
		isLocalServerName: cfg.Global.IsLocalServerName,
		rsAPI:             rsAPI,
	}
	return qm.RegisterSubscriber(ctx, &cfg.Queues.OutputKeyChangeEvent, c)
}

// Handle is called in response to a message received on the
// key change events topic from the key server.
func (t *KeyChangeConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {
	var m api.DeviceMessage
	if err := json.Unmarshal(message, &m); err != nil {
		util.Log(ctx).WithError(err).
			WithField("component", "keychange_consumer").
			Error("Failed to read device message from key change topic")
		return nil
	}
	if m.DeviceKeys == nil && m.OutputCrossSigningKeyUpdate == nil {
		// This probably shouldn't happen but stops us from panicking if we come
		// across an update that doesn't satisfy either types.
		return nil
	}
	switch m.Type {
	case api.TypeCrossSigningUpdate:
		return t.onCrossSigningMessage(ctx, m)
	case api.TypeDeviceKeyUpdate:
		fallthrough
	default:
		return t.onDeviceKeyMessage(ctx, m)
	}
}

func (t *KeyChangeConsumer) onDeviceKeyMessage(ctx context.Context, m api.DeviceMessage) error {
	if m.DeviceKeys == nil {
		return nil
	}
	logger := util.Log(ctx).WithField("user_id", m.UserID).WithField("component", "keychange_consumer")

	// only send key change events which originated from us
	_, originServerName, err := gomatrixserverlib.SplitID('@', m.UserID)
	if err != nil {
		logger.WithError(err).Error("Failed to extract domain from key change event")
		return nil
	}
	if !t.isLocalServerName(originServerName) {
		return nil
	}

	userID, err := spec.NewUserID(m.UserID, true)
	if err != nil {
		logger.WithError(err).Error("invalid user ID")
		return nil
	}

	roomIDs, err := t.rsAPI.QueryRoomsForUser(ctx, *userID, "join")
	if err != nil {
		logger.WithError(err).Error("failed to calculate joined rooms for user")
		return nil
	}

	roomIDStrs := make([]string, len(roomIDs))
	for i, room := range roomIDs {
		roomIDStrs[i] = room.String()
	}

	// send this key change to all servers who share rooms with this user.
	destinations, err := t.db.GetJoinedHostsForRooms(ctx, roomIDStrs, true, true)
	if err != nil {
		logger.WithError(err).Error("failed to calculate joined hosts for rooms user is in")
		return nil
	}

	if len(destinations) == 0 {
		return nil
	}
	// Pack the EDU and marshal it
	edu := &gomatrixserverlib.EDU{
		Type:   spec.MDeviceListUpdate,
		Origin: string(originServerName),
	}
	event := gomatrixserverlib.DeviceListUpdateEvent{
		UserID:            m.UserID,
		DeviceID:          m.DeviceID,
		DeviceDisplayName: m.DisplayName,
		StreamID:          m.StreamID,
		PrevID:            prevID(m.StreamID),
		Deleted:           len(m.KeyJSON) == 0,
		Keys:              m.KeyJSON,
	}
	if edu.Content, err = json.Marshal(event); err != nil {

		logger.WithError(err).Error("failed to marshal EDU JSON")
		return nil
	}

	logger.WithField("destinations", destinations).Debug("Sending device list update message")
	return t.queues.SendEDU(ctx, edu, originServerName, destinations)
}

func (t *KeyChangeConsumer) onCrossSigningMessage(ctx context.Context, m api.DeviceMessage) error {
	output := m.CrossSigningKeyUpdate
	_, host, err := gomatrixserverlib.SplitID('@', output.UserID)
	if err != nil {
		util.Log(ctx).WithError(err).
			WithField("component", "keychange_consumer").
			Error("fedsender key change consumer: user ID parse failure")
		return nil
	}
	if !t.isLocalServerName(host) {
		// Ignore any messages that didn't originate locally, otherwise we'll
		// end up parroting information we received from other servers.
		return nil
	}
	logger := util.Log(ctx).WithField("user_id", output.UserID).WithField("component", "keychange_consumer")

	outputUserID, err := spec.NewUserID(output.UserID, true)
	if err != nil {
		logger.WithError(err).Error("invalid user ID")
		return nil
	}

	rooms, err := t.rsAPI.QueryRoomsForUser(ctx, *outputUserID, "join")
	if err != nil {
		logger.WithError(err).Error("fedsender key change consumer: failed to calculate joined rooms for user")
		return nil
	}

	roomIDStrs := make([]string, len(rooms))
	for i, room := range rooms {
		roomIDStrs[i] = room.String()
	}

	// send this key change to all servers who share rooms with this user.
	destinations, err := t.db.GetJoinedHostsForRooms(ctx, roomIDStrs, true, true)
	if err != nil {

		logger.WithError(err).Error("fedsender key change consumer: failed to calculate joined hosts for rooms user is in")
		return nil
	}

	if len(destinations) == 0 {
		return nil
	}

	// Pack the EDU and marshal it
	edu := &gomatrixserverlib.EDU{
		Type:   types.MSigningKeyUpdate,
		Origin: string(host),
	}
	if edu.Content, err = json.Marshal(output); err != nil {

		logger.WithError(err).Error("fedsender key change consumer: failed to marshal output, dropping")
		return nil
	}

	logger.WithField("destinations", destinations).Debug("Sending cross-signing update message")
	return t.queues.SendEDU(ctx, edu, host, destinations)
}

func prevID(streamID int64) []int64 {
	if streamID <= 1 {
		return nil
	}
	return []int64{streamID - 1}
}
