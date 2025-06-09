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
	"github.com/pitabwire/frame"

	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/syncapi/notifier"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/streams"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/userapi/api"
)

// OutputKeyChangeEventConsumer consumes events that originated in the key server.
type OutputKeyChangeEventConsumer struct {
	qm       queueutil.QueueManager
	topic    *config.QueueOptions
	db       storage.Database
	notifier *notifier.Notifier
	stream   streams.StreamProvider
	rsAPI    roomserverAPI.SyncRoomserverAPI
}

// NewOutputKeyChangeEventConsumer creates a new OutputKeyChangeEventConsumer.
// Call Start() to begin consuming from the key server.
func NewOutputKeyChangeEventConsumer(
	ctx context.Context,
	cfg *config.SyncAPI,
	qm queueutil.QueueManager,
	rsAPI roomserverAPI.SyncRoomserverAPI,
	store storage.Database,
	notifier *notifier.Notifier,
	stream streams.StreamProvider,
) error {

	s := &OutputKeyChangeEventConsumer{
		qm:       qm,
		db:       store,
		rsAPI:    rsAPI,
		notifier: notifier,
		stream:   stream,
	}

	return qm.RegisterSubscriber(ctx, &cfg.Queues.OutputKeyChangeEvent, s)
}

func (s *OutputKeyChangeEventConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {
	var m api.DeviceMessage
	if err := json.Unmarshal(message, &m); err != nil {
		frame.Log(ctx).WithError(err).Error("failed to read device message from key change topic")
		return nil
	}
	if m.DeviceKeys == nil && m.OutputCrossSigningKeyUpdate == nil {
		// This probably shouldn't happen but stops us from panicking if we come
		// across an update that doesn't satisfy either types.
		return nil
	}
	switch m.Type {
	case api.TypeCrossSigningUpdate:
		return s.onCrossSigningMessage(ctx, m, m.DeviceChangeID)
	case api.TypeDeviceKeyUpdate:
		fallthrough
	default:
		return s.onDeviceKeyMessage(ctx, m, m.DeviceChangeID)
	}
}

func (s *OutputKeyChangeEventConsumer) onDeviceKeyMessage(ctx context.Context, m api.DeviceMessage, deviceChangeID int64) error {
	if m.DeviceKeys == nil {
		return nil
	}
	output := m.DeviceKeys
	// work out who we need to notify about the new key
	var queryRes roomserverAPI.QuerySharedUsersResponse
	err := s.rsAPI.QuerySharedUsers(ctx, &roomserverAPI.QuerySharedUsersRequest{
		UserID:    output.UserID,
		LocalOnly: true,
	}, &queryRes)
	if err != nil {
		frame.Log(ctx).WithError(err).Error("syncapi: failed to QuerySharedUsers for key change event from key server")
		return err
	}
	// make sure we get our own key updates too!
	queryRes.UserIDsToCount[output.UserID] = 1
	posUpdate := types.StreamPosition(deviceChangeID)

	s.stream.Advance(posUpdate)
	for userID := range queryRes.UserIDsToCount {
		s.notifier.OnNewKeyChange(types.StreamingToken{DeviceListPosition: posUpdate}, userID, output.UserID)
	}

	return nil
}

func (s *OutputKeyChangeEventConsumer) onCrossSigningMessage(ctx context.Context, m api.DeviceMessage, deviceChangeID int64) error {
	output := m.CrossSigningKeyUpdate
	// work out who we need to notify about the new key
	var queryRes roomserverAPI.QuerySharedUsersResponse
	err := s.rsAPI.QuerySharedUsers(ctx, &roomserverAPI.QuerySharedUsersRequest{
		UserID:    output.UserID,
		LocalOnly: true,
	}, &queryRes)
	if err != nil {
		frame.Log(ctx).WithError(err).Error("syncapi: failed to QuerySharedUsers for key change event from key server")
		return err
	}
	// make sure we get our own key updates too!
	queryRes.UserIDsToCount[output.UserID] = 1
	posUpdate := types.StreamPosition(deviceChangeID)

	s.stream.Advance(posUpdate)
	for userID := range queryRes.UserIDsToCount {
		s.notifier.OnNewKeyChange(types.StreamingToken{DeviceListPosition: posUpdate}, userID, output.UserID)
	}

	return nil
}
