// Copyright 2020 The Global.org Foundation C.I.C.
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

	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/antinvestor/matrix/syncapi/notifier"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/streams"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/getsentry/sentry-go"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// OutputKeyChangeEventConsumer consumes events that originated in the key server.
type OutputKeyChangeEventConsumer struct {
	jetstream nats.JetStreamContext
	durable   string
	topic     string
	db        storage.Database
	notifier  *notifier.Notifier
	stream    streams.StreamProvider
	rsAPI     roomserverAPI.SyncRoomserverAPI
}

// NewOutputKeyChangeEventConsumer creates a new OutputKeyChangeEventConsumer.
// Call Start() to begin consuming from the key server.
func NewOutputKeyChangeEventConsumer(
	ctx context.Context,
	cfg *config.SyncAPI,
	topic string,
	js nats.JetStreamContext,
	rsAPI roomserverAPI.SyncRoomserverAPI,
	store storage.Database,
	notifier *notifier.Notifier,
	stream streams.StreamProvider,
) *OutputKeyChangeEventConsumer {
	s := &OutputKeyChangeEventConsumer{
		jetstream: js,
		durable:   cfg.Global.JetStream.Durable("SyncAPIKeyChangeConsumer"),
		topic:     topic,
		db:        store,
		rsAPI:     rsAPI,
		notifier:  notifier,
		stream:    stream,
	}

	return s
}

// Start consuming from the key server
func (s *OutputKeyChangeEventConsumer) Start(ctx context.Context) error {
	return jetstream.Consumer(
		ctx, s.jetstream, s.topic, s.durable, 1,
		s.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

func (s *OutputKeyChangeEventConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	var m api.DeviceMessage
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		logrus.WithError(err).Errorf("failed to read device message from key change topic")
		return true
	}
	if m.DeviceKeys == nil && m.OutputCrossSigningKeyUpdate == nil {
		// This probably shouldn't happen but stops us from panicking if we come
		// across an update that doesn't satisfy either types.
		return true
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

func (s *OutputKeyChangeEventConsumer) onDeviceKeyMessage(ctx context.Context, m api.DeviceMessage, deviceChangeID int64) bool {
	if m.DeviceKeys == nil {
		return true
	}
	output := m.DeviceKeys
	// work out who we need to notify about the new key
	var queryRes roomserverAPI.QuerySharedUsersResponse
	err := s.rsAPI.QuerySharedUsers(ctx, &roomserverAPI.QuerySharedUsersRequest{
		UserID:    output.UserID,
		LocalOnly: true,
	}, &queryRes)
	if err != nil {
		logrus.WithError(err).Error("syncapi: failed to QuerySharedUsers for key change event from key server")
		sentry.CaptureException(err)
		return true
	}
	// make sure we get our own key updates too!
	queryRes.UserIDsToCount[output.UserID] = 1
	posUpdate := types.StreamPosition(deviceChangeID)

	s.stream.Advance(posUpdate)
	for userID := range queryRes.UserIDsToCount {
		s.notifier.OnNewKeyChange(types.StreamingToken{DeviceListPosition: posUpdate}, userID, output.UserID)
	}

	return true
}

func (s *OutputKeyChangeEventConsumer) onCrossSigningMessage(ctx context.Context, m api.DeviceMessage, deviceChangeID int64) bool {
	output := m.CrossSigningKeyUpdate
	// work out who we need to notify about the new key
	var queryRes roomserverAPI.QuerySharedUsersResponse
	err := s.rsAPI.QuerySharedUsers(ctx, &roomserverAPI.QuerySharedUsersRequest{
		UserID:    output.UserID,
		LocalOnly: true,
	}, &queryRes)
	if err != nil {
		logrus.WithError(err).Error("syncapi: failed to QuerySharedUsers for key change event from key server")
		sentry.CaptureException(err)
		return true
	}
	// make sure we get our own key updates too!
	queryRes.UserIDsToCount[output.UserID] = 1
	posUpdate := types.StreamPosition(deviceChangeID)

	s.stream.Advance(posUpdate)
	for userID := range queryRes.UserIDsToCount {
		s.notifier.OnNewKeyChange(types.StreamingToken{DeviceListPosition: posUpdate}, userID, output.UserID)
	}

	return true
}
