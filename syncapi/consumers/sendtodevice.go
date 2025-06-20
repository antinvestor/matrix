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

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/constants"
	"github.com/antinvestor/matrix/syncapi/notifier"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/streams"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
	"github.com/tidwall/gjson"
)

// OutputSendToDeviceEventConsumer consumes events that originated in the EDU server.
type OutputSendToDeviceEventConsumer struct {
	qm                queueutil.QueueManager
	db                storage.Database
	userAPI           api.SyncKeyAPI
	isLocalServerName func(spec.ServerName) bool
	stream            streams.StreamProvider
	notifier          *notifier.Notifier
}

// NewOutputSendToDeviceEventConsumer creates a new OutputSendToDeviceEventConsumer.
// Call Start() to begin consuming from the EDU server.
func NewOutputSendToDeviceEventConsumer(
	ctx context.Context,
	cfg *config.SyncAPI,
	qm queueutil.QueueManager,
	store storage.Database,
	userAPI api.SyncKeyAPI,
	notifier *notifier.Notifier,
	stream streams.StreamProvider,
) error {
	c := &OutputSendToDeviceEventConsumer{
		qm:                qm,
		db:                store,
		userAPI:           userAPI,
		isLocalServerName: cfg.Global.IsLocalServerName,
		notifier:          notifier,
		stream:            stream,
	}

	return qm.RegisterSubscriber(ctx, &cfg.Queues.OutputSendToDeviceEvent, c)
}

func (s *OutputSendToDeviceEventConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {

	log := util.Log(ctx)

	userID := metadata[constants.UserID]
	_, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {

		log.WithError(err).Error("send-to-device: failed to split user id, dropping message")
		return nil
	}
	if !s.isLocalServerName(domain) {
		log.Debug("ignoring send-to-device event with destination %s", domain)
		return nil
	}

	var output types.OutputSendToDeviceEvent
	if err = json.Unmarshal(message, &output); err != nil {
		// If the message was invalid, log it and move on to the next message in the stream
		log.WithError(err).Error("send-to-device: message parse failure")
		return err
	}

	log = log.
		WithField("sender", output.Sender).
		WithField("user_id", output.UserID).
		WithField("device_id", output.DeviceID).
		WithField("event_type", output.Type)
	log.Debug("sync API received send-to-device event from the clientapi/federationsender")

	// Check we actually got the requesting device in our store, if we receive a room key request
	if output.Type == "m.room_key_request" {
		requestingDeviceID := gjson.GetBytes(output.SendToDeviceEvent.Content, "requesting_device_id").Str
		_, senderDomain, _ := gomatrixserverlib.SplitID('@', output.Sender)
		if requestingDeviceID != "" && !s.isLocalServerName(senderDomain) {
			// Mark the requesting device as stale, if we don't know about it.
			if err = s.userAPI.PerformMarkAsStaleIfNeeded(ctx, &api.PerformMarkAsStaleRequest{
				UserID: output.Sender, Domain: senderDomain, DeviceID: requestingDeviceID,
			}, &struct{}{}); err != nil {
				log.WithError(err).Error("failed to mark as stale if needed")
				return err
			}
		}
	}

	streamPos, err := s.db.StoreNewSendForDeviceMessage(
		ctx, output.UserID, output.DeviceID, output.SendToDeviceEvent,
	)
	if err != nil {

		log.WithError(err).Error("send-to-device: failed to store message")
		return err
	}

	s.stream.Advance(streamPos)
	s.notifier.OnNewSendToDevice(
		output.UserID,
		[]string{output.DeviceID},
		types.StreamingToken{SendToDevicePosition: streamPos},
	)

	return nil
}
