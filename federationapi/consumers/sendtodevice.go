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

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/pitabwire/util"

	"github.com/antinvestor/matrix/federationapi/queue"
	"github.com/antinvestor/matrix/federationapi/storage"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/syncapi/types"
)

// OutputSendToDeviceConsumer consumes events that originate in the clientapi.
type OutputSendToDeviceConsumer struct {
	qm                queueutil.QueueManager
	db                storage.Database
	queues            *queue.OutgoingQueues
	isLocalServerName func(spec.ServerName) bool
}

// NewOutputSendToDeviceConsumer creates a new OutputSendToDeviceConsumer. Call Start() to begin consuming send-to-device events.
func NewOutputSendToDeviceConsumer(
	ctx context.Context,
	cfg *config.FederationAPI,
	qm queueutil.QueueManager,
	queues *queue.OutgoingQueues,
	store storage.Database,
) error {
	c := &OutputSendToDeviceConsumer{
		qm:                qm,
		queues:            queues,
		db:                store,
		isLocalServerName: cfg.Global.IsLocalServerName,
	}

	return qm.RegisterSubscriber(ctx, &cfg.Queues.OutputSendToDeviceEvent, c)
}

// Handle is called in response to a message received on the
// send-to-device events topic from the client api.
func (t *OutputSendToDeviceConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {

	log := util.Log(ctx)
	// only send send-to-device events which originated from us
	sender := metadata["sender"]
	_, originServerName, err := gomatrixserverlib.SplitID('@', sender)
	if err != nil {

		log.WithError(err).WithField("user_id", sender).Error("Failed to extract domain from send-to-device sender")
		return nil
	}
	if !t.isLocalServerName(originServerName) {
		return nil
	}
	// Extract the send-to-device event from msg.
	var ote types.OutputSendToDeviceEvent
	if err = json.Unmarshal(message, &ote); err != nil {

		log.WithError(err).Error("output log: message parse failed (expected send-to-device)")
		return nil
	}

	_, destServerName, err := gomatrixserverlib.SplitID('@', ote.UserID)
	if err != nil {

		log.WithError(err).WithField("user_id", ote.UserID).Error("Failed to extract domain from send-to-device destination")
		return nil
	}

	// The SyncAPI is already handling sendToDevice for the local server
	if t.isLocalServerName(destServerName) {
		return nil
	}

	// Pack the EDU and marshal it
	edu := &gomatrixserverlib.EDU{
		Type:   spec.MDirectToDevice,
		Origin: string(originServerName),
	}
	tdm := gomatrixserverlib.ToDeviceMessage{
		Sender:    ote.Sender,
		Type:      ote.Type,
		MessageID: util.RandomString(32),
		Messages: map[string]map[string]json.RawMessage{
			ote.UserID: {
				ote.DeviceID: ote.Content,
			},
		},
	}
	if edu.Content, err = json.Marshal(tdm); err != nil {

		log.WithError(err).Error("failed to marshal EDU JSON")
		return nil
	}

	log.Debug("Sending send-to-device message into %q destination queue", destServerName)
	err = t.queues.SendEDU(ctx, edu, originServerName, []spec.ServerName{destServerName})
	if err != nil {
		log.WithError(err).Error("failed to send EDU")
		return err
	}

	return nil
}
