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

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/getsentry/sentry-go"
	"github.com/nats-io/nats.go"
	"github.com/pitabwire/util"
	log "github.com/sirupsen/logrus"

	"github.com/antinvestor/matrix/federationapi/queue"
	"github.com/antinvestor/matrix/federationapi/storage"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/antinvestor/matrix/syncapi/types"
)

// OutputSendToDeviceConsumer consumes events that originate in the clientapi.
type OutputSendToDeviceConsumer struct {
	jetstream         nats.JetStreamContext
	durable           string
	db                storage.Database
	queues            *queue.OutgoingQueues
	isLocalServerName func(spec.ServerName) bool
	topic             string
}

// NewOutputSendToDeviceConsumer creates a new OutputSendToDeviceConsumer. Call Start() to begin consuming send-to-device events.
func NewOutputSendToDeviceConsumer(
	_ context.Context,
	cfg *config.FederationAPI,
	js nats.JetStreamContext,
	queues *queue.OutgoingQueues,
	store storage.Database,
) *OutputSendToDeviceConsumer {
	return &OutputSendToDeviceConsumer{
		jetstream:         js,
		queues:            queues,
		db:                store,
		isLocalServerName: cfg.Matrix.IsLocalServerName,
		durable:           cfg.Matrix.JetStream.Durable("FederationAPIESendToDeviceConsumer"),
		topic:             cfg.Matrix.JetStream.Prefixed(jetstream.OutputSendToDeviceEvent),
	}
}

// Start consuming from the client api
func (t *OutputSendToDeviceConsumer) Start(ctx context.Context) error {
	return jetstream.Consumer(
		ctx, t.jetstream, t.topic, t.durable, 1,
		t.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

// onMessage is called in response to a message received on the
// send-to-device events topic from the client api.
func (t *OutputSendToDeviceConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	// only send send-to-device events which originated from us
	sender := msg.Header.Get("sender")
	_, originServerName, err := gomatrixserverlib.SplitID('@', sender)
	if err != nil {
		sentry.CaptureException(err)
		log.WithError(err).WithField("user_id", sender).Error("Failed to extract domain from send-to-device sender")
		return true
	}
	if !t.isLocalServerName(originServerName) {
		return true
	}
	// Extract the send-to-device event from msg.
	var ote types.OutputSendToDeviceEvent
	if err = json.Unmarshal(msg.Data, &ote); err != nil {
		sentry.CaptureException(err)
		log.WithError(err).Errorf("output log: message parse failed (expected send-to-device)")
		return true
	}

	_, destServerName, err := gomatrixserverlib.SplitID('@', ote.UserID)
	if err != nil {
		sentry.CaptureException(err)
		log.WithError(err).WithField("user_id", ote.UserID).Error("Failed to extract domain from send-to-device destination")
		return true
	}

	// The SyncAPI is already handling sendToDevice for the local server
	if t.isLocalServerName(destServerName) {
		return true
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
		sentry.CaptureException(err)
		log.WithError(err).Error("failed to marshal EDU JSON")
		return true
	}

	log.Debugf("Sending send-to-device message into %q destination queue", destServerName)
	if err := t.queues.SendEDU(ctx, edu, originServerName, []spec.ServerName{destServerName}); err != nil {
		log.WithError(err).Error("failed to send EDU")
		return false
	}

	return true
}
