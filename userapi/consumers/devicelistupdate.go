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
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/userapi/internal"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
)

// DeviceListUpdateConsumer consumes device list updates that came in over federation.
type DeviceListUpdateConsumer struct {
	jetstream         nats.JetStreamContext
	durable           string
	topic             string
	updater           *internal.DeviceListUpdater
	isLocalServerName func(spec.ServerName) bool
}

// NewDeviceListUpdateConsumer creates a new DeviceListConsumer. Call Start() to begin consuming from key servers.
func NewDeviceListUpdateConsumer(
	_ context.Context,
	cfg *config.UserAPI,
	js nats.JetStreamContext,
	updater *internal.DeviceListUpdater,
) *DeviceListUpdateConsumer {
	return &DeviceListUpdateConsumer{
		jetstream:         js,
		durable:           cfg.Matrix.JetStream.Prefixed("KeyServerInputDeviceListConsumer"),
		topic:             cfg.Matrix.JetStream.Prefixed(jetstream.InputDeviceListUpdate),
		updater:           updater,
		isLocalServerName: cfg.Matrix.IsLocalServerName,
	}
}

// Start consuming from key servers
func (t *DeviceListUpdateConsumer) Start(ctx context.Context) error {
	return jetstream.Consumer(
		ctx, t.jetstream, t.topic, t.durable, 1,
		t.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

// onMessage is called in response to a message received on the
// key change events topic from the key server.
func (t *DeviceListUpdateConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	var m gomatrixserverlib.DeviceListUpdateEvent
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		logrus.WithError(err).Errorf("Failed to read from device list update input topic")
		return true
	}
	origin := spec.ServerName(msg.Header.Get("origin"))
	if _, serverName, err := gomatrixserverlib.SplitID('@', m.UserID); err != nil {
		return true
	} else if t.isLocalServerName(serverName) {
		return true
	} else if serverName != origin {
		return true
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	err := t.updater.Update(timeoutCtx, m)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"user_id":   m.UserID,
			"device_id": m.DeviceID,
			"stream_id": m.StreamID,
			"prev_id":   m.PrevID,
		}).WithError(err).Errorf("Failed to update device list")
		return false
	}
	return true
}
