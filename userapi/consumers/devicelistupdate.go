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
	"time"

	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/pitabwire/frame"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/userapi/internal"
)

// DeviceListUpdateConsumer consumes device list updates that came in over federation.
type DeviceListUpdateConsumer struct {
	qm                queueutil.QueueManager
	updater           *internal.DeviceListUpdater
	isLocalServerName func(spec.ServerName) bool
}

// NewDeviceListUpdateConsumer creates a new DeviceListConsumer. Call Start() to begin consuming from key servers.
func NewDeviceListUpdateConsumer(
	ctx context.Context,
	cfg *config.UserAPI,
	qm queueutil.QueueManager,
	updater *internal.DeviceListUpdater,
) error {
	c := &DeviceListUpdateConsumer{
		qm:                qm,
		updater:           updater,
		isLocalServerName: cfg.Global.IsLocalServerName,
	}

	return qm.RegisterSubscriber(ctx, &cfg.Queues.InputDeviceListUpdate, c)
}

// Handle is called in response to a message received on the
// key change events topic from the key server.
func (t *DeviceListUpdateConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {

	var m gomatrixserverlib.DeviceListUpdateEvent
	if err := json.Unmarshal(message, &m); err != nil {
		frame.Log(ctx).WithError(err).Error("Failed to read from device list update input topic")
		return nil
	}
	origin := spec.ServerName(metadata["origin"])
	if _, serverName, err := gomatrixserverlib.SplitID('@', m.UserID); err != nil {
		return nil
	} else if t.isLocalServerName(serverName) {
		return nil
	} else if serverName != origin {
		return nil
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	err := t.updater.Update(timeoutCtx, m)
	if err != nil {
		frame.Log(ctx).
			WithField("user_id", m.UserID).
			WithField("device_id", m.DeviceID).
			WithField("stream_id", m.StreamID).
			WithField("prev_id", m.PrevID).
			WithError(err).
			Error("Failed to update device list")
		return err
	}
	return nil
}
