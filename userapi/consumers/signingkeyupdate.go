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
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/antinvestor/matrix/userapi/api"
)

// SigningKeyUpdateConsumer consumes signing key updates that came in over federation.
type SigningKeyUpdateConsumer struct {
	jetstream         nats.JetStreamContext
	durable           string
	topic             string
	userAPI           api.UploadDeviceKeysAPI
	cfg               *config.UserAPI
	isLocalServerName func(spec.ServerName) bool
}

// NewSigningKeyUpdateConsumer creates a new SigningKeyUpdateConsumer. Call Start() to begin consuming from key servers.
func NewSigningKeyUpdateConsumer(
	_ context.Context,
	cfg *config.UserAPI,
	js nats.JetStreamContext,
	userAPI api.UploadDeviceKeysAPI,
) *SigningKeyUpdateConsumer {
	return &SigningKeyUpdateConsumer{
		jetstream:         js,
		durable:           cfg.Matrix.JetStream.Prefixed("KeyServerSigningKeyConsumer"),
		topic:             cfg.Matrix.JetStream.Prefixed(jetstream.InputSigningKeyUpdate),
		userAPI:           userAPI,
		cfg:               cfg,
		isLocalServerName: cfg.Matrix.IsLocalServerName,
	}
}

// Start consuming from key servers
func (t *SigningKeyUpdateConsumer) Start(ctx context.Context) error {
	return jetstream.Consumer(
		ctx, t.jetstream, t.topic, t.durable, 1,
		t.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

// onMessage is called in response to a message received on the
// signing key update events topic from the key server.
func (t *SigningKeyUpdateConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	var updatePayload api.CrossSigningKeyUpdate
	if err := json.Unmarshal(msg.Data, &updatePayload); err != nil {
		logrus.WithError(err).Errorf("Failed to read from signing key update input topic")
		return true
	}
	origin := spec.ServerName(msg.Header.Get("origin"))
	if _, serverName, err := gomatrixserverlib.SplitID('@', updatePayload.UserID); err != nil {
		logrus.WithError(err).Error("failed to split user id")
		return true
	} else if t.isLocalServerName(serverName) {
		logrus.Warn("dropping device key update from ourself")
		return true
	} else if serverName != origin {
		logrus.Warnf("dropping device key update, %s != %s", serverName, origin)
		return true
	}

	keys := fclient.CrossSigningKeys{}
	if updatePayload.MasterKey != nil {
		keys.MasterKey = *updatePayload.MasterKey
	}
	if updatePayload.SelfSigningKey != nil {
		keys.SelfSigningKey = *updatePayload.SelfSigningKey
	}
	uploadReq := &api.PerformUploadDeviceKeysRequest{
		CrossSigningKeys: keys,
		UserID:           updatePayload.UserID,
	}
	uploadRes := &api.PerformUploadDeviceKeysResponse{}
	t.userAPI.PerformUploadDeviceKeys(ctx, uploadReq, uploadRes)
	if uploadRes.Error != nil {
		logrus.WithError(uploadRes.Error).Error("failed to upload device keys")
		return true
	}

	return true
}
