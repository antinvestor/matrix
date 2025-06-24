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
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

// SigningKeyUpdateConsumer consumes signing key updates that came in over federation.
type SigningKeyUpdateConsumer struct {
	qm                queueutil.QueueManager
	userAPI           api.UploadDeviceKeysAPI
	cfg               *config.UserAPI
	isLocalServerName func(spec.ServerName) bool
}

// NewSigningKeyUpdateConsumer creates a new SigningKeyUpdateConsumer. Call Start() to begin consuming from key servers.
func NewSigningKeyUpdateConsumer(
	ctx context.Context,
	cfg *config.UserAPI,
	qm queueutil.QueueManager,
	userAPI api.UploadDeviceKeysAPI,
) error {
	c := &SigningKeyUpdateConsumer{
		qm:                qm,
		userAPI:           userAPI,
		cfg:               cfg,
		isLocalServerName: cfg.Global.IsLocalServerName,
	}

	return qm.RegisterSubscriber(ctx, &cfg.Queues.InputSigningKeyUpdate, c)
}

// Handle is called in response to a message received on the
// signing key update events topic from the key server.
func (t *SigningKeyUpdateConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {
	log := util.Log(ctx)
	var updatePayload api.CrossSigningKeyUpdate
	err := json.Unmarshal(message, &updatePayload)
	if err != nil {
		log.WithError(err).Error("Failed to read from signing key update input topic")
		return nil
	}
	origin := spec.ServerName(metadata["origin"])
	var serverName spec.ServerName
	_, serverName, err = gomatrixserverlib.SplitID('@', updatePayload.UserID)
	if err != nil {
		log.WithError(err).Error("Failed to split user id")
		return nil
	} else if t.isLocalServerName(serverName) {
		log.Warn("Dropping device key update from ourself")
		return nil
	} else if serverName != origin {
		log.WithField("server_name", serverName).WithField("origin", origin).Warn("Dropping device key update due to server name mismatch")
		return nil
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
		log.WithError(uploadRes.Error).Error("Failed to upload device keys")
		return nil
	}

	return nil
}
