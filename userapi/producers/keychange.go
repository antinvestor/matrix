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

package producers

import (
	"context"

	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame"

	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage"
)

// KeyChange produces key change events for the sync API and federation sender to consume
type KeyChange struct {
	Topic *config.QueueOptions
	Qm    queueutil.QueueManager
	DB    storage.KeyChangeDatabase
}

// ProduceKeyChanges creates new change events for each key
func (p *KeyChange) ProduceKeyChanges(ctx context.Context, keys []api.DeviceMessage) error {
	userToDeviceCount := make(map[string]int)
	for _, key := range keys {
		id, err := p.DB.StoreKeyChange(ctx, key.UserID)
		if err != nil {
			return err
		}
		key.DeviceChangeID = id

		header := map[string]string{
			queueutil.UserID: key.UserID,
		}

		err = p.Qm.Publish(ctx, p.Topic.Ref(), key, header)
		if err != nil {
			return err
		}

		userToDeviceCount[key.UserID]++
	}
	for userID, count := range userToDeviceCount {
		frame.Log(ctx).
			WithField("user_id", userID).
			WithField("num_key_changes", count).
			Debug("Produced to key change topic '%s'", p.Topic.Ref())
	}
	return nil
}

func (p *KeyChange) ProduceSigningKeyUpdate(ctx context.Context, key api.CrossSigningKeyUpdate) error {
	output := &api.DeviceMessage{
		Type: api.TypeCrossSigningUpdate,
		OutputCrossSigningKeyUpdate: &api.OutputCrossSigningKeyUpdate{
			CrossSigningKeyUpdate: key,
		},
	}

	id, err := p.DB.StoreKeyChange(ctx, key.UserID)
	if err != nil {
		return err
	}
	output.DeviceChangeID = id

	header := map[string]string{
		queueutil.UserID: key.UserID,
	}

	err = p.Qm.Publish(ctx, p.Topic.Ref(), output, header)
	if err != nil {
		return err
	}

	frame.Log(ctx).
		WithField("user_id", key.UserID).
		Debug("Produced to cross-signing update topic '%s'", p.Topic)
	return nil
}
