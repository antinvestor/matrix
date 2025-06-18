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

package producers

import (
	"context"

	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/roomserver/acls"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
	"github.com/tidwall/gjson"
)

var keyContentFields = map[string]string{
	"m.room.join_rules":         "join_rule",
	"m.room.history_visibility": "history_visibility",
	"m.room.member":             "membership",
}

type RoomEventProducer struct {
	Topic *config.QueueOptions
	ACLs  *acls.ServerACLs
	Qm    queueutil.QueueManager
}

func (r *RoomEventProducer) ProduceRoomEvents(ctx context.Context, roomID string, updates []api.OutputEvent) error {
	var err error

	log := util.Log(ctx)

	for _, update := range updates {

		logger := log.WithField("room_id", roomID).
			WithField("type", update.Type)
		if update.NewRoomEvent != nil {
			eventType := update.NewRoomEvent.Event.Type()
			logger = logger.
				WithField("event_type", eventType).
				WithField("event_id", update.NewRoomEvent.Event.EventID()).
				WithField("adds_state", len(update.NewRoomEvent.AddsStateEventIDs)).
				WithField("removes_state", len(update.NewRoomEvent.RemovesStateEventIDs)).
				WithField("send_as_server", update.NewRoomEvent.SendAsServer).
				WithField("sender", update.NewRoomEvent.Event.SenderID())
			if update.NewRoomEvent.Event.StateKey() != nil {
				logger = logger.WithField("state_key", *update.NewRoomEvent.Event.StateKey())
			}
			contentKey := keyContentFields[eventType]
			if contentKey != "" {
				value := gjson.GetBytes(update.NewRoomEvent.Event.Content(), contentKey)
				if value.Exists() {
					logger = logger.WithField("content_value", value.String())
				}
			}

			if eventType == acls.MRoomServerACL && update.NewRoomEvent.Event.StateKeyEquals("") {
				ev := update.NewRoomEvent.Event.PDU
				strippedEvent := tables.StrippedEvent{
					RoomID:       ev.RoomID().String(),
					EventType:    ev.Type(),
					StateKey:     *ev.StateKey(),
					ContentValue: string(ev.Content()),
				}
				defer r.ACLs.OnServerACLUpdate(ctx, strippedEvent)
			}
		}

		h := map[string]string{
			queueutil.RoomEventType: string(update.Type),
			queueutil.RoomID:        roomID,
		}

		logger.WithField("topic", r.Topic.DSrc()).Info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")

		err = r.Qm.Publish(ctx, r.Topic.Ref(), update, h)
		if err != nil {
			logger.WithError(err).WithField("topic", r.Topic).Error("Failed to produce to topic ")
			return err
		}
	}
	return nil
}
