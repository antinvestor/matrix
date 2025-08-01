// Copyright 2017 Vector Creations Ltd
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
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/constants"
	"github.com/antinvestor/matrix/syncapi/types"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

// SyncAPIProducer produces events for the sync API server to consume
type SyncAPIProducer struct {
	TopicReceiptEvent      string
	TopicSendToDeviceEvent string
	TopicTypingEvent       string
	TopicPresenceEvent     string
	Qm                     queueutil.QueueManager
	ServerName             spec.ServerName
	UserAPI                userapi.ClientUserAPI
}

func (p *SyncAPIProducer) SendReceipt(
	ctx context.Context,
	userID, roomID, eventID, receiptType string, timestamp spec.Timestamp,
) error {

	log := util.Log(ctx)

	h := map[string]string{
		constants.UserID:  userID,
		constants.RoomID:  roomID,
		constants.EventID: eventID,
		"type":            receiptType,
		"timestamp":       fmt.Sprintf("%d", timestamp),
	}

	log.WithField("receipt_type", receiptType).Debug("Producing to topic '%s'", p.TopicReceiptEvent)
	return p.Qm.Publish(ctx, p.TopicReceiptEvent, "", h)
}

func (p *SyncAPIProducer) SendToDevice(
	ctx context.Context, sender string, userID *spec.UserID, deviceID, eventType string,
	message json.RawMessage,
) error {

	log := util.Log(ctx)

	var devices []string

	domain := userID.Domain()

	// If the event is targeted locally then we want to expand the wildcard
	// out into individual device IDs so that we can send them to each respective
	// device. If the event isn't targeted locally then we can't expand the
	// wildcard as we don't know about the remote devices, so instead we leave it
	// as-is, so that the federation sender can send it on with the wildcard intact.
	if domain == p.ServerName && deviceID == "*" {
		var res userapi.QueryDevicesResponse
		err := p.UserAPI.QueryDevices(ctx, &userapi.QueryDevicesRequest{
			UserID: userID.String(),
		}, &res)
		if err != nil {
			return err
		}
		for _, dev := range res.Devices {
			devices = append(devices, dev.ID)
		}
	} else {
		devices = append(devices, deviceID)
	}

	log.
		WithField("user_id", userID).
		WithField("num_devices", len(devices)).
		WithField("type", eventType).
		Debug("Producing to topic '%s'", p.TopicSendToDeviceEvent)
	for i, device := range devices {
		ote := &types.OutputSendToDeviceEvent{
			UserID:   userID.String(),
			DeviceID: device,
			SendToDeviceEvent: gomatrixserverlib.SendToDeviceEvent{
				Sender:  sender,
				Type:    eventType,
				Content: message,
			},
		}

		h := map[string]string{
			"sender":         sender,
			constants.UserID: constants.EncodeUserID(userID),
		}

		err := p.Qm.Publish(ctx, p.TopicSendToDeviceEvent, ote, h)
		if err != nil {
			if i < len(devices)-1 {
				log.WithError(err).Warn("sendToDevice failed to PublishMsg, trying further devices")
				continue
			}
			log.WithError(err).Error("sendToDevice failed to PublishMsg for all devices")
			return err
		}
	}
	return nil
}

func (p *SyncAPIProducer) SendTyping(
	ctx context.Context, userID, roomID string, typing bool, timeoutMS int64,
) error {

	h := map[string]string{
		constants.UserID: userID,
		constants.RoomID: roomID,
		"typing":         strconv.FormatBool(typing),
		"timeout_ms":     strconv.Itoa(int(timeoutMS)),
	}
	return p.Qm.Publish(ctx, p.TopicTypingEvent, "", h)
}

func (p *SyncAPIProducer) SendPresence(
	ctx context.Context, userID string, presence types.Presence, statusMsg *string,
) error {

	h := map[string]string{
		constants.UserID: userID,
		"presence":       presence.String(),
	}
	if statusMsg != nil {
		h["status_msg"] = *statusMsg
	}

	h["last_active_ts"] = strconv.Itoa(int(spec.AsTimestamp(time.Now())))

	return p.Qm.Publish(ctx, p.TopicPresenceEvent, "", h)
}
