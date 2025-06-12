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
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/pitabwire/util"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/config"

	"github.com/antinvestor/matrix/syncapi/types"
	userapi "github.com/antinvestor/matrix/userapi/api"
)

// SyncAPIProducer produces events for the sync API server to consume
type SyncAPIProducer struct {
	TopicReceiptEvent      string
	TopicSendToDeviceEvent string
	TopicTypingEvent       string
	TopicPresenceEvent     string
	TopicDeviceListUpdate  string
	TopicSigningKeyUpdate  string
	Qm                     queueutil.QueueManager
	Config                 *config.FederationAPI
	UserAPI                userapi.FederationUserAPI
}

func (p *SyncAPIProducer) SendReceipt(
	ctx context.Context,
	userID, roomID, eventID, receiptType string, timestamp spec.Timestamp,
) error {
	h := map[string]string{
		queueutil.UserID:  userID,
		queueutil.RoomID:  roomID,
		queueutil.EventID: eventID,
		"type":            receiptType,
		"timestamp":       fmt.Sprintf("%d", timestamp),
	}

	util.Log(ctx).
		WithField("component", "syncapi_producer").
		WithField("topic", p.TopicReceiptEvent).
		Debug("Producing to topic")
	return p.Qm.Publish(ctx, p.TopicReceiptEvent, "", h)
}

func (p *SyncAPIProducer) SendToDevice(
	ctx context.Context, sender, userID, deviceID, eventType string,
	message json.RawMessage,
) error {
	var devices []string
	_, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		return err
	}

	// If the event is targeted locally then we want to expand the wildcard
	// out into individual device IDs so that we can send them to each respective
	// device. If the event isn't targeted locally then we can't expand the
	// wildcard as we don't know about the remote devices, so instead we leave it
	// as-is, so that the federation sender can send it on with the wildcard intact.
	if p.Config.Global.IsLocalServerName(domain) && deviceID == "*" {
		var res userapi.QueryDevicesResponse
		err = p.UserAPI.QueryDevices(ctx, &userapi.QueryDevicesRequest{
			UserID: userID,
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

	util.Log(ctx).
		WithField("user_id", userID).
		WithField("num_devices", len(devices)).
		WithField("type", eventType).
		Debug("Producing to topic")
	for i, device := range devices {
		ote := &types.OutputSendToDeviceEvent{
			UserID:   userID,
			DeviceID: device,
			SendToDeviceEvent: gomatrixserverlib.SendToDeviceEvent{
				Sender:  sender,
				Type:    eventType,
				Content: message,
			},
		}

		h := map[string]string{
			"sender":         sender,
			queueutil.UserID: userID,
		}

		err = p.Qm.Publish(ctx, p.TopicSendToDeviceEvent, ote, h)
		if err != nil {
			if i < len(devices)-1 {
				util.Log(ctx).WithError(err).
					WithField("component", "syncapi_producer").
					Warn("sendToDevice failed to PublishMsg, trying further devices")
				continue
			}
			util.Log(ctx).WithError(err).
				WithField("component", "syncapi_producer").
				Error("sendToDevice failed to PublishMsg for all devices")
			return err
		}
	}
	return nil
}

func (p *SyncAPIProducer) SendTyping(
	ctx context.Context, userID, roomID string, typing bool, timeoutMS int64,
) error {

	h := map[string]string{
		queueutil.UserID: userID,
		queueutil.RoomID: roomID,
		"typing":         strconv.FormatBool(typing),
		"timeout_ms":     strconv.Itoa(int(timeoutMS)),
	}
	util.Log(ctx).
		WithField(
			"component", "syncapi_producer").
		WithField("topic", p.TopicTypingEvent).
		Debug("Producing to topic")
	return p.Qm.Publish(ctx, p.TopicTypingEvent, "", h)
}

func (p *SyncAPIProducer) SendPresence(
	ctx context.Context, userID string, presence types.Presence, statusMsg *string, lastActiveAgo int64,
) error {
	h := map[string]string{
		queueutil.UserID: userID,
		"presence":       presence.String(),
	}
	if statusMsg != nil {
		h["status_msg"] = *statusMsg
	}
	lastActiveTS := spec.AsTimestamp(time.Now().Add(-(time.Duration(lastActiveAgo) * time.Millisecond)))

	h["last_active_ts"] = strconv.Itoa(int(lastActiveTS))
	util.Log(ctx).WithField(
		"component", "syncapi_producer").
		WithField("presence", h).
		Debug("Sending presence to syncAPI")
	return p.Qm.Publish(ctx, p.TopicPresenceEvent, "", h)
}

func (p *SyncAPIProducer) SendDeviceListUpdate(
	ctx context.Context, deviceListUpdate json.RawMessage, origin spec.ServerName,
) (err error) {
	h := map[string]string{
		"origin": string(origin),
	}

	util.Log(ctx).
		WithField("component", "syncapi_producer").
		WithField("device_list_update", h).
		Debug("Sending device list update")
	return p.Qm.Publish(ctx, p.TopicPresenceEvent, []byte(deviceListUpdate), h)
}

func (p *SyncAPIProducer) SendSigningKeyUpdate(
	ctx context.Context, data json.RawMessage, origin spec.ServerName,
) (err error) {
	h := map[string]string{
		"origin": string(origin),
	}

	util.Log(ctx).WithField(
		"component", "syncapi_producer").
		Debug("Sending signing key update")
	return p.Qm.Publish(ctx, p.TopicSigningKeyUpdate, data, h)
}
