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
	"strconv"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/antinvestor/matrix/syncapi/notifier"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/streams"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// PresenceConsumer consumes presence events that originated in the EDU server.
type PresenceConsumer struct {
	jetstream     nats.JetStreamContext
	nats          *nats.Conn
	durable       string
	requestTopic  string
	presenceTopic string
	db            storage.Database
	stream        streams.StreamProvider
	notifier      *notifier.Notifier
	deviceAPI     api.SyncUserAPI
	cfg           *config.SyncAPI
}

// NewPresenceConsumer creates a new PresenceConsumer.
// Call Start() to begin consuming events.
func NewPresenceConsumer(
	_ context.Context,
	cfg *config.SyncAPI,
	js nats.JetStreamContext,
	nats *nats.Conn,
	db storage.Database,
	notifier *notifier.Notifier,
	stream streams.StreamProvider,
	deviceAPI api.SyncUserAPI,
) *PresenceConsumer {
	return &PresenceConsumer{
		nats:          nats,
		jetstream:     js,
		durable:       cfg.Matrix.JetStream.Durable("SyncAPIPresenceConsumer"),
		presenceTopic: cfg.Matrix.JetStream.Prefixed(jetstream.OutputPresenceEvent),
		requestTopic:  cfg.Matrix.JetStream.Prefixed(jetstream.RequestPresence),
		db:            db,
		notifier:      notifier,
		stream:        stream,
		deviceAPI:     deviceAPI,
		cfg:           cfg,
	}
}

// Start consuming typing events.
func (s *PresenceConsumer) Start(ctx context.Context) error {
	// Normal NATS subscription, used by Request/Reply
	_, err := s.nats.Subscribe(s.requestTopic, func(msg *nats.Msg) {
		userID := msg.Header.Get(jetstream.UserID)
		presences, err := s.db.GetPresences(ctx, []string{userID})
		m := &nats.Msg{
			Header: nats.Header{},
		}
		if err != nil {
			m.Header.Set("error", err.Error())
			if err = msg.RespondMsg(m); err != nil {
				logrus.WithError(err).Error("Unable to respond to messages")
			}
			return
		}

		presence := &types.PresenceInternal{
			UserID: userID,
		}
		if len(presences) > 0 {
			presence = presences[0]
		}

		deviceRes := api.QueryDevicesResponse{}
		if err = s.deviceAPI.QueryDevices(ctx, &api.QueryDevicesRequest{UserID: userID}, &deviceRes); err != nil {
			m.Header.Set("error", err.Error())
			if err = msg.RespondMsg(m); err != nil {
				logrus.WithError(err).Error("Unable to respond to messages")
			}
			return
		}

		for i := range deviceRes.Devices {
			if int64(presence.LastActiveTS) < deviceRes.Devices[i].LastSeenTS {
				presence.LastActiveTS = spec.Timestamp(deviceRes.Devices[i].LastSeenTS)
			}
		}

		m.Header.Set(jetstream.UserID, presence.UserID)
		m.Header.Set("presence", presence.ClientFields.Presence)
		if presence.ClientFields.StatusMsg != nil {
			m.Header.Set("status_msg", *presence.ClientFields.StatusMsg)
		}
		m.Header.Set("last_active_ts", strconv.Itoa(int(presence.LastActiveTS)))

		if err = msg.RespondMsg(m); err != nil {
			logrus.WithError(err).Error("Unable to respond to messages")
			return
		}
	})
	if err != nil {
		return err
	}
	if !s.cfg.Matrix.Presence.EnableInbound && !s.cfg.Matrix.Presence.EnableOutbound {
		return nil
	}
	return jetstream.Consumer(
		ctx, s.jetstream, s.presenceTopic, s.durable, 1, s.onMessage,
		nats.DeliverAll(), nats.ManualAck(), nats.HeadersOnly(),
	)
}

func (s *PresenceConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	userID := msg.Header.Get(jetstream.UserID)
	presence := msg.Header.Get("presence")
	timestamp := msg.Header.Get("last_active_ts")
	fromSync, _ := strconv.ParseBool(msg.Header.Get("from_sync"))
	logrus.Tracef("syncAPI received presence event: %+v", msg.Header)

	if fromSync { // do not process local presence changes; we already did this synchronously.
		return true
	}

	ts, err := strconv.ParseUint(timestamp, 10, 64)
	if err != nil {
		return true
	}

	var statusMsg *string = nil
	if data, ok := msg.Header["status_msg"]; ok && len(data) > 0 {
		newMsg := msg.Header.Get("status_msg")
		statusMsg = &newMsg
	}
	// already checked, so no need to check error
	p, _ := types.PresenceFromString(presence)

	s.EmitPresence(ctx, userID, p, statusMsg, spec.Timestamp(ts), fromSync)
	return true
}

func (s *PresenceConsumer) EmitPresence(ctx context.Context, userID string, presence types.Presence, statusMsg *string, ts spec.Timestamp, fromSync bool) {
	pos, err := s.db.UpdatePresence(ctx, userID, presence, statusMsg, ts, fromSync)
	if err != nil {
		logrus.WithError(err).WithField("user", userID).WithField("presence", presence).Warn("failed to updated presence for user")
		return
	}
	s.stream.Advance(pos)
	s.notifier.OnNewPresence(types.StreamingToken{PresencePosition: pos}, userID)
}
