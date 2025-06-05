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
	"github.com/antinvestor/matrix/internal/queueutil"
	"strconv"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/setup/config"

	"github.com/antinvestor/matrix/syncapi/notifier"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/streams"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/sirupsen/logrus"
)

// PresenceConsumer consumes presence events that originated in the EDU server.
type PresenceConsumer struct {
	qm        queueutil.QueueManager
	db        storage.Database
	notifier  *notifier.Notifier
	stream    streams.StreamProvider
	cfg       *config.SyncAPI
	deviceAPI api.SyncUserAPI
}

// NewPresenceConsumer creates a new PresenceConsumer.
// Call Start() to begin consuming events.
func NewPresenceConsumer(
	ctx context.Context,
	cfg *config.SyncAPI,
	qm queueutil.QueueManager,
	db storage.Database,
	notifier *notifier.Notifier,
	stream streams.StreamProvider,
	deviceAPI api.SyncUserAPI,
) (*PresenceConsumer, error) {
	consumer := &PresenceConsumer{
		qm:        qm,
		db:        db,
		deviceAPI: deviceAPI,
		notifier:  notifier,
		cfg:       cfg,
		stream:    stream,
	}

	if err := consumer.register(ctx); err != nil {
		logrus.WithError(err).Error("Failed to register presence consumer")
		return nil, err
	}

	return consumer, nil
}

// Start consuming typing events.
func (s *PresenceConsumer) register(ctx context.Context) error {

	if !s.cfg.Global.Presence.EnableInbound && !s.cfg.Global.Presence.EnableOutbound {
		return nil
	}
	return s.qm.RegisterSubscriber(ctx, &s.cfg.Queues.OutputPresenceEvent, s)
}

func (s *PresenceConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {
	userID := metadata[queueutil.UserID]
	presence := metadata["presence"]
	timestamp := metadata["last_active_ts"]
	fromSync, _ := strconv.ParseBool(metadata["from_sync"])
	logrus.Tracef("syncAPI received presence event: %+v", metadata)

	if fromSync { // do not process local presence changes; we already did this synchronously.
		return nil
	}

	ts, err := strconv.ParseUint(timestamp, 10, 64)
	if err != nil {
		return nil
	}

	var statusMsg *string = nil
	if data, ok := metadata["status_msg"]; ok && len(data) > 0 {
		newMsg := metadata["status_msg"]
		statusMsg = &newMsg
	}
	// already checked, so no need to check error
	p, _ := types.PresenceFromString(presence)

	s.EmitPresence(ctx, userID, p, statusMsg, spec.Timestamp(ts), fromSync)
	return nil
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
