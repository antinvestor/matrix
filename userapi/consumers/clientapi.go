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

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"

	"github.com/antinvestor/matrix/internal/pushgateway"
	"github.com/antinvestor/matrix/userapi/storage"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/antinvestor/matrix/userapi/producers"
	"github.com/antinvestor/matrix/userapi/util"
)

// OutputReceiptEventConsumer consumes events that originated in the clientAPI.
type OutputReceiptEventConsumer struct {
	ctx          context.Context
	jetstream    nats.JetStreamContext
	durable      string
	topic        string
	db           storage.UserDatabase
	serverName   spec.ServerName
	syncProducer *producers.SyncAPI
	pgClient     pushgateway.Client
}

// NewOutputReceiptEventConsumer creates a new OutputReceiptEventConsumer.
// Call Start() to begin consuming from the EDU server.
func NewOutputReceiptEventConsumer(
	ctx context.Context,
	cfg *config.UserAPI,
	js nats.JetStreamContext,
	store storage.UserDatabase,
	syncProducer *producers.SyncAPI,
	pgClient pushgateway.Client,
) *OutputReceiptEventConsumer {
	return &OutputReceiptEventConsumer{
		ctx:          ctx,
		jetstream:    js,
		topic:        cfg.Matrix.JetStream.Prefixed(jetstream.OutputReceiptEvent),
		durable:      cfg.Matrix.JetStream.Durable("UserAPIReceiptConsumer"),
		db:           store,
		serverName:   cfg.Matrix.ServerName,
		syncProducer: syncProducer,
		pgClient:     pgClient,
	}
}

// Start consuming receipts events.
func (s *OutputReceiptEventConsumer) Start(ctx context.Context) error {
	return jetstream.Consumer(
		s.ctx, s.jetstream, s.topic, s.durable, 1,
		s.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

func (s *OutputReceiptEventConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called

	userID := msg.Header.Get(jetstream.UserID)
	roomID := msg.Header.Get(jetstream.RoomID)
	readPos := msg.Header.Get(jetstream.EventID)
	evType := msg.Header.Get("type")

	if readPos == "" || (evType != "m.read" && evType != "m.read.private") {
		return true
	}

	log := log.WithFields(log.Fields{
		"room_id": roomID,
		"user_id": userID,
	})

	localpart, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		log.WithError(err).Error("userapi clientapi consumer: SplitID failure")
		return true
	}
	if domain != s.serverName {
		return true
	}

	metadata, err := msg.Metadata()
	if err != nil {
		return false
	}

	updated, err := s.db.SetNotificationsRead(ctx, localpart, domain, roomID, uint64(spec.AsTimestamp(metadata.Timestamp)), true)
	if err != nil {
		log.WithError(err).Error("userapi EDU consumer")
		return false
	}

	if err = s.syncProducer.GetAndSendNotificationData(ctx, userID, roomID); err != nil {
		log.WithError(err).Error("userapi EDU consumer: GetAndSendNotificationData failed")
		return false
	}

	if !updated {
		return true
	}
	if err = util.NotifyUserCountsAsync(ctx, s.pgClient, localpart, domain, s.db); err != nil {
		log.WithError(err).Error("userapi EDU consumer: NotifyUserCounts failed")
		return false
	}

	return true
}
