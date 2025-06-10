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
	"time"

	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/pitabwire/frame"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/pushgateway"
	"github.com/antinvestor/matrix/userapi/storage"

	"github.com/antinvestor/matrix/setup/config"

	"github.com/antinvestor/matrix/userapi/producers"
	"github.com/antinvestor/matrix/userapi/util"
)

// OutputReceiptEventConsumer consumes events that originated in the clientAPI.
type OutputReceiptEventConsumer struct {
	ctx          context.Context
	qm           queueutil.QueueManager
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
	qm queueutil.QueueManager,
	store storage.UserDatabase,
	syncProducer *producers.SyncAPI,
	pgClient pushgateway.Client,
) error {
	c := &OutputReceiptEventConsumer{
		ctx:          ctx,
		qm:           qm,
		db:           store,
		serverName:   cfg.Global.ServerName,
		syncProducer: syncProducer,
		pgClient:     pgClient,
	}

	return qm.RegisterSubscriber(ctx, &cfg.Queues.OutputReceiptEvent, c)
}

func (s *OutputReceiptEventConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {

	userID := metadata[queueutil.UserID]
	roomID := metadata[queueutil.RoomID]
	readPos := metadata[queueutil.EventID]
	evType := metadata["type"]

	if readPos == "" || (evType != "m.read" && evType != "m.read.private") {
		return nil
	}

	log := frame.Log(ctx).
		WithField("room_id", roomID).
		WithField("user_id", userID)

	localpart, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		log.WithError(err).Error("userapi clientapi consumer: SplitID failure")
		return nil
	}
	if domain != s.serverName {
		return nil
	}

	//TODO: previously this was extracted from message metadata, figure a way too pass in a stable position
	timeNow := time.Now()

	updated, err := s.db.SetNotificationsRead(ctx, localpart, domain, roomID, uint64(spec.AsTimestamp(timeNow)), true)
	if err != nil {
		log.WithError(err).Error("userapi EDU consumer")
		return err
	}

	if err = s.syncProducer.GetAndSendNotificationData(ctx, userID, roomID); err != nil {
		log.WithError(err).Error("userapi EDU consumer: GetAndSendNotificationData failed")
		return err
	}

	if !updated {
		return nil
	}
	if err = util.NotifyUserCountsAsync(ctx, s.pgClient, localpart, domain, s.db); err != nil {
		log.WithError(err).Error("userapi EDU consumer: NotifyUserCounts failed")
		return err
	}

	return nil
}
