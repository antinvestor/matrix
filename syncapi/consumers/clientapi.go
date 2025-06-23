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

package consumers

import (
	"context"
	"encoding/json"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/eventutil"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/constants"
	"github.com/antinvestor/matrix/syncapi/notifier"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/streams"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/pitabwire/util"
)

// OutputClientDataConsumer consumes events that originated in the client API server.
type OutputClientDataConsumer struct {
	qm           queueutil.QueueManager
	topicReIndex *config.QueueOptions
	db           storage.Database
	stream       streams.StreamProvider
	notifier     *notifier.Notifier
	serverName   spec.ServerName
	cfg          *config.SyncAPI
}

// NewOutputClientDataConsumer creates a new OutputClientData consumer. Call Start() to begin consuming from room servers.
func NewOutputClientDataConsumer(
	ctx context.Context,
	cfg *config.SyncAPI,
	qm queueutil.QueueManager,
	store storage.Database,
	notifier *notifier.Notifier,
	stream streams.StreamProvider,
) error {

	c := &OutputClientDataConsumer{
		qm:           qm,
		topicReIndex: &cfg.Queues.InputFulltextReindex,
		db:           store,
		notifier:     notifier,
		stream:       stream,
		serverName:   cfg.Global.ServerName,
		cfg:          cfg,
	}

	return qm.RegisterSubscriber(ctx, &cfg.Queues.OutputClientData, c)
}

// Handle is called when the sync server receives a new event from the client API server output log.
// It is not safe for this function to be called from multiple goroutines, or else the
// sync stream position may race and be incorrectly calculated.
func (s *OutputClientDataConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {

	log := util.Log(ctx)
	// Parse out the event JSON
	userID := metadata[constants.UserID]
	var output eventutil.AccountData
	if err := json.Unmarshal(message, &output); err != nil {
		// If the message was invalid, log it and move on to the next message in the stream
		log.WithError(err).Error("client API server output log: message parse failure")
		return nil
	}

	log.WithField("type", output.Type).
		WithField("room_id", output.RoomID).
		Debug("Received data from client API server")

	streamPos, err := s.db.UpsertAccountData(
		ctx, userID, output.RoomID, output.Type,
	)
	if err != nil {
		log.WithError(err).
			WithField("type", output.Type).
			WithField("room_id", output.RoomID).Error("could not save account data")
		return err
	}

	if output.IgnoredUsers != nil {
		err = s.db.UpdateIgnoresForUser(ctx, userID, output.IgnoredUsers)
		if err != nil {
			log.WithError(err).
				WithField("user_id", userID).Error("Failed to update ignored users")

		}
	}

	s.stream.Advance(streamPos)
	s.notifier.OnNewAccountData(userID, types.StreamingToken{AccountDataPosition: streamPos})

	return nil
}
