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
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/antinvestor/matrix/syncapi/notifier"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/streams"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/getsentry/sentry-go"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

// OutputClientDataConsumer consumes events that originated in the client API server.
type OutputClientDataConsumer struct {
	jetstream    nats.JetStreamContext
	nats         *nats.Conn
	durable      string
	topic        string
	topicReIndex string
	db           storage.Database
	stream       streams.StreamProvider
	notifier     *notifier.Notifier
	serverName   spec.ServerName
	cfg          *config.SyncAPI
}

// NewOutputClientDataConsumer creates a new OutputClientData consumer. Call Start() to begin consuming from room servers.
func NewOutputClientDataConsumer(
	_ context.Context,
	cfg *config.SyncAPI,
	js nats.JetStreamContext,
	nats *nats.Conn,
	store storage.Database,
	notifier *notifier.Notifier,
	stream streams.StreamProvider,
) *OutputClientDataConsumer {
	return &OutputClientDataConsumer{
		jetstream:    js,
		topic:        cfg.Matrix.JetStream.Prefixed(jetstream.OutputClientData),
		topicReIndex: cfg.Matrix.JetStream.Prefixed(jetstream.InputFulltextReindex),
		durable:      cfg.Matrix.JetStream.Durable("SyncAPIAccountDataConsumer"),
		nats:         nats,
		db:           store,
		notifier:     notifier,
		stream:       stream,
		serverName:   cfg.Matrix.ServerName,
		cfg:          cfg,
	}
}

// Start consuming from room servers
func (s *OutputClientDataConsumer) Start(ctx context.Context) error {
	return jetstream.Consumer(
		ctx, s.jetstream, s.topic, s.durable, 1,
		s.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

// onMessage is called when the sync server receives a new event from the client API server output log.
// It is not safe for this function to be called from multiple goroutines, or else the
// sync stream position may race and be incorrectly calculated.
func (s *OutputClientDataConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	// Parse out the event JSON
	userID := msg.Header.Get(jetstream.UserID)
	var output eventutil.AccountData
	if err := json.Unmarshal(msg.Data, &output); err != nil {
		// If the message was invalid, log it and move on to the next message in the stream
		log.WithError(err).Errorf("client API server output log: message parse failure")
		sentry.CaptureException(err)
		return true
	}

	log.WithFields(log.Fields{
		"type":    output.Type,
		"room_id": output.RoomID,
	}).Debug("Received data from client API server")

	streamPos, err := s.db.UpsertAccountData(
		ctx, userID, output.RoomID, output.Type,
	)
	if err != nil {
		sentry.CaptureException(err)
		log.WithFields(log.Fields{
			"type":       output.Type,
			"room_id":    output.RoomID,
			log.ErrorKey: err,
		}).Errorf("could not save account data")
		return false
	}

	if output.IgnoredUsers != nil {
		if err := s.db.UpdateIgnoresForUser(ctx, userID, output.IgnoredUsers); err != nil {
			log.WithError(err).WithFields(log.Fields{
				"user_id": userID,
			}).Errorf("Failed to update ignored users")
			sentry.CaptureException(err)
		}
	}

	s.stream.Advance(streamPos)
	s.notifier.OnNewAccountData(userID, types.StreamingToken{AccountDataPosition: streamPos})

	return true
}
