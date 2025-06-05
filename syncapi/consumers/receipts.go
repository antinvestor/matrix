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

package consumers

import (
	"context"
	"github.com/antinvestor/matrix/internal/queueutil"
	"strconv"

	log "github.com/sirupsen/logrus"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/setup/config"

	"github.com/antinvestor/matrix/syncapi/notifier"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/streams"
	"github.com/antinvestor/matrix/syncapi/types"
)

// OutputReceiptEventConsumer consumes events that originated in the EDU server.
type OutputReceiptEventConsumer struct {
	qm       queueutil.QueueManager
	db       storage.Database
	stream   streams.StreamProvider
	notifier *notifier.Notifier
}

// NewOutputReceiptEventConsumer creates a new OutputReceiptEventConsumer.
// Call Start() to begin consuming from the EDU server.
func NewOutputReceiptEventConsumer(
	ctx context.Context,
	cfg *config.SyncAPI,
	qm queueutil.QueueManager,
	store storage.Database,
	notifier *notifier.Notifier,
	stream streams.StreamProvider,
) error {
	c := &OutputReceiptEventConsumer{
		qm:       qm,
		db:       store,
		notifier: notifier,
		stream:   stream,
	}

	return qm.RegisterSubscriber(ctx, &cfg.Queues.OutputReceiptEvent, c)
}

func (s *OutputReceiptEventConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {
	output := types.OutputReceiptEvent{
		UserID:  metadata[queueutil.UserID],
		RoomID:  metadata[queueutil.RoomID],
		EventID: metadata[queueutil.EventID],
		Type:    metadata["type"],
	}

	timestamp, err := strconv.ParseUint(metadata["timestamp"], 10, 64)
	if err != nil {
		// If the message was invalid, log it and move on to the next message in the stream
		log.WithError(err).Errorf("output log: message parse failure")
		return err
	}

	output.Timestamp = spec.Timestamp(timestamp)

	streamPos, err := s.db.StoreReceipt(
		ctx,
		output.RoomID,
		output.Type,
		output.UserID,
		output.EventID,
		output.Timestamp,
	)
	if err != nil {
		return err
	}

	s.stream.Advance(streamPos)
	s.notifier.OnNewReceipt(output.RoomID, types.StreamingToken{ReceiptPosition: streamPos})

	return nil
}
