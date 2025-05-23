// Copyright 2019 Alex Chen
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
	"time"

	"github.com/antinvestor/matrix/internal/cacheutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/antinvestor/matrix/syncapi/notifier"
	"github.com/antinvestor/matrix/syncapi/streams"
	"github.com/antinvestor/matrix/syncapi/types"
	log "github.com/sirupsen/logrus"
)

// OutputTypingEventConsumer consumes events that originated in the EDU server.
type OutputTypingEventConsumer struct {
	qm       queueutil.QueueManager
	eduCache *cacheutil.EDUCache
	stream   streams.StreamProvider
	notifier *notifier.Notifier
}

// NewOutputTypingEventConsumer creates a new OutputTypingEventConsumer.
// Call Start() to begin consuming from the EDU server.
func NewOutputTypingEventConsumer(
	ctx context.Context,
	cfg *config.SyncAPI,
	qm queueutil.QueueManager,
	eduCache *cacheutil.EDUCache,
	notifier *notifier.Notifier,
	stream streams.StreamProvider,
) error {
	c := &OutputTypingEventConsumer{
		qm:       qm,
		eduCache: eduCache,
		notifier: notifier,
		stream:   stream,
	}

	return qm.RegisterSubscriber(ctx, &cfg.Queues.OutputTypingEvent, c)
}

func (s *OutputTypingEventConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {
	roomID := metadata[jetstream.RoomID]
	userID := metadata[jetstream.UserID]
	typing, err := strconv.ParseBool(metadata["typing"])
	if err != nil {
		log.WithError(err).Errorf("output log: typing parse failure")
		return nil
	}
	timeout, err := strconv.Atoi(metadata["timeout_ms"])
	if err != nil {
		log.WithError(err).Errorf("output log: timeout_ms parse failure")
		return nil
	}

	log.WithFields(log.Fields{
		"room_id": roomID,
		"user_id": userID,
		"typing":  typing,
		"timeout": timeout,
	}).Debug("syncapi received EDU data from client api")

	var typingPos types.StreamPosition
	if typing {
		expiry := time.Now().Add(time.Duration(timeout) * time.Millisecond)
		typingPos = types.StreamPosition(
			s.eduCache.AddTypingUser(userID, roomID, &expiry),
		)
	} else {
		typingPos = types.StreamPosition(
			s.eduCache.RemoveUser(userID, roomID),
		)
	}

	s.stream.Advance(typingPos)
	s.notifier.OnNewTyping(roomID, types.StreamingToken{TypingPosition: typingPos})

	return nil
}
