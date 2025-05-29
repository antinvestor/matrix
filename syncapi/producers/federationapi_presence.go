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
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/config"
	"strconv"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/antinvestor/matrix/syncapi/types"
)

// FederationAPIPresenceProducer produces events for the federation API server to consume
type FederationAPIPresenceProducer struct {
	Topic *config.QueueOptions
	Qm    queueutil.QueueManager
}

func (f *FederationAPIPresenceProducer) SendPresence(ctx context.Context,
	userID string, presence types.Presence, statusMsg *string,
) error {

	header := map[string]string{
		jetstream.UserID: userID,
		"presence":       presence.String(),
		"from_sync":      "true", // only update last_active_ts and presence
		"last_active_ts": strconv.Itoa(int(spec.AsTimestamp(time.Now()))),
	}
	if statusMsg != nil {
		header["status_msg"] = *statusMsg
	}

	return f.Qm.Publish(ctx, f.Topic.Ref(), []byte{}, header)
}
