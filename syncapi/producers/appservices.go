// Copyright 2023 The Global.org Foundation C.I.C.
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
)

// AppserviceEventProducer produces events for the appservice API to consume
type AppserviceEventProducer struct {
	Topic *config.QueueOptions
	Qm    queueutil.QueueManager
}

func (a *AppserviceEventProducer) ProduceRoomEvents(ctx context.Context, message any, header map[string]string) error {
	return a.Qm.Publish(ctx, a.Topic.Ref(), message, header)
}
