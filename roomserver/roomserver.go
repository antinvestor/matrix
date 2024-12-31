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

package roomserver

import (
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/antinvestor/matrix/setup/process"
	"github.com/sirupsen/logrus"

	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/internal"
	"github.com/antinvestor/matrix/roomserver/storage"
)

// NewInternalAPI returns a concrete implementation of the internal API.
//
// Many of the methods provided by this API depend on access to a federation API, and so
// you may wish to call `SetFederationAPI` on the returned struct to avoid nil-dereference errors.
func NewInternalAPI(
	processContext *process.ProcessContext,
	cfg *config.Dendrite,
	cm *sqlutil.Connections,
	natsInstance *jetstream.NATSInstance,
	caches caching.RoomServerCaches,
	enableMetrics bool,
) api.RoomserverInternalAPI {
	roomserverDB, err := storage.Open(processContext.Context(), cm, &cfg.RoomServer.Database, caches)
	if err != nil {
		logrus.With(slog.Any("error", err)).Panicf("failed to connect to room server db")
	}

	js, nc := natsInstance.Prepare(processContext, &cfg.Global.JetStream)

	return internal.NewRoomserverAPI(
		processContext, cfg, roomserverDB, js, nc, caches, enableMetrics,
	)
}
