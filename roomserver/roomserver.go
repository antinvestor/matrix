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
	"context"

	"github.com/antinvestor/matrix/internal/actorutil"
	"github.com/antinvestor/matrix/internal/cacheutil"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/internal"
	"github.com/antinvestor/matrix/roomserver/storage"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
)

// NewInternalAPI returns a concrete implementation of the internal API.
//
// Many of the methods provided by this API depend on access to a federation API, and so
// you may wish to call `SetFederationAPI` on the returned struct to avoid nil-dereference errors.
func NewInternalAPI(
	ctx context.Context,
	cfg *config.Matrix,
	cm sqlutil.ConnectionManager,
	qm queueutil.QueueManager,
	caches cacheutil.RoomServerCaches,
	am actorutil.ActorManager,
	enableMetrics bool,
) api.RoomserverInternalAPI {

	roomserverCm, err := cm.FromOptions(ctx, &cfg.RoomServer.Database)
	if err != nil {
		util.Log(ctx).WithError(err).Panic("could not obtain connection manager for roomserver")
	}

	roomserverDB, err := storage.NewDatabase(ctx, roomserverCm, caches)
	if err != nil {
		util.Log(ctx).WithError(err).Panic("failed to connect to room server db")
	}

	return internal.NewRoomserverAPI(ctx, cfg, roomserverDB, qm, caches, am, enableMetrics)
}
