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

package mediaapi

import (
	"context"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/mediaapi/routing"
	"github.com/antinvestor/matrix/mediaapi/storage"
	"github.com/antinvestor/matrix/setup/config"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

// AddPublicRoutes sets up and registers HTTP handlers for the MediaAPI component.
func AddPublicRoutes(
	ctx context.Context,
	routers httputil.Routers,
	cm sqlutil.ConnectionManager,
	cfg *config.Matrix,
	userAPI userapi.MediaUserAPI,
	client *fclient.Client,
	fedClient fclient.FederationClient,
	keyRing gomatrixserverlib.JSONVerifier,
) {
	mediaCm, err := cm.FromOptions(ctx, &cfg.MediaAPI.Database)
	if err != nil {
		util.Log(ctx).WithError(err).Panic("failed to obtain a media db connection manager :%v", err)
	}

	mediaDB, err := storage.NewMediaAPIDatasource(ctx, mediaCm)
	if err != nil {
		util.Log(ctx).WithError(err).Panic("failed to connect to media db")
	}

	routing.Setup(
		routers, cfg, mediaDB, userAPI, client, fedClient, keyRing,
	)
}
