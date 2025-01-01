// Copyright 2022 The Matrix.org Foundation C.I.C.
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

package relayapi

import (
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/matrix/federationapi/producers"
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/relayapi/api"
	"github.com/antinvestor/matrix/relayapi/internal"
	"github.com/antinvestor/matrix/relayapi/routing"
	"github.com/antinvestor/matrix/relayapi/storage"
	rsAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/sirupsen/logrus"
)

// AddPublicRoutes sets up and registers HTTP handlers on the base API muxes for the FederationAPI component.
func AddPublicRoutes(
	routers httputil.Routers,
	dendriteCfg *config.Dendrite,
	keyRing gomatrixserverlib.JSONVerifier,
	relayAPI api.RelayInternalAPI,
) {
	relay, ok := relayAPI.(*internal.RelayInternalAPI)
	if !ok {
		panic("relayapi.AddPublicRoutes called with a RelayInternalAPI impl which was not " +
			"RelayInternalAPI. This is a programming error.")
	}

	routing.Setup(
		routers.Federation,
		&dendriteCfg.FederationAPI,
		relay,
		keyRing,
	)
}

func NewRelayInternalAPI(
	dendriteCfg *config.Dendrite,
	cm *sqlutil.Connections,
	fedClient fclient.FederationClient,
	rsAPI rsAPI.RoomserverInternalAPI,
	keyRing *gomatrixserverlib.KeyRing,
	producer *producers.SyncAPIProducer,
	relayingEnabled bool,
	caches caching.FederationCache,
) api.RelayInternalAPI {
	relayDB, err := storage.NewDatabase(cm, &dendriteCfg.RelayAPI.Database, caches, dendriteCfg.Global.IsLocalServerName)
	if err != nil {
		logrus.WithError(err).Panic("failed to connect to relay db")
	}

	return internal.NewRelayInternalAPI(
		relayDB,
		fedClient,
		rsAPI,
		keyRing,
		producer,
		dendriteCfg.Global.Presence.EnableInbound,
		dendriteCfg.Global.ServerName,
		relayingEnabled,
	)
}
