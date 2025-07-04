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

package federationapi

import (
	"context"
	"net/http"
	"time"

	"buf.build/gen/go/antinvestor/presence/connectrpc/go/presencev1connect"
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	federationAPI "github.com/antinvestor/matrix/federationapi/api"
	"github.com/antinvestor/matrix/federationapi/consumers"
	"github.com/antinvestor/matrix/federationapi/internal"
	"github.com/antinvestor/matrix/federationapi/producers"
	"github.com/antinvestor/matrix/federationapi/queue"
	"github.com/antinvestor/matrix/federationapi/routing"
	"github.com/antinvestor/matrix/federationapi/statistics"
	"github.com/antinvestor/matrix/federationapi/storage"
	"github.com/antinvestor/matrix/internal/actorutil"
	"github.com/antinvestor/matrix/internal/cacheutil"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/setup/config"
	userAPI "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

// AddPublicRoutes sets up and registers HTTP handlers on the base API muxes for the FederationAPI component.
func AddPublicRoutes(
	ctx context.Context,
	routers httputil.Routers,
	dendriteConfig *config.Matrix,
	qm queueutil.QueueManager,
	userAPI userAPI.FederationUserAPI,
	federation fclient.FederationClient,
	keyRing gomatrixserverlib.JSONVerifier,
	rsAPI roomserverAPI.FederationRoomserverAPI,
	fedAPI federationAPI.FederationInternalAPI,
	enableMetrics bool,
) {
	cfg := &dendriteConfig.FederationAPI
	cfgUserApi := &dendriteConfig.UserAPI
	mscCfg := &dendriteConfig.MSCs

	_, err := qm.GetOrCreatePublisher(ctx, &cfg.Queues.OutputReceiptEvent)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("failed to register receipt event publisher")
	}
	_, err = qm.GetOrCreatePublisher(ctx, &cfg.Queues.OutputSendToDeviceEvent)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("failed to register send to device event publisher")
	}
	_, err = qm.GetOrCreatePublisher(ctx, &cfg.Queues.OutputTypingEvent)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("failed to register typing event publisher")
	}
	_, err = qm.GetOrCreatePublisher(ctx, &cfg.Queues.OutputPresenceEvent)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("failed to register presence event publisher")
	}
	_, err = qm.GetOrCreatePublisher(ctx, &cfgUserApi.Queues.InputDeviceListUpdate)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("failed to register input device list update event publisher")
	}
	_, err = qm.GetOrCreatePublisher(ctx, &cfgUserApi.Queues.InputSigningKeyUpdate)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("failed to register input signing key event publisher")
	}

	producer := &producers.SyncAPIProducer{
		Qm:                     qm,
		TopicReceiptEvent:      cfg.Queues.OutputReceiptEvent.Ref(),
		TopicSendToDeviceEvent: cfg.Queues.OutputSendToDeviceEvent.Ref(),
		TopicTypingEvent:       cfg.Queues.OutputTypingEvent.Ref(),
		TopicPresenceEvent:     cfg.Queues.OutputPresenceEvent.Ref(),
		TopicDeviceListUpdate:  cfgUserApi.Queues.InputDeviceListUpdate.Ref(),
		TopicSigningKeyUpdate:  cfgUserApi.Queues.InputSigningKeyUpdate.Ref(),
		Config:                 cfg,
		UserAPI:                userAPI,
	}

	// the federationapi component is a bit unique in that it attaches public routes AND serves
	// internal APIs (because it used to be 2 components: the 2nd being fedsender). As a result,
	// the constructor shape is a bit wonky in that it is not valid to AddPublicRoutes without a
	// concrete impl of FederationInternalAPI as the public routes and the internal API _should_
	// be the same thing now.
	f, ok := fedAPI.(*internal.FederationInternalAPI)
	if !ok {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("federationapi.AddPublicRoutes called with a FederationInternalAPI impl which was not " +
			"FederationInternalAPI. This is a programming error.")
	}

	routing.Setup(
		ctx,
		routers,
		dendriteConfig,
		rsAPI, f, keyRing,
		federation, userAPI, mscCfg,
		producer, enableMetrics,
	)
}

// NewInternalAPI returns a concerete implementation of the internal API. Callers
// can call functions directly on the returned API or via an HTTP interface using AddInternalRoutes.
func NewInternalAPI(
	ctx context.Context,
	mcfg *config.Matrix,
	cm sqlutil.ConnectionManager,
	qm queueutil.QueueManager,
	am actorutil.ActorManager,
	federation fclient.FederationClient,
	rsAPI roomserverAPI.FederationRoomserverAPI,
	caches *cacheutil.Caches,
	keyRing *gomatrixserverlib.KeyRing,
	resetBlacklist bool,
	presenceCli presencev1connect.PresenceServiceClient,
) *internal.FederationInternalAPI {
	cfg := &mcfg.FederationAPI

	federationCm, err := cm.FromOptions(ctx, &cfg.Database)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("failed to obtain federation sender db connection manager")
	}
	federationDB, err := storage.NewDatabase(ctx, federationCm, caches, cfg.Global.IsLocalServerName)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("failed to connect to federation sender db")
	}

	if presenceCli == nil {
		presenceCli = presencev1connect.NewPresenceServiceClient(
			http.DefaultClient, cfg.Global.SyncAPIPresenceURI,
		)
	}

	if resetBlacklist {
		_ = federationDB.RemoveAllServersFromBlacklist(ctx)
	}

	stats := statistics.NewStatistics(federationDB, cfg.FederationMaxRetries+1, cfg.P2PFederationRetriesUntilAssumedOffline+1, cfg.EnableRelays)

	signingInfo := cfg.Global.SigningIdentities()

	queues := queue.NewOutgoingQueues(ctx,
		federationDB,
		cfg.Global.DisableFederation,
		cfg.Global.ServerName, federation, &stats,
		signingInfo,
	)

	err = consumers.NewOutputRoomEventConsumer(
		ctx, cfg, qm, am, queues, federationDB, rsAPI, presenceCli,
	)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("failed to start room server consumer")
	}

	err = consumers.NewOutputSendToDeviceConsumer(
		ctx, cfg, qm, queues, federationDB,
	)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("failed to start send-to-device consumer")
	}
	err = consumers.NewOutputReceiptConsumer(
		ctx, cfg, qm, queues, federationDB,
	)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("failed to start receipt consumer")
	}
	err = consumers.NewOutputTypingConsumer(
		ctx, cfg, qm, queues, federationDB,
	)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("failed to start typing consumer")
	}
	err = consumers.NewKeyChangeConsumer(
		ctx, &mcfg.KeyServer, qm, queues, federationDB, rsAPI,
	)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("failed to start key server consumer")
	}

	err = consumers.NewOutputPresenceConsumer(
		ctx, cfg, qm, queues, federationDB, rsAPI,
	)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("component", "federationapi").Panic("failed to start presence consumer")
	}

	var cleanExpiredEDUs func()
	cleanExpiredEDUs = func() {

		select {
		case <-ctx.Done():
			return
		default:

			util.Log(ctx).
				WithField("component", "federationapi").
				Info("Cleaning expired EDUs")

			if err = federationDB.DeleteExpiredEDUs(ctx); err != nil {
				util.Log(ctx).WithError(err).
					WithField("component", "federationapi").
					Error("Failed to clean expired EDUs")
			}
			time.AfterFunc(time.Hour, cleanExpiredEDUs)
		}
	}
	time.AfterFunc(time.Minute, cleanExpiredEDUs)

	return internal.NewFederationInternalAPI(ctx, federationDB, cfg, rsAPI, federation, &stats, caches, queues, keyRing)
}
