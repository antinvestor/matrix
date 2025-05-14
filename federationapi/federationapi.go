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
	"time"

	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/sirupsen/logrus"

	federationAPI "github.com/antinvestor/matrix/federationapi/api"
	"github.com/antinvestor/matrix/federationapi/consumers"
	"github.com/antinvestor/matrix/federationapi/internal"
	"github.com/antinvestor/matrix/federationapi/producers"
	"github.com/antinvestor/matrix/federationapi/queue"
	"github.com/antinvestor/matrix/federationapi/statistics"
	"github.com/antinvestor/matrix/federationapi/storage"
	"github.com/antinvestor/matrix/internal/caching"
	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	userAPI "github.com/antinvestor/matrix/userapi/api"

	"github.com/antinvestor/gomatrixserverlib"

	"github.com/antinvestor/matrix/federationapi/routing"
)

// AddPublicRoutes sets up and registers HTTP handlers on the base API muxes for the FederationAPI component.
func AddPublicRoutes(
	ctx context.Context,
	routers httputil.Routers,
	dendriteConfig *config.Matrix,
	natsInstance *jetstream.NATSInstance,
	userAPI userAPI.FederationUserAPI,
	federation fclient.FederationClient,
	keyRing gomatrixserverlib.JSONVerifier,
	rsAPI roomserverAPI.FederationRoomserverAPI,
	fedAPI federationAPI.FederationInternalAPI,
	enableMetrics bool,
) {
	cfg := &dendriteConfig.FederationAPI
	mscCfg := &dendriteConfig.MSCs
	js, _ := natsInstance.Prepare(ctx, &cfg.Global.JetStream)
	producer := &producers.SyncAPIProducer{
		JetStream:              js,
		TopicReceiptEvent:      cfg.Global.JetStream.Prefixed(jetstream.OutputReceiptEvent),
		TopicSendToDeviceEvent: cfg.Global.JetStream.Prefixed(jetstream.OutputSendToDeviceEvent),
		TopicTypingEvent:       cfg.Global.JetStream.Prefixed(jetstream.OutputTypingEvent),
		TopicPresenceEvent:     cfg.Global.JetStream.Prefixed(jetstream.OutputPresenceEvent),
		TopicDeviceListUpdate:  cfg.Global.JetStream.Prefixed(jetstream.InputDeviceListUpdate),
		TopicSigningKeyUpdate:  cfg.Global.JetStream.Prefixed(jetstream.InputSigningKeyUpdate),
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
		panic("federationapi.AddPublicRoutes called with a FederationInternalAPI impl which was not " +
			"FederationInternalAPI. This is a programming error.")
	}

	routing.Setup(
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
	dendriteCfg *config.Matrix,
	cm sqlutil.ConnectionManager,
	natsInstance *jetstream.NATSInstance,
	federation fclient.FederationClient,
	rsAPI roomserverAPI.FederationRoomserverAPI,
	caches *caching.Caches,
	keyRing *gomatrixserverlib.KeyRing,
	resetBlacklist bool,
) *internal.FederationInternalAPI {
	cfg := &dendriteCfg.FederationAPI

	federationCm, err := cm.FromOptions(ctx, &cfg.Database)
	if err != nil {
		logrus.WithError(err).Panic("failed to obtain federation sender db connection manager")
	}
	federationDB, err := storage.NewDatabase(ctx, federationCm, caches, dendriteCfg.Global.IsLocalServerName)
	if err != nil {
		logrus.WithError(err).Panic("failed to connect to federation sender db")
	}

	if resetBlacklist {
		_ = federationDB.RemoveAllServersFromBlacklist(ctx)
	}

	stats := statistics.NewStatistics(federationDB, cfg.FederationMaxRetries+1, cfg.P2PFederationRetriesUntilAssumedOffline+1, cfg.EnableRelays)

	js, nats := natsInstance.Prepare(ctx, &cfg.Global.JetStream)

	signingInfo := cfg.Global.SigningIdentities()

	queues := queue.NewOutgoingQueues(ctx,
		federationDB,
		cfg.Global.DisableFederation,
		cfg.Global.ServerName, federation, &stats,
		signingInfo,
	)

	rsConsumer := consumers.NewOutputRoomEventConsumer(
		ctx, cfg, js, nats, queues, federationDB, rsAPI,
	)
	if err = rsConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panic("failed to start room server consumer")
	}
	tsConsumer := consumers.NewOutputSendToDeviceConsumer(
		ctx, cfg, js, queues, federationDB,
	)
	if err = tsConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panic("failed to start send-to-device consumer")
	}
	receiptConsumer := consumers.NewOutputReceiptConsumer(
		ctx, cfg, js, queues, federationDB,
	)
	if err = receiptConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panic("failed to start receipt consumer")
	}
	typingConsumer := consumers.NewOutputTypingConsumer(
		ctx, cfg, js, queues, federationDB,
	)
	if err = typingConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panic("failed to start typing consumer")
	}
	keyConsumer := consumers.NewKeyChangeConsumer(
		ctx, &dendriteCfg.KeyServer, js, queues, federationDB, rsAPI,
	)
	if err = keyConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panic("failed to start key server consumer")
	}

	presenceConsumer := consumers.NewOutputPresenceConsumer(
		ctx, cfg, js, queues, federationDB, rsAPI,
	)
	if err = presenceConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panic("failed to start presence consumer")
	}

	var cleanExpiredEDUs func()
	cleanExpiredEDUs = func() {
		logrus.Infof("Cleaning expired EDUs")
		if err := federationDB.DeleteExpiredEDUs(ctx); err != nil {
			logrus.WithError(err).Error("Failed to clean expired EDUs")
		}
		time.AfterFunc(time.Hour, cleanExpiredEDUs)
	}
	time.AfterFunc(time.Minute, cleanExpiredEDUs)

	return internal.NewFederationInternalAPI(federationDB, cfg, rsAPI, federation, &stats, caches, queues, keyRing)
}
