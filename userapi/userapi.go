// Copyright 2020 The Matrix.org Foundation C.I.C.
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

package userapi

import (
	"context"
	"time"

	profilev1 "github.com/antinvestor/apis/go/profile/v1"

	"github.com/antinvestor/gomatrixserverlib/spec"
	fedsenderapi "github.com/antinvestor/matrix/federationapi/api"
	"github.com/antinvestor/matrix/federationapi/statistics"
	"github.com/antinvestor/matrix/internal/pushgateway"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/consumers"
	"github.com/antinvestor/matrix/userapi/internal"
	"github.com/antinvestor/matrix/userapi/producers"
	"github.com/antinvestor/matrix/userapi/storage"
	"github.com/antinvestor/matrix/userapi/util"
	"github.com/sirupsen/logrus"

	rsapi "github.com/antinvestor/matrix/roomserver/api"
)

// NewInternalAPI returns a concrete implementation of the internal API. Callers
// can call functions directly on the returned API or via an HTTP interface using AddInternalRoutes.
//
// Creating a new instance of the user API requires a roomserver API with a federation API set
// using its `SetFederationAPI` method, other you may get nil-dereference errors.
func NewInternalAPI(
	ctx context.Context,
	dendriteCfg *config.Dendrite,
	cm *sqlutil.Connections,
	natsInstance *jetstream.NATSInstance,
	rsAPI rsapi.UserRoomserverAPI,
	fedClient fedsenderapi.KeyserverFederationAPI,
	profileCli *profilev1.ProfileClient,
	enableMetrics bool,
	blacklistedOrBackingOffFn func(ctx context.Context, s spec.ServerName) (*statistics.ServerStatistics, error),
) *internal.UserInternalAPI {

	var err error
	js, _ := natsInstance.Prepare(ctx, &dendriteCfg.Global.JetStream)
	appServices := dendriteCfg.Derived.ApplicationServices

	pgClient := pushgateway.NewHTTPClient(dendriteCfg.UserAPI.PushGatewayDisableTLSValidation)

	db, err := storage.NewUserDatabase(
		ctx,
		profileCli,
		cm,
		&dendriteCfg.UserAPI.AccountDatabase,
		dendriteCfg.Global.ServerName,
		dendriteCfg.UserAPI.BCryptCost,
		dendriteCfg.UserAPI.OpenIDTokenLifetimeMS,
		api.DefaultLoginTokenLifetime,
		dendriteCfg.UserAPI.Matrix.ServerNotices.LocalPart,
	)
	if err != nil {
		logrus.WithError(err).Panicf("failed to connect to accounts db")
	}

	keyDB, err := storage.NewKeyDatabase(ctx, cm, &dendriteCfg.KeyServer.Database)
	if err != nil {
		logrus.WithError(err).Panicf("failed to connect to key db")
	}

	syncProducer := producers.NewSyncAPI(
		db, js,
		// TODO: user API should handle syncs for account data. Right now,
		// it's handled by clientapi, and hence uses its topic. When user
		// API handles it for all account data, we can remove it from
		// here.
		dendriteCfg.Global.JetStream.Prefixed(jetstream.OutputClientData),
		dendriteCfg.Global.JetStream.Prefixed(jetstream.OutputNotificationData),
	)
	keyChangeProducer := &producers.KeyChange{
		Topic:     dendriteCfg.Global.JetStream.Prefixed(jetstream.OutputKeyChangeEvent),
		JetStream: js,
		DB:        keyDB,
	}

	userAPI := &internal.UserInternalAPI{
		DB:                   db,
		KeyDatabase:          keyDB,
		SyncProducer:         syncProducer,
		KeyChangeProducer:    keyChangeProducer,
		Config:               &dendriteCfg.UserAPI,
		AppServices:          appServices,
		RSAPI:                rsAPI,
		DisableTLSValidation: dendriteCfg.UserAPI.PushGatewayDisableTLSValidation,
		PgClient:             pgClient,
		FedClient:            fedClient,
	}

	updater := internal.NewDeviceListUpdater(ctx, keyDB, userAPI, keyChangeProducer, fedClient, dendriteCfg.UserAPI.WorkerCount, rsAPI, dendriteCfg.Global.ServerName, enableMetrics, blacklistedOrBackingOffFn)
	userAPI.Updater = updater
	// Remove users which we don't share a room with anymore
	if err = updater.CleanUp(ctx); err != nil {
		logrus.WithError(err).Error("failed to cleanup stale device lists")
	}

	go func(ctx context.Context) {
		err0 := updater.Start(ctx)
		if err0 != nil {
			logrus.WithError(err0).Panicf("failed to start device list updater")
		}
	}(ctx)

	dlConsumer := consumers.NewDeviceListUpdateConsumer(
		ctx, &dendriteCfg.UserAPI, js, updater,
	)
	if err = dlConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panic("failed to start device list consumer")
	}

	sigConsumer := consumers.NewSigningKeyUpdateConsumer(
		ctx, &dendriteCfg.UserAPI, js, userAPI,
	)
	err = sigConsumer.Start(ctx)
	if err != nil {
		logrus.WithError(err).Panic("failed to start signing key consumer")
	}

	receiptConsumer := consumers.NewOutputReceiptEventConsumer(
		ctx, &dendriteCfg.UserAPI, js, db, syncProducer, pgClient,
	)
	err = receiptConsumer.Start(ctx)
	if err != nil {
		logrus.WithError(err).Panic("failed to start user API receipt consumer")
	}

	eventConsumer := consumers.NewOutputRoomEventConsumer(
		ctx, &dendriteCfg.UserAPI, js, db, pgClient, rsAPI, syncProducer,
	)
	err = eventConsumer.Start(ctx)
	if err != nil {
		logrus.WithError(err).Panic("failed to start user API streamed event consumer")
	}

	var cleanOldNotifs func()
	cleanOldNotifs = func() {
		logrus.Infof("Cleaning old notifications")
		err = db.DeleteOldNotifications(ctx)
		if err != nil {
			logrus.WithError(err).Error("Failed to clean old notifications")
		}
		time.AfterFunc(time.Hour, cleanOldNotifs)
	}
	time.AfterFunc(time.Minute, cleanOldNotifs)

	if dendriteCfg.Global.ReportStats.Enabled {
		go util.StartPhoneHomeCollector(ctx, time.Now(), dendriteCfg, db)
	}

	return userAPI
}
