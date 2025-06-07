// Copyright 2025 Ant Investor Ltd.
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
	"github.com/antinvestor/matrix/internal/queueutil"
	"time"

	profilev1 "github.com/antinvestor/apis/go/profile/v1"

	"github.com/antinvestor/gomatrixserverlib/spec"
	fedsenderapi "github.com/antinvestor/matrix/federationapi/api"
	"github.com/antinvestor/matrix/federationapi/statistics"
	"github.com/antinvestor/matrix/internal/pushgateway"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
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
	cfg *config.Matrix,
	cm sqlutil.ConnectionManager,
	qm queueutil.QueueManager,
	rsAPI rsapi.UserRoomserverAPI,
	fedClient fedsenderapi.KeyserverFederationAPI,
	profileCli *profilev1.ProfileClient,
	enableMetrics bool,
	blacklistedOrBackingOffFn func(ctx context.Context, s spec.ServerName) (*statistics.ServerStatistics, error),
) *internal.UserInternalAPI {

	var err error

	cfgUsrApi := cfg.UserAPI
	cfgSyncApi := cfg.SyncAPI
	cfgKeySrv := cfg.KeyServer
	appServices := cfg.Derived.ApplicationServices

	pgClient := pushgateway.NewHTTPClient(cfgUsrApi.PushGatewayDisableTLSValidation)

	userapiCm, err := cm.FromOptions(ctx, &cfgUsrApi.AccountDatabase)
	if err != nil {
		logrus.WithError(err).Panicf("failed to obtain accounts db connection manager :%v", err)
	}
	db, err := storage.NewUserDatabase(
		ctx,
		profileCli,
		userapiCm,
		cfg.Global.ServerName,
		cfg.UserAPI.BCryptCost,
		cfg.UserAPI.OpenIDTokenLifetimeMS,
		api.DefaultLoginTokenLifetime,
		cfg.UserAPI.Global.ServerNotices.LocalPart,
	)
	if err != nil {
		logrus.WithError(err).Panicf("failed to connect to accounts db")
	}

	keyCm, err := cm.FromOptions(ctx, &cfgKeySrv.Database)
	if err != nil {
		logrus.WithError(err).Panicf("failed to obtain key db connection manager")
	}
	keyDB, err := storage.NewKeyDatabase(ctx, keyCm)
	if err != nil {
		logrus.WithError(err).Panicf("failed to connect to key db")
	}

	err = qm.RegisterPublisher(ctx, &cfgSyncApi.Queues.OutputClientData)
	if err != nil {
		logrus.WithError(err).Panicf("failed to register publisher for client data")
	}

	err = qm.RegisterPublisher(ctx, &cfgSyncApi.Queues.OutputNotificationData)
	if err != nil {
		logrus.WithError(err).Panicf("failed to register publisher for notification data")
	}

	syncProducer, err := producers.NewSyncAPI(
		ctx, &cfg.SyncAPI,
		db, qm,
	)
	if err != nil {
		logrus.WithError(err).Panicf("failed to obtain sync publisher")
	}

	err = qm.RegisterPublisher(ctx, &cfgKeySrv.Queues.OutputKeyChangeEvent)
	if err != nil {
		logrus.WithError(err).Panicf("failed to register publisher for key change events")
	}

	keyChangeProducer := &producers.KeyChange{
		Topic: &cfgKeySrv.Queues.OutputKeyChangeEvent,
		Qm:    qm,
		DB:    keyDB,
	}

	userAPI := &internal.UserInternalAPI{
		DB:                   db,
		KeyDatabase:          keyDB,
		SyncProducer:         syncProducer,
		KeyChangeProducer:    keyChangeProducer,
		Config:               &cfg.UserAPI,
		AppServices:          appServices,
		RSAPI:                rsAPI,
		DisableTLSValidation: cfg.UserAPI.PushGatewayDisableTLSValidation,
		PgClient:             pgClient,
		FedClient:            fedClient,
	}

	updater := internal.NewDeviceListUpdater(ctx, keyDB, userAPI, keyChangeProducer, fedClient, cfg.UserAPI.WorkerCount, rsAPI, cfg.Global.ServerName, enableMetrics, blacklistedOrBackingOffFn)
	userAPI.Updater = updater
	// Remove users which we don't share a room with anymore
	if err = updater.CleanUp(ctx); err != nil {
		logrus.WithError(err).Error("failed to cleanup stale device lists")
	}

	go func() {
		if err = updater.Start(ctx); err != nil {
			logrus.WithError(err).Panicf("failed to start device list updater")
		}
	}()

	err = consumers.NewDeviceListUpdateConsumer(
		ctx, &cfg.UserAPI, qm, updater,
	)
	if err != nil {
		logrus.WithError(err).Panic("failed to start device list consumer")
	}

	err = consumers.NewSigningKeyUpdateConsumer(
		ctx, &cfg.UserAPI, qm, userAPI,
	)
	if err != nil {
		logrus.WithError(err).Panic("failed to start signing key consumer")
	}

	err = consumers.NewOutputReceiptEventConsumer(
		ctx, &cfg.UserAPI, qm, db, syncProducer, pgClient,
	)
	if err != nil {
		logrus.WithError(err).Panic("failed to start user API receipt consumer")
	}

	err = consumers.NewOutputRoomEventConsumer(
		ctx, &cfg.UserAPI, qm, db, pgClient, rsAPI, syncProducer,
	)
	if err != nil {
		logrus.WithError(err).Panic("failed to start user API streamed event consumer")
	}

	var cleanOldNotifs func()
	cleanOldNotifs = func() {
		select {
		case <-ctx.Done():
			return
		default:
			// Context is still valid, continue with operation

			logrus.Infof("Cleaning old notifications")
			if err = db.DeleteOldNotifications(ctx); err != nil {
				logrus.WithError(err).Error("Failed to clean old notifications")
			}
			time.AfterFunc(time.Hour, cleanOldNotifs)
		}
	}
	time.AfterFunc(time.Minute, cleanOldNotifs)

	if cfg.Global.ReportStats.Enabled {
		go util.StartPhoneHomeCollector(ctx, time.Now(), cfg, db)
	}

	return userAPI
}
