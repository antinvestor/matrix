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

package syncapi

import (
	"context"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/pitabwire/frame"

	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	userapi "github.com/antinvestor/matrix/userapi/api"

	"github.com/antinvestor/matrix/internal/cacheutil"

	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/syncapi/consumers"
	"github.com/antinvestor/matrix/syncapi/notifier"
	"github.com/antinvestor/matrix/syncapi/producers"
	"github.com/antinvestor/matrix/syncapi/routing"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/streams"
	"github.com/antinvestor/matrix/syncapi/sync"
)

// AddPublicRoutes sets up and registers HTTP handlers for the SyncAPI
// component.
func AddPublicRoutes(
	ctx context.Context,
	routers httputil.Routers,
	cfg *config.Matrix,
	cm sqlutil.ConnectionManager,
	qm queueutil.QueueManager,
	userAPI userapi.SyncUserAPI,
	rsAPI api.SyncRoomserverAPI,
	caches cacheutil.LazyLoadCache,
	enableMetrics bool,
) {

	cfgSyncAPI := cfg.SyncAPI

	syncCm, err := cm.FromOptions(ctx, &cfgSyncAPI.Database)
	if err != nil {
		frame.Log(ctx).WithError(err).Panic("failed to obtain sync db connection manager")
	}
	syncDB, err := storage.NewSyncServerDatabase(ctx, syncCm)
	if err != nil {
		frame.Log(ctx).WithError(err).Panic("failed to connect to sync db")
	}

	eduCache := cacheutil.NewTypingCache()
	ntf := notifier.NewNotifier(rsAPI)
	strms := streams.NewSyncStreamProviders(ctx, syncDB, userAPI, rsAPI, eduCache, caches, ntf)
	ntf.SetCurrentPosition(strms.Latest(ctx))
	if err = ntf.Load(ctx, syncDB); err != nil {
		frame.Log(ctx).WithError(err).Panic("failed to load notifier ")
	}

	presenceConsumer, err := consumers.NewPresenceConsumer(
		ctx, &cfgSyncAPI, qm, syncDB,
		ntf, strms.PresenceStreamProvider,
		userAPI,
	)

	if err != nil {
		frame.Log(ctx).WithError(err).Panic("failed to start presence consumer")
	}

	err = qm.RegisterPublisher(ctx, &cfgSyncAPI.Queues.OutputPresenceEvent)
	if err != nil {
		frame.Log(ctx).WithError(err).Panic("failed to register publisher for output presence event")
	}

	federationPresenceProducer := &producers.FederationAPIPresenceProducer{
		Topic: &cfgSyncAPI.Queues.OutputPresenceEvent,
		Qm:    qm,
	}

	requestPool := sync.NewRequestPool(ctx, syncDB, &cfgSyncAPI, userAPI, rsAPI, strms, ntf, federationPresenceProducer, presenceConsumer, enableMetrics)

	err = consumers.NewOutputKeyChangeEventConsumer(
		ctx, &cfgSyncAPI, qm, rsAPI, syncDB, ntf,
		strms.DeviceListStreamProvider,
	)
	if err != nil {
		frame.Log(ctx).WithError(err).Panic("failed to start key change consumer")
	}

	var asProducer *producers.AppserviceEventProducer
	if len(cfg.AppServiceAPI.Derived.ApplicationServices) > 0 {

		err = qm.RegisterPublisher(ctx, &cfg.AppServiceAPI.Queues.OutputAppserviceEvent)
		if err != nil {
			frame.Log(ctx).WithError(err).Panic("failed to register publisher for output appservice event")
		}

		asProducer = &producers.AppserviceEventProducer{
			Qm: qm, Topic: &cfg.AppServiceAPI.Queues.OutputAppserviceEvent,
		}
	}

	err = consumers.NewOutputRoomEventConsumer(
		ctx, &cfgSyncAPI, qm, syncDB, ntf, strms.PDUStreamProvider,
		strms.InviteStreamProvider, rsAPI, asProducer,
	)
	if err != nil {
		frame.Log(ctx).WithError(err).Panic("failed to start room server consumer")
	}

	err = consumers.NewOutputClientDataConsumer(
		ctx, &cfgSyncAPI, qm, syncDB, ntf,
		strms.AccountDataStreamProvider,
	)
	if err != nil {
		frame.Log(ctx).WithError(err).Panic("failed to start client data consumer")
	}

	err = consumers.NewOutputNotificationDataConsumer(
		ctx, &cfgSyncAPI, qm, syncDB, ntf, strms.NotificationDataStreamProvider,
	)
	if err != nil {
		frame.Log(ctx).WithError(err).Panic("failed to start notification data consumer")
	}

	err = consumers.NewOutputTypingEventConsumer(
		ctx, &cfgSyncAPI, qm, eduCache, ntf, strms.TypingStreamProvider,
	)
	if err != nil {
		frame.Log(ctx).WithError(err).Panic("failed to start typing consumer")
	}

	err = consumers.NewOutputSendToDeviceEventConsumer(
		ctx, &cfgSyncAPI, qm, syncDB, userAPI, ntf, strms.SendToDeviceStreamProvider,
	)
	if err != nil {
		frame.Log(ctx).WithError(err).Panic("failed to start send-to-device consumer")
	}

	err = consumers.NewOutputReceiptEventConsumer(
		ctx, &cfgSyncAPI, qm, syncDB, ntf, strms.ReceiptStreamProvider,
	)
	if err != nil {
		frame.Log(ctx).WithError(err).Panic("failed to start receipts consumer")
	}

	rateLimits := httputil.NewRateLimits(&cfg.ClientAPI.RateLimiting)

	err = routing.Setup(
		routers.Client, routers.Validator, requestPool, syncDB, userAPI,
		rsAPI, &cfgSyncAPI, caches,
		rateLimits,
	)
	if err != nil {
		frame.Log(ctx).WithError(err).Panic("failed to start receipts consumer")
	}
}
