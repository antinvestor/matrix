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

	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	userapi "github.com/antinvestor/matrix/userapi/api"

	"github.com/antinvestor/matrix/internal/caching"

	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/syncapi/consumers"
	"github.com/antinvestor/matrix/syncapi/notifier"
	"github.com/antinvestor/matrix/syncapi/producers"
	"github.com/antinvestor/matrix/syncapi/routing"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/streams"
	"github.com/antinvestor/matrix/syncapi/sync"

	"github.com/sirupsen/logrus"
)

// AddPublicRoutes sets up and registers HTTP handlers for the SyncAPI
// component.
func AddPublicRoutes(
	ctx context.Context,
	routers httputil.Routers,
	dendriteCfg *config.Matrix,
	cm sqlutil.ConnectionManager,
	natsInstance *jetstream.NATSInstance,
	userAPI userapi.SyncUserAPI,
	rsAPI api.SyncRoomserverAPI,
	caches caching.LazyLoadCache,
	enableMetrics bool,
) {
	js, natsClient := natsInstance.Prepare(ctx, &dendriteCfg.Global.JetStream)

	syncCm, err := cm.FromOptions(ctx, &dendriteCfg.SyncAPI.Database)
	syncDB, err := storage.NewSyncServerDatabase(ctx, syncCm)
	if err != nil {
		logrus.WithError(err).Panicf("failed to connect to sync db")
	}

	eduCache := caching.NewTypingCache()
	notifier := notifier.NewNotifier(rsAPI)
	streams := streams.NewSyncStreamProviders(ctx, syncDB, userAPI, rsAPI, eduCache, caches, notifier)
	notifier.SetCurrentPosition(streams.Latest(ctx))
	if err = notifier.Load(ctx, syncDB); err != nil {
		logrus.WithError(err).Panicf("failed to load notifier ")
	}

	federationPresenceProducer := &producers.FederationAPIPresenceProducer{
		Topic:     dendriteCfg.Global.JetStream.Prefixed(jetstream.OutputPresenceEvent),
		JetStream: js,
	}
	presenceConsumer := consumers.NewPresenceConsumer(
		ctx, &dendriteCfg.SyncAPI, js, natsClient, syncDB,
		notifier, streams.PresenceStreamProvider,
		userAPI,
	)

	requestPool := sync.NewRequestPool(ctx, syncDB, &dendriteCfg.SyncAPI, userAPI, rsAPI, streams, notifier, federationPresenceProducer, presenceConsumer, enableMetrics)

	if err = presenceConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panicf("failed to start presence consumer")
	}

	keyChangeConsumer := consumers.NewOutputKeyChangeEventConsumer(
		ctx, &dendriteCfg.SyncAPI, dendriteCfg.Global.JetStream.Prefixed(jetstream.OutputKeyChangeEvent),
		js, rsAPI, syncDB, notifier,
		streams.DeviceListStreamProvider,
	)
	if err = keyChangeConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panicf("failed to start key change consumer")
	}

	var asProducer *producers.AppserviceEventProducer
	if len(dendriteCfg.AppServiceAPI.Derived.ApplicationServices) > 0 {
		asProducer = &producers.AppserviceEventProducer{
			JetStream: js, Topic: dendriteCfg.Global.JetStream.Prefixed(jetstream.OutputAppserviceEvent),
		}
	}

	roomConsumer := consumers.NewOutputRoomEventConsumer(
		ctx, &dendriteCfg.SyncAPI, js, syncDB, notifier, streams.PDUStreamProvider,
		streams.InviteStreamProvider, rsAPI, asProducer,
	)
	if err = roomConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panicf("failed to start room server consumer")
	}

	clientConsumer := consumers.NewOutputClientDataConsumer(
		ctx, &dendriteCfg.SyncAPI, js, natsClient, syncDB, notifier,
		streams.AccountDataStreamProvider,
	)
	if err = clientConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panicf("failed to start client data consumer")
	}

	notificationConsumer := consumers.NewOutputNotificationDataConsumer(
		ctx, &dendriteCfg.SyncAPI, js, syncDB, notifier, streams.NotificationDataStreamProvider,
	)
	if err = notificationConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panicf("failed to start notification data consumer")
	}

	typingConsumer := consumers.NewOutputTypingEventConsumer(
		ctx, &dendriteCfg.SyncAPI, js, eduCache, notifier, streams.TypingStreamProvider,
	)
	if err = typingConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panicf("failed to start typing consumer")
	}

	sendToDeviceConsumer := consumers.NewOutputSendToDeviceEventConsumer(
		ctx, &dendriteCfg.SyncAPI, js, syncDB, userAPI, notifier, streams.SendToDeviceStreamProvider,
	)
	if err = sendToDeviceConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panicf("failed to start send-to-device consumer")
	}

	receiptConsumer := consumers.NewOutputReceiptEventConsumer(
		ctx, &dendriteCfg.SyncAPI, js, syncDB, notifier, streams.ReceiptStreamProvider,
	)
	if err = receiptConsumer.Start(ctx); err != nil {
		logrus.WithError(err).Panicf("failed to start receipts consumer")
	}

	rateLimits := httputil.NewRateLimits(&dendriteCfg.ClientAPI.RateLimiting)

	routing.Setup(
		routers.Client, requestPool, syncDB, userAPI,
		rsAPI, &dendriteCfg.SyncAPI, caches,
		rateLimits,
	)
}
