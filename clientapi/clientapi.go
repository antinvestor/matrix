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

package clientapi

import (
	"buf.build/gen/go/antinvestor/presence/connectrpc/go/presencev1connect"
	"context"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/sirupsen/logrus"

	partitionv1 "github.com/antinvestor/apis/go/partition/v1"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	appserviceAPI "github.com/antinvestor/matrix/appservice/api"
	"github.com/antinvestor/matrix/clientapi/api"
	"github.com/antinvestor/matrix/clientapi/producers"
	"github.com/antinvestor/matrix/clientapi/routing"
	federationAPI "github.com/antinvestor/matrix/federationapi/api"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/transactions"
	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/setup/config"

	userapi "github.com/antinvestor/matrix/userapi/api"
)

// AddPublicRoutes sets up and registers HTTP handlers for the ClientAPI component.
func AddPublicRoutes(
	ctx context.Context,
	routers httputil.Routers,
	cfg *config.Matrix,
	qm queueutil.QueueManager,
	federation fclient.FederationClient,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	asAPI appserviceAPI.AppServiceInternalAPI,
	transactionsCache *transactions.Cache,
	fsAPI federationAPI.ClientFederationAPI,
	userAPI userapi.ClientUserAPI,
	userDirectoryProvider userapi.QuerySearchProfilesAPI,
	extRoomsProvider api.ExtraPublicRoomsProvider,
	partitionCli *partitionv1.PartitionClient,
	presenceCli presencev1connect.PresenceServiceClient,
	enableMetrics bool,
) {

	syncApiCfg := cfg.SyncAPI.Queues

	err := qm.RegisterPublisher(ctx, &syncApiCfg.OutputReceiptEvent)
	if err != nil {
		logrus.WithError(err).Panic("failed to register publisher for receipt event")
	}

	err = qm.RegisterPublisher(ctx, &syncApiCfg.OutputSendToDeviceEvent)
	if err != nil {
		logrus.WithError(err).Panic("failed to register publisher for send to device event")
	}

	err = qm.RegisterPublisher(ctx, &syncApiCfg.OutputTypingEvent)
	if err != nil {
		logrus.WithError(err).Panic("failed to register publisher for typing event")
	}

	err = qm.RegisterPublisher(ctx, &syncApiCfg.OutputPresenceEvent)
	if err != nil {
		logrus.WithError(err).Panic("failed to register publisher for presence event")
	}

	syncProducer := &producers.SyncAPIProducer{
		Qm:                     qm,
		TopicReceiptEvent:      syncApiCfg.OutputReceiptEvent.Ref(),
		TopicSendToDeviceEvent: syncApiCfg.OutputSendToDeviceEvent.Ref(),
		TopicTypingEvent:       syncApiCfg.OutputTypingEvent.Ref(),
		TopicPresenceEvent:     syncApiCfg.OutputPresenceEvent.Ref(),
		UserAPI:                userAPI,
		ServerName:             cfg.Global.ServerName,
	}

	routing.Setup(
		ctx,
		routers,
		cfg, rsAPI, asAPI,
		userAPI, userDirectoryProvider, federation,
		syncProducer, transactionsCache, fsAPI,
		extRoomsProvider, partitionCli, presenceCli, enableMetrics,
	)
}
