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

package main

import (
	"buf.build/gen/go/antinvestor/presence/connectrpc/go/presencev1connect"
	"fmt"
	"github.com/antinvestor/matrix/internal/queueutil"
	"net/http"
	"strings"

	partitionv1 "github.com/antinvestor/apis/go/partition/v1"
	"github.com/pitabwire/frame"

	apis "github.com/antinvestor/apis/go/common"
	profilev1 "github.com/antinvestor/apis/go/profile/v1"

	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/cacheutil"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/getsentry/sentry-go"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/antinvestor/matrix/appservice"
	"github.com/antinvestor/matrix/federationapi"
	"github.com/antinvestor/matrix/roomserver"
	"github.com/antinvestor/matrix/setup"
	basepkg "github.com/antinvestor/matrix/setup/base"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/mscs"
	"github.com/antinvestor/matrix/userapi"
)

func main() {

	serviceName := "service_matrix"

	cfg := setup.ParseFlags(true)
	globalCfg := cfg.Global

	ctx, service := frame.NewService(serviceName, frame.Config(&globalCfg))
	defer service.Stop(ctx)

	log := service.L(ctx)

	log.
		WithField("oauth2 service uri", globalCfg.Oauth2ServiceURI).
		WithField("well known server name", globalCfg.WellKnownServerName).
		WithField("debug", globalCfg.LoggingLevel()).
		Info("debug configuration values")

	configErrors := &config.ConfigErrors{}
	cfg.Verify(configErrors)
	if len(*configErrors) > 0 {
		for _, err := range *configErrors {
			log.Error("Configuration error: %s", err)
		}
		log.Fatal("Failed to start due to configuration errors")
	}

	log.Info("Global version %s", internal.VersionString())
	if !cfg.ClientAPI.RegistrationDisabled && cfg.ClientAPI.OpenRegistrationWithoutVerificationEnabled {
		log.Warn("Open registration is enabled")
	}

	// create DNS cache
	var dnsCache *fclient.DNSCache
	if globalCfg.DNSCache.Enabled {
		dnsCache = fclient.NewDNSCache(
			globalCfg.DNSCache.CacheSize,
			globalCfg.DNSCache.CacheLifetime,
		)
		log.Info(
			"DNS cache enabled (size %d, lifetime %s)",
			globalCfg.DNSCache.CacheSize,
			globalCfg.DNSCache.CacheLifetime,
		)
	}

	// setup sentry
	if globalCfg.Sentry.Enabled {
		log.Info("Setting up Sentry for debugging...")
		err := sentry.Init(sentry.ClientOptions{
			Dsn:              globalCfg.Sentry.DSN,
			Environment:      globalCfg.Sentry.Environment,
			Debug:            true,
			ServerName:       string(globalCfg.ServerName),
			Release:          "matrix@" + internal.VersionString(),
			AttachStacktrace: true,
		})
		if err != nil {
			log.WithError(err).Panic("failed to start Sentry")
		}

	}

	var (
		profileCli   *profilev1.ProfileClient
		partitionCli *partitionv1.PartitionClient
	)

	log.
		WithField("enabled", globalCfg.DistributedAPI.Enabled).
		Info("distributed apis")
	if globalCfg.DistributedAPI.Enabled {

		err := service.RegisterForJwt(ctx)
		if err != nil {
			log.WithError(err).Fatal("main -- could not register fo jwt")
		}

		log.
			WithField("oauth2 service uri", globalCfg.Oauth2ServiceURI).
			WithField("client_id", service.JwtClientID()).
			WithField("client_secret", service.JwtClientSecret()).
			Info("distributed apis token configuration")

		oauth2ServiceURL := fmt.Sprintf("%s/oauth2/token", globalCfg.Oauth2ServiceURI)

		audienceList := make([]string, 0)
		oauth2ServiceAudience := globalCfg.Oauth2ServiceAudience
		if oauth2ServiceAudience != "" {
			audienceList = strings.Split(oauth2ServiceAudience, ",")
		}

		apiConfig := globalCfg.DistributedAPI
		profileCli, err = profilev1.NewProfileClient(ctx,
			apis.WithEndpoint(apiConfig.ProfileServiceUri),
			apis.WithTokenEndpoint(oauth2ServiceURL),
			apis.WithTokenUsername(service.JwtClientID()),
			apis.WithTokenPassword(service.JwtClientSecret()),
			apis.WithAudiences(audienceList...))
		if err != nil {
			log.WithError(err).Panic("failed to initialise profile api client")
		}

		partitionCli, err = partitionv1.NewPartitionsClient(ctx,
			apis.WithEndpoint(apiConfig.PartitionServiceUri),
			apis.WithTokenEndpoint(oauth2ServiceURL),
			apis.WithTokenUsername(service.JwtClientID()),
			apis.WithTokenPassword(service.JwtClientSecret()),
			apis.WithAudiences(audienceList...))

		if err != nil {
			log.WithError(err).Panic("failed to initialise partition api client")
		}
	}

	federationClient := basepkg.CreateFederationClient(cfg, dnsCache)
	httpClient := basepkg.CreateClient(cfg, dnsCache)

	// prepare required dependencies
	cm := sqlutil.NewConnectionManager(service)
	routers := httputil.NewRouters()

	globalCfg.Cache.EnablePrometheus = cacheutil.EnableMetrics
	caches, err := cacheutil.NewCache(&globalCfg.Cache)
	if err != nil {
		log.WithError(err).Panic("failed to create cache")
	}

	qm := queueutil.NewQueueManager(service)

	presenceCli := presencev1connect.NewPresenceServiceClient(
		http.DefaultClient, cfg.Global.SyncAPIPresenceURI,
	)

	rsAPI := roomserver.NewInternalAPI(ctx, cfg, cm, qm, caches, cacheutil.EnableMetrics)
	fsAPI := federationapi.NewInternalAPI(
		ctx, cfg, cm, qm, federationClient, rsAPI, caches, nil, false, presenceCli,
	)

	keyRing := fsAPI.KeyRing()

	// The underlying roomserver implementation needs to be able to call the fedsender.
	// This is different to rsAPI which can be the http client which doesn't need this
	// dependency. Other components also need updating after their dependencies are up.
	rsAPI.SetFederationAPI(ctx, fsAPI, keyRing)

	userAPI := userapi.NewInternalAPI(ctx, cfg, cm, qm, rsAPI, federationClient, profileCli, cacheutil.EnableMetrics, fsAPI.IsBlacklistedOrBackingOff)
	asAPI := appservice.NewInternalAPI(ctx, cfg, qm, userAPI, rsAPI)

	rsAPI.SetAppserviceAPI(ctx, asAPI)
	rsAPI.SetUserAPI(ctx, userAPI)

	monolith := setup.Monolith{
		Config:    cfg,
		Service:   service,
		Client:    httpClient,
		FedClient: federationClient,
		KeyRing:   keyRing,

		AppserviceAPI: asAPI,
		// always use the concrete impl here even in -http mode because adding public routes
		// must be done on the concrete impl not an HTTP client else fedapi will call itself
		FederationAPI: fsAPI,
		RoomserverAPI: rsAPI,
		UserAPI:       userAPI,

		PartitionCli: partitionCli,
		ProfileCli:   profileCli,

		PresenceCli: presenceCli,
	}
	monolith.AddAllPublicRoutes(ctx, cfg, routers, cm, qm, caches, cacheutil.EnableMetrics)

	if len(cfg.MSCs.MSCs) > 0 {
		err = mscs.Enable(ctx, cfg, cm, routers, &monolith, caches)
		if err != nil {
			log.WithError(err).Fatal("Failed to enable MSCs")
		}
	}

	upCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "matrix",
		Name:      "up",
		ConstLabels: map[string]string{
			"version": internal.VersionString(),
		},
	})
	upCounter.Add(1)
	prometheus.MustRegister(upCounter)

	var httpOpt frame.Option
	httpOpt, err = basepkg.SetupHTTPOption(ctx, cfg, routers)
	if err != nil {
		log.WithError(err).Fatal("could not setup Server Routers")
	}

	serviceOptions := []frame.Option{httpOpt}
	service.Init(serviceOptions...)

	log.WithField("server http port", globalCfg.HttpServerPort).
		Info(" Initiating server operations")
	defer monolith.Service.Stop(ctx)
	err = monolith.Service.Run(ctx, "")
	if err != nil {
		log.WithError(err).Fatal("could not run Server ")
	}

}
