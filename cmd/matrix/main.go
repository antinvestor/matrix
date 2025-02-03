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
	"flag"
	partitionv1 "github.com/antinvestor/apis/go/partition/v1"
	"time"

	apis "github.com/antinvestor/apis/go/common"
	profilev1 "github.com/antinvestor/apis/go/profile/v1"

	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/antinvestor/matrix/setup/process"
	"github.com/getsentry/sentry-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/antinvestor/matrix/appservice"
	"github.com/antinvestor/matrix/federationapi"
	"github.com/antinvestor/matrix/roomserver"
	"github.com/antinvestor/matrix/setup"
	basepkg "github.com/antinvestor/matrix/setup/base"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/mscs"
	"github.com/antinvestor/matrix/userapi"
)

var (
	unixSocket = flag.String("unix-socket", "",
		"EXPERIMENTAL(unstable): The HTTP listening unix socket for the server (disables http[s]-bind-address feature)",
	)
	unixSocketPermission = flag.String("unix-socket-permission", "755",
		"EXPERIMENTAL(unstable): The HTTP listening unix socket permission for the server (in chmod format like 755)",
	)
	httpBindAddr  = flag.String("http-bind-address", ":8008", "The HTTP listening port for the server")
	httpsBindAddr = flag.String("https-bind-address", ":8448", "The HTTPS listening port for the server")
	certFile      = flag.String("tls-cert", "", "The PEM formatted X509 certificate to use for TLS")
	keyFile       = flag.String("tls-key", "", "The PEM private key to use for TLS")
)

func main() {
	cfg := setup.ParseFlags(true)
	httpAddr := config.ServerAddress{}
	httpsAddr := config.ServerAddress{}
	if *unixSocket == "" {
		http, err := config.HTTPAddress("http://" + *httpBindAddr)
		if err != nil {
			logrus.WithError(err).Fatalf("Failed to parse http address")
		}
		httpAddr = http
		https, err := config.HTTPAddress("https://" + *httpsBindAddr)
		if err != nil {
			logrus.WithError(err).Fatalf("Failed to parse https address")
		}
		httpsAddr = https
	} else {
		socket, err := config.UnixSocketAddress(*unixSocket, *unixSocketPermission)
		if err != nil {
			logrus.WithError(err).Fatalf("Failed to parse unix socket")
		}
		httpAddr = socket
	}

	configErrors := &config.ConfigErrors{}
	cfg.Verify(configErrors)
	if len(*configErrors) > 0 {
		for _, err := range *configErrors {
			logrus.Errorf("Configuration error: %s", err)
		}
		logrus.Fatalf("Failed to start due to configuration errors")
	}
	processCtx := process.NewProcessContext()

	internal.SetupStdLogging()
	internal.SetupHookLogging(cfg.Logging)
	internal.SetupPprof()

	basepkg.PlatformSanityChecks()

	logrus.Infof("Matrix version %s", internal.VersionString())
	if !cfg.ClientAPI.RegistrationDisabled && cfg.ClientAPI.OpenRegistrationWithoutVerificationEnabled {
		logrus.Warn("Open registration is enabled")
	}

	// create DNS cache
	var dnsCache *fclient.DNSCache
	if cfg.Global.DNSCache.Enabled {
		dnsCache = fclient.NewDNSCache(
			cfg.Global.DNSCache.CacheSize,
			cfg.Global.DNSCache.CacheLifetime,
		)
		logrus.Infof(
			"DNS cache enabled (size %d, lifetime %s)",
			cfg.Global.DNSCache.CacheSize,
			cfg.Global.DNSCache.CacheLifetime,
		)
	}

	// setup tracing
	closer, err := cfg.SetupTracing()
	if err != nil {
		logrus.WithError(err).Panicf("failed to start opentracing")
	}
	defer closer.Close() // nolint: errcheck

	// setup sentry
	if cfg.Global.Sentry.Enabled {
		logrus.Info("Setting up Sentry for debugging...")
		err = sentry.Init(sentry.ClientOptions{
			Dsn:              cfg.Global.Sentry.DSN,
			Environment:      cfg.Global.Sentry.Environment,
			Debug:            true,
			ServerName:       string(cfg.Global.ServerName),
			Release:          "matrix@" + internal.VersionString(),
			AttachStacktrace: true,
		})
		if err != nil {
			logrus.WithError(err).Panic("failed to start Sentry")
		}
		go func() {
			processCtx.ComponentStarted()
			<-processCtx.WaitForShutdown()
			if !sentry.Flush(time.Second * 5) {
				logrus.Warnf("failed to flush all Sentry events!")
			}
			processCtx.ComponentFinished()
		}()
	}

	var (
		profileCli   *profilev1.ProfileClient
		partitionCli *partitionv1.PartitionClient
	)

	if cfg.Global.DistributedAPI.Enabled {
		apiConfig := cfg.Global.DistributedAPI
		ctx := processCtx.Context()
		profileCli, err = profilev1.NewProfileClient(ctx,
			apis.WithEndpoint(apiConfig.ProfileServiceUri),
			apis.WithTokenEndpoint(apiConfig.TokenServiceUri),
			apis.WithTokenUsername(apiConfig.TokenServiceUserName),
			apis.WithTokenPassword(apiConfig.TokenServiceSecret),
			apis.WithAudiences(apiConfig.TokenServiceAudience...))
		if err != nil {
			logrus.WithError(err).Panicf("failed to initialise profile api client")
		}

		partitionCli, err = partitionv1.NewPartitionsClient(ctx,
			apis.WithEndpoint(apiConfig.ProfileServiceUri),
			apis.WithTokenEndpoint(apiConfig.TokenServiceUri),
			apis.WithTokenUsername(apiConfig.TokenServiceUserName),
			apis.WithTokenPassword(apiConfig.TokenServiceSecret),
			apis.WithAudiences(apiConfig.TokenServiceAudience...))

		if err != nil {
			logrus.WithError(err).Panicf("failed to initialise partition api client")
		}
	}

	federationClient := basepkg.CreateFederationClient(cfg, dnsCache)
	httpClient := basepkg.CreateClient(cfg, dnsCache)

	// prepare required dependencies
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	routers := httputil.NewRouters()

	cfg.Global.Cache.EnablePrometheus = caching.EnableMetrics
	caches, err := caching.NewCache(&cfg.Global.Cache)
	if err != nil {
		logrus.WithError(err).Panicf("failed to create cache")
	}

	natsInstance := jetstream.NATSInstance{}
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.EnableMetrics)
	fsAPI := federationapi.NewInternalAPI(
		processCtx, cfg, cm, &natsInstance, federationClient, rsAPI, caches, nil, false,
	)

	keyRing := fsAPI.KeyRing()

	// The underlying roomserver implementation needs to be able to call the fedsender.
	// This is different to rsAPI which can be the http client which doesn't need this
	// dependency. Other components also need updating after their dependencies are up.
	rsAPI.SetFederationAPI(fsAPI, keyRing)

	userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, federationClient, profileCli, caching.EnableMetrics, fsAPI.IsBlacklistedOrBackingOff)
	asAPI := appservice.NewInternalAPI(processCtx, cfg, &natsInstance, userAPI, rsAPI)

	rsAPI.SetAppserviceAPI(asAPI)
	rsAPI.SetUserAPI(userAPI)

	monolith := setup.Monolith{
		Config:    cfg,
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
	}
	monolith.AddAllPublicRoutes(processCtx, cfg, routers, cm, &natsInstance, caches, caching.EnableMetrics)

	if len(cfg.MSCs.MSCs) > 0 {
		if err := mscs.Enable(cfg, cm, routers, &monolith, caches); err != nil {
			logrus.WithError(err).Fatalf("Failed to enable MSCs")
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

	// Expose the matrix APIs directly rather than putting them under a /api path.
	go func() {
		basepkg.SetupAndServeHTTP(processCtx, cfg, routers, httpAddr, nil, nil)
	}()
	// Handle HTTPS if certificate and key are provided
	if *unixSocket == "" && *certFile != "" && *keyFile != "" {
		go func() {
			basepkg.SetupAndServeHTTP(processCtx, cfg, routers, httpsAddr, certFile, keyFile)
		}()
	}

	// We want to block forever to let the HTTP and HTTPS handler serve the APIs
	basepkg.WaitForShutdown(processCtx)
}
