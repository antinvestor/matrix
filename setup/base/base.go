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

package base

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pitabwire/frame"
	"github.com/pitabwire/util"

	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/setup/config"
	sentryhttp "github.com/getsentry/sentry-go/http"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//go:embed static/*.gotmpl
var staticContent embed.FS

//go:embed static/client/login
var loginFallback embed.FS
var StaticContent = staticContent

const HTTPServerTimeout = time.Minute * 5

// CreateClient creates a new client (normally used for media fetch requests).
// Should only be called once per component.
func CreateClient(cfg *config.Matrix, dnsCache *fclient.DNSCache) *fclient.Client {
	if cfg.Global.DisableFederation {
		return fclient.NewClient(
			fclient.WithTransport(noOpHTTPTransport),
		)
	}
	opts := []fclient.ClientOption{
		fclient.WithSkipVerify(cfg.FederationAPI.DisableTLSValidation),
		fclient.WithWellKnownSRVLookups(true),
	}
	if cfg.Global.DNSCache.Enabled && dnsCache != nil {
		opts = append(opts, fclient.WithDNSCache(dnsCache))
	}
	client := fclient.NewClient(opts...)
	client.SetUserAgent(fmt.Sprintf("Matrix/%s", internal.VersionString()))
	return client
}

// CreateFederationClient creates a new federation client. Should only be called
// once per component.
func CreateFederationClient(cfg *config.Matrix, dnsCache *fclient.DNSCache) fclient.FederationClient {
	identities := cfg.Global.SigningIdentities()
	if cfg.Global.DisableFederation {
		return fclient.NewFederationClient(
			identities, fclient.WithTransport(noOpHTTPTransport),
		)
	}
	opts := []fclient.ClientOption{
		fclient.WithTimeout(time.Minute * 5),
		fclient.WithSkipVerify(cfg.FederationAPI.DisableTLSValidation),
		fclient.WithKeepAlives(!cfg.FederationAPI.DisableHTTPKeepalives),
		fclient.WithUserAgent(fmt.Sprintf("Matrix/%s", internal.VersionString())),
	}
	if cfg.Global.DNSCache.Enabled {
		opts = append(opts, fclient.WithDNSCache(dnsCache))
	}
	client := fclient.NewFederationClient(
		identities, opts...,
	)
	return client
}

func ConfigureAdminEndpoints(ctx context.Context, routers httputil.Routers) {
	routers.DendriteAdmin.HandleFunc("/monitor/up", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	routers.DendriteAdmin.HandleFunc("/monitor/health", func(w http.ResponseWriter, r *http.Request) {
		// isDegraded and reasons are not used in this function, so we don't need to call IsDegraded()
		w.WriteHeader(200)
	})
}

// SetupHTTPOption sets up the HTTP server to serve client & federation APIs
// and adds a prometheus handler under /_dendrite/metrics.
func SetupHTTPOption(
	ctx context.Context,
	cfg *config.Matrix,
	routers httputil.Routers,

) (frame.Option, error) {

	log := util.Log(ctx)

	externalRouter := mux.NewRouter().SkipClean(true).UseEncodedPath()

	//Redirect for Landing Page
	externalRouter.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, httputil.PublicStaticPath, http.StatusFound)
	})

	if cfg.Global.Metrics.Enabled {
		externalRouter.Handle("/metrics", httputil.WrapHandlerInBasicAuth(promhttp.Handler(), cfg.Global.Metrics.BasicAuth))
	}

	ConfigureAdminEndpoints(ctx, routers)

	// Parse and execute the landing page template
	tmpl := template.Must(template.ParseFS(staticContent, "static/*.gotmpl"))
	landingPage := &bytes.Buffer{}
	if err := tmpl.ExecuteTemplate(landingPage, "index.gotmpl", map[string]string{
		"Version": internal.VersionString(),
	}); err != nil {
		util.Log(ctx).WithError(err).Error("failed to execute landing page template")
		return nil, err
	}

	routers.Static.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(landingPage.Bytes())
	})

	// We only need the files beneath the static/client/login folder.
	sub, err := fs.Sub(loginFallback, "static/client/login")
	if err != nil {
		log.WithError(err).Panic("unable to read embedded files, this should never happen")
	}
	// Serve a static page for login fallback
	routers.Static.PathPrefix("/client/login/").Handler(http.StripPrefix("/_matrix/static/client/login/", http.FileServer(http.FS(sub))))

	var clientHandler http.Handler
	clientHandler = routers.Client
	if cfg.Global.Sentry.Enabled {
		sentryHandler := sentryhttp.New(sentryhttp.Options{
			Repanic: true,
		})
		clientHandler = sentryHandler.Handle(routers.Client)
	}
	var federationHandler http.Handler
	federationHandler = routers.Federation
	if cfg.Global.Sentry.Enabled {
		sentryHandler := sentryhttp.New(sentryhttp.Options{
			Repanic: true,
		})
		federationHandler = sentryHandler.Handle(routers.Federation)
	}
	externalRouter.PathPrefix(httputil.DendriteAdminPathPrefix).Handler(routers.DendriteAdmin)
	externalRouter.PathPrefix(httputil.PublicClientPathPrefix).Handler(clientHandler)
	if !cfg.Global.DisableFederation {
		externalRouter.PathPrefix(httputil.PublicKeyPathPrefix).Handler(routers.Keys)
		externalRouter.PathPrefix(httputil.PublicFederationPathPrefix).Handler(federationHandler)
	}
	externalRouter.PathPrefix(httputil.SynapseAdminPathPrefix).Handler(routers.SynapseAdmin)
	externalRouter.PathPrefix(httputil.PublicMediaPathPrefix).Handler(routers.Media)
	externalRouter.PathPrefix(httputil.PublicWellKnownPrefix).Handler(routers.WellKnown)
	externalRouter.PathPrefix(httputil.PublicStaticPath).Handler(routers.Static)

	externalRouter.NotFoundHandler = httputil.NotFoundCORSHandler
	externalRouter.MethodNotAllowedHandler = httputil.NotAllowedHandler

	return frame.WithHttpHandler(externalRouter), nil

}

func WaitForShutdown(ctx context.Context) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigs:
	case <-ctx.Done():
	}
	signal.Reset(syscall.SIGINT, syscall.SIGTERM)

	util.Log(ctx).Warn("Shutdown signal received")

	// ShutdownDendrite and WaitForComponentsToFinish are not used in this function, so we don't need to call them
	util.Log(ctx).Warn("Matrix is exiting now")
}
