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

package httputil

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"strings"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/auth"
	"github.com/antinvestor/matrix/internal"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// BasicAuth is used for authorization on /metrics handlers
type BasicAuth struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type AuthAPIOpts struct {
	GuestAccessAllowed bool
	WithAuth           bool
}

// AuthAPIOption is an option to MakeAuthAPI to add additional checks (e.g. guest access) to verify
// the user is allowed to do specific things.
type AuthAPIOption func(opts *AuthAPIOpts)

// WithAllowGuests checks that guest users have access to this endpoint
func WithAllowGuests() AuthAPIOption {
	return func(opts *AuthAPIOpts) {
		opts.GuestAccessAllowed = true
	}
}

// WithAuth is an option to MakeHTTPAPI to add authentication.
func WithAuth() AuthAPIOption {
	return func(opts *AuthAPIOpts) {
		opts.WithAuth = true
	}
}

// MakeAuthAPI turns a util.JSONRequestHandler function into an http.Handler which authenticates the request.
func MakeAuthAPI(
	metricsName string, userAPI userapi.QueryAcccessTokenAPI,
	f func(*http.Request, *userapi.Device) util.JSONResponse,
	checks ...AuthAPIOption,
) http.Handler {
	h := func(req *http.Request) util.JSONResponse {
		log := util.Log(req.Context())
		ctx, device, err := auth.VerifyUserFromRequest(req, userAPI)
		if err != nil {
			log.Debug("VerifyUserFromRequest %s -> HTTP %d", req.RemoteAddr, err.Code)
			return *err
		}

		if ctx != nil {
			req = req.WithContext(ctx)
		}

		// add the user ID to the log
		log = log.WithField("user_id", device.UserID).WithField("device_id", device.ID)
		req = req.WithContext(util.ContextWithLogger(req.Context(), log))

		defer func() {
			if r := recover(); r != nil {
				// re-panic to return the 500
				panic(r)
			}
		}()

		// apply additional checks, if any
		opts := AuthAPIOpts{}
		for _, opt := range checks {
			opt(&opts)
		}

		if !opts.GuestAccessAllowed && device.AccountType == userapi.AccountTypeGuest {
			return util.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.GuestAccessForbidden("Guest access not allowed"),
			}
		}

		jsonRes := f(req, device)
		return jsonRes
	}
	return MakeExternalAPI(metricsName, h)
}

// MakeAdminAPI is a wrapper around MakeAuthAPI which enforces that the request can only be
// completed by a user that is a server administrator.
func MakeAdminAPI(
	metricsName string, userAPI userapi.QueryAcccessTokenAPI,
	f func(*http.Request, *userapi.Device) util.JSONResponse,
) http.Handler {
	return MakeAuthAPI(metricsName, userAPI, func(req *http.Request, device *userapi.Device) util.JSONResponse {
		if device.AccountType != userapi.AccountTypeAdmin {
			return util.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.Forbidden("This API can only be used by admin users."),
			}
		}
		return f(req, device)
	})
}

// MakeExternalAPI turns a util.JSONRequestHandler function into an http.Handler.
// This is used for APIs that are called from the internet.
func MakeExternalAPI(metricsName string, f func(*http.Request) util.JSONResponse) http.Handler {
	// TODO: We shouldn't be directly reading env vars here, inject it in instead.
	// Refactor this when we split out config structs.
	verbose := os.Getenv("DENDRITE_TRACE_HTTP") == "1"

	h := util.MakeJSONAPI(util.NewJSONRequestHandler(f))
	withSpan := func(w http.ResponseWriter, req *http.Request) {
		nextWriter := w
		if verbose {
			logger := util.Log(req.Context())
			// Log outgoing response
			rec := httptest.NewRecorder()
			nextWriter = rec
			defer func() {
				resp := rec.Result()
				dump, err := httputil.DumpResponse(resp, true)
				if err != nil {
					logger.Debug("Failed to dump outgoing response: %s", err)
				} else {
					strSlice := strings.Split(string(dump), "\n")
					for _, s := range strSlice {
						logger.Debug(s)
					}
				}
				// copy the response to the client
				for hdr, vals := range resp.Header {
					for _, val := range vals {
						w.Header().Add(hdr, val)
					}
				}
				w.WriteHeader(resp.StatusCode)
				// discard errors as this is for debugging
				_, _ = io.Copy(w, resp.Body)
				_ = resp.Body.Close()
			}()

			// Log incoming request
			dump, err := httputil.DumpRequest(req, true)
			if err != nil {
				logger.Debug("Failed to dump incoming request: %s", err)
			} else {
				strSlice := strings.Split(string(dump), "\n")
				for _, s := range strSlice {
					logger.Debug(s)
				}
			}
		}

		trace, ctx := internal.StartTask(req.Context(), metricsName)
		defer trace.EndTask()
		req = req.WithContext(ctx)
		h.ServeHTTP(nextWriter, req)

	}

	return http.HandlerFunc(withSpan)
}

// MakeHTTPAPI adds Span metrics to the HTML Handler function
// This is used to serve HTML alongside JSON error messages
func MakeHTTPAPI(metricsName string, userAPI userapi.QueryAcccessTokenAPI, enableMetrics bool, f func(http.ResponseWriter, *http.Request), checks ...AuthAPIOption) http.Handler {
	withSpan := func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodOptions {
			util.SetCORSHeaders(w)
			w.WriteHeader(http.StatusOK) // Maybe http.StatusNoContent?
			return
		}

		trace, ctx := internal.StartTask(req.Context(), metricsName)
		defer trace.EndTask()
		req = req.WithContext(ctx)

		// apply additional checks, if any
		opts := AuthAPIOpts{}
		for _, opt := range checks {
			opt(&opts)
		}

		if opts.WithAuth {
			logger := util.Log(req.Context())
			nueCtx, _, jsonErr := auth.VerifyUserFromRequest(req, userAPI)
			if jsonErr != nil {
				w.WriteHeader(jsonErr.Code)
				if err := json.NewEncoder(w).Encode(jsonErr.JSON); err != nil {
					logger.WithError(err).Error("failed to encode JSON response")
				}
				return
			}

			if nueCtx != nil {
				req = req.WithContext(nueCtx)
			}
		}

		f(w, req)
	}

	if !enableMetrics {
		return http.HandlerFunc(withSpan)
	}

	return promhttp.InstrumentHandlerCounter(
		promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name:      metricsName,
				Help:      "Total number of http requests for HTML resources",
				Namespace: "matrix",
			},
			[]string{"code"},
		),
		http.HandlerFunc(withSpan),
	)
}

// WrapHandlerInBasicAuth adds basic auth to a handler. Only used for /metrics
func WrapHandlerInBasicAuth(h http.Handler, b BasicAuth) http.HandlerFunc {
	logger := util.Log(context.TODO())

	if b.Username == "" || b.Password == "" {
		logger.Warn("Metrics are exposed without protection. Make sure you set up protection at proxy level.")
	}
	return func(w http.ResponseWriter, r *http.Request) {
		// Serve without authorization if either Username or Password is unset
		if b.Username == "" || b.Password == "" {
			h.ServeHTTP(w, r)
			return
		}
		user, pass, ok := r.BasicAuth()

		if !ok || user != b.Username || pass != b.Password {
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		h.ServeHTTP(w, r)
	}
}

// WrapHandlerInCORS adds CORS headers to all responses, including all error
// responses.
// Handles OPTIONS requests directly.
func WrapHandlerInCORS(h http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization")

		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			// Its easiest just to always return a 200 OK for everything. Whether
			// this is technically correct or not is a question, but in the end this
			// is what a lot of other people do (including synapse) and the clients
			// are perfectly happy with it.
			w.WriteHeader(http.StatusOK)
		} else {
			h.ServeHTTP(w, r)
		}
	}
}
