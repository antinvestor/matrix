// Copyright 2022 The Global.org Foundation C.I.C.
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

package relayapi_test

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/cacheutil"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/relayapi"
	"github.com/antinvestor/matrix/setup/signing"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

func TestCreateNewRelayInternalAPI(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		caches, err := cacheutil.NewCache(&cfg.Global.Cache)
		if err != nil {
			t.Fatalf("failed to create a cache: %v", err)
		}

		cm := sqlutil.NewConnectionManager(svc)
		relayAPI := relayapi.NewRelayInternalAPI(ctx, cfg, cm, nil, nil, nil, nil, true, caches)
		assert.NotNil(t, relayAPI)
	})
}

func TestCreateRelayInternalInvalidDatabasePanics(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		cm := sqlutil.NewConnectionManager(nil)
		assert.Panics(t, func() {
			relayapi.NewRelayInternalAPI(ctx, cfg, cm, nil, nil, nil, nil, true, nil)
		})
	})
}

func TestCreateInvalidRelayPublicRoutesPanics(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		routers := httputil.NewRouters()
		assert.Panics(t, func() {
			relayapi.AddPublicRoutes(ctx, routers, cfg, nil, nil)
		})
	})
}

func createGetRelayTxnHTTPRequest(serverName spec.ServerName, userID string) *http.Request {
	_, sk, _ := ed25519.GenerateKey(nil)
	keyID := signing.KeyID
	pk := sk.Public().(ed25519.PublicKey)
	origin := spec.ServerName(hex.EncodeToString(pk))
	req := fclient.NewFederationRequest("GET", origin, serverName, "/_matrix/federation/v1/relay_txn/"+userID)
	content := fclient.RelayEntry{EntryID: 0}
	req.SetContent(content)
	req.Sign(origin, gomatrixserverlib.KeyID(keyID), sk)
	httpreq, _ := req.HTTPRequest()
	vars := map[string]string{"userID": userID}
	httpreq = mux.SetURLVars(httpreq, vars)
	return httpreq
}

type sendRelayContent struct {
	PDUs []json.RawMessage       `json:"pdus"`
	EDUs []gomatrixserverlib.EDU `json:"edus"`
}

func createSendRelayTxnHTTPRequest(serverName spec.ServerName, txnID string, userID string) *http.Request {
	_, sk, _ := ed25519.GenerateKey(nil)
	keyID := signing.KeyID
	pk := sk.Public().(ed25519.PublicKey)
	origin := spec.ServerName(hex.EncodeToString(pk))
	req := fclient.NewFederationRequest("PUT", origin, serverName, "/_matrix/federation/v1/send_relay/"+txnID+"/"+userID)
	content := sendRelayContent{}
	req.SetContent(content)
	req.Sign(origin, gomatrixserverlib.KeyID(keyID), sk)
	httpreq, _ := req.HTTPRequest()
	vars := map[string]string{"userID": userID, "txnID": txnID}
	httpreq = mux.SetURLVars(httpreq, vars)
	return httpreq
}

func TestCreateRelayPublicRoutes(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		routers := httputil.NewRouters()
		caches, err := cacheutil.NewCache(&cfg.Global.Cache)
		if err != nil {
			t.Fatalf("failed to create a cache: %v", err)
		}

		cm := sqlutil.NewConnectionManager(svc)

		relayAPI := relayapi.NewRelayInternalAPI(ctx, cfg, cm, nil, nil, nil, nil, true, caches)
		assert.NotNil(t, relayAPI)

		serverKeyAPI := &signing.YggdrasilKeys{}
		keyRing := serverKeyAPI.KeyRing()
		relayapi.AddPublicRoutes(ctx, routers, cfg, keyRing, relayAPI)

		testCases := []struct {
			name     string
			req      *http.Request
			wantCode int
		}{
			{
				name:     "relay_txn invalid user id",
				req:      createGetRelayTxnHTTPRequest(cfg.Global.ServerName, "user:local"),
				wantCode: 400,
			},
			{
				name:     "relay_txn valid user id",
				req:      createGetRelayTxnHTTPRequest(cfg.Global.ServerName, "@user:local"),
				wantCode: 200,
			},
			{
				name:     "send_relay invalid user id",
				req:      createSendRelayTxnHTTPRequest(cfg.Global.ServerName, "123", "user:local"),
				wantCode: 400,
			},
			{
				name:     "send_relay valid user id",
				req:      createSendRelayTxnHTTPRequest(cfg.Global.ServerName, "123", "@user:local"),
				wantCode: 200,
			},
		}

		for _, tc := range testCases {
			w := httptest.NewRecorder()
			routers.Federation.ServeHTTP(w, tc.req)
			if w.Code != tc.wantCode {
				t.Fatalf("%s: got HTTP %d want %d", tc.name, w.Code, tc.wantCode)
			}
		}
	})
}

func TestDisableRelayPublicRoutes(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		routers := httputil.NewRouters()
		caches, err := cacheutil.NewCache(&cfg.Global.Cache)
		if err != nil {
			t.Fatalf("failed to create a cache: %v", err)
		}

		cm := sqlutil.NewConnectionManager(svc)

		relayAPI := relayapi.NewRelayInternalAPI(ctx, cfg, cm, nil, nil, nil, nil, false, caches)
		assert.NotNil(t, relayAPI)

		serverKeyAPI := &signing.YggdrasilKeys{}
		keyRing := serverKeyAPI.KeyRing()
		relayapi.AddPublicRoutes(ctx, routers, cfg, keyRing, relayAPI)

		testCases := []struct {
			name     string
			req      *http.Request
			wantCode int
		}{
			{
				name:     "relay_txn valid user id",
				req:      createGetRelayTxnHTTPRequest(cfg.Global.ServerName, "@user:local"),
				wantCode: 404,
			},
			{
				name:     "send_relay valid user id",
				req:      createSendRelayTxnHTTPRequest(cfg.Global.ServerName, "123", "@user:local"),
				wantCode: 404,
			},
		}

		for _, tc := range testCases {
			w := httptest.NewRecorder()
			routers.Federation.ServeHTTP(w, tc.req)
			if w.Code != tc.wantCode {
				t.Fatalf("%s: got HTTP %d want %d", tc.name, w.Code, tc.wantCode)
			}
		}
	})
}
