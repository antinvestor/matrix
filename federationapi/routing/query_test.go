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

package routing_test

import (
	"context"
	"encoding/hex"
	"io"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	fedAPI "github.com/antinvestor/matrix/federationapi"
	"github.com/antinvestor/matrix/federationapi/routing"
	"github.com/antinvestor/matrix/internal/actorutil"
	"github.com/antinvestor/matrix/internal/cacheutil"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/signing"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/ed25519"
)

type fakeFedClient struct {
	fclient.FederationClient
}

func (f *fakeFedClient) LookupRoomAlias(ctx context.Context, origin, s spec.ServerName, roomAlias string) (res fclient.RespDirectory, err error) {
	return
}

func TestHandleQueryDirectory(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		cm := sqlutil.NewConnectionManager(svc)
		routers := httputil.NewRouters()

		fedMux := mux.NewRouter().SkipClean(true).PathPrefix(httputil.PublicFederationPathPrefix).Subrouter().UseEncodedPath()
		qm := queueutil.NewQueueManager(svc)

		am, err := actorutil.NewManager(ctx, &cfg.Global.Actors, qm)
		if err != nil {
			t.Fatalf("failed to create an actor manager: %v", err)
		}

		routers.Federation = fedMux
		cfg.FederationAPI.Global.ServerName = testOrigin
		cfg.FederationAPI.Global.Metrics.Enabled = false
		fedClient := fakeFedClient{}
		serverKeyAPI := &signing.YggdrasilKeys{}
		keyRing := serverKeyAPI.KeyRing()
		fedapi := fedAPI.NewInternalAPI(ctx, cfg, cm, qm, am, &fedClient, nil, nil, keyRing, true, nil)
		userapi := fakeUserAPI{}

		routing.Setup(ctx, routers, cfg, nil, fedapi, keyRing, &fedClient, &userapi, &cfg.MSCs, nil, cacheutil.DisableMetrics)

		handler := fedMux.Get(routing.QueryDirectoryRouteName).GetHandler().ServeHTTP
		_, sk, _ := ed25519.GenerateKey(nil)
		keyID := signing.KeyID
		pk := sk.Public().(ed25519.PublicKey)
		serverName := spec.ServerName(hex.EncodeToString(pk))
		req := fclient.NewFederationRequest("GET", serverName, testOrigin, "/query/directory?room_alias="+url.QueryEscape("#room:server"))
		type queryContent struct{}
		content := queryContent{}
		err = req.SetContent(content)
		if err != nil {
			t.Fatalf("Error: %s", err.Error())
		}
		req.Sign(serverName, gomatrixserverlib.KeyID(keyID), sk)
		httpReq, err := req.HTTPRequest()
		if err != nil {
			t.Fatalf("Error: %s", err.Error())
		}
		// vars := map[string]string{"room_alias": "#room:server"}
		w := httptest.NewRecorder()
		// httpReq = mux.SetURLVars(httpReq, vars)
		handler(w, httpReq)

		res := w.Result()
		data, _ := io.ReadAll(res.Body)
		println(string(data))
		assert.Equal(t, 200, res.StatusCode)
	})
}
