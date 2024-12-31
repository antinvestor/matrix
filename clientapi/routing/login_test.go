package routing

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/pitabwire/util"

	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/antinvestor/matrix/userapi"
	uapi "github.com/antinvestor/matrix/userapi/api"
)

func TestLogin(t *testing.T) {
	aliceAdmin := test.NewUser(t, test.WithAccountType(uapi.AccountTypeAdmin))
	bobUser := &test.User{ID: "@bob:test", AccountType: uapi.AccountTypeUser}
	charlie := &test.User{ID: "@Charlie:test", AccountType: uapi.AccountTypeUser}
	vhUser := &test.User{ID: "@vhuser:vh1"}

	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		cfg, processCtx, closeRig := testrig.CreateConfig(t, testOpts)
		defer closeRig()

		cfg.ClientAPI.RateLimiting.Enabled = false
		natsInstance := jetstream.NATSInstance{}
		// add a vhost
		cfg.Global.VirtualHosts = append(cfg.Global.VirtualHosts, &config.VirtualHost{
			SigningIdentity: fclient.SigningIdentity{ServerName: "vh1"},
		})

		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		routers := httputil.NewRouters()
		caches, err := caching.NewCache(&cfg.Global.Cache)
		if err != nil {
			t.Fatalf("failed to create a cache: %v", err)
		}
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		// Needed for /login
		userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)

		// We mostly need the userAPI for this test, so nil for other APIs/caches etc.
		Setup(routers, cfg, nil, nil, userAPI, nil, nil, nil, nil, nil, nil, nil, caching.DisableMetrics)

		// Create password
		password := util.RandomString(8)

		// create the users
		for _, u := range []*test.User{aliceAdmin, bobUser, vhUser, charlie} {
			localpart, serverName, _ := gomatrixserverlib.SplitID('@', u.ID)
			userRes := &uapi.PerformAccountCreationResponse{}

			if err := userAPI.PerformAccountCreation(ctx, &uapi.PerformAccountCreationRequest{
				AccountType: u.AccountType,
				Localpart:   localpart,
				ServerName:  serverName,
				Password:    password,
			}, userRes); err != nil {
				t.Error("failed to create account: %s", err)
			}
			if !userRes.AccountCreated {
				t.Fatalf("account not created")
			}
		}

		testCases := []struct {
			name   string
			userID string
			wantOK bool
		}{
			{
				name:   "aliceAdmin can login",
				userID: aliceAdmin.ID,
				wantOK: true,
			},
			{
				name:   "bobUser can login",
				userID: bobUser.ID,
				wantOK: true,
			},
			{
				name:   "vhuser can login",
				userID: vhUser.ID,
				wantOK: true,
			},
			{
				name:   "bob with uppercase can login",
				userID: "@Bob:test",
				wantOK: true,
			},
			{
				name:   "Charlie can login (existing uppercase)",
				userID: charlie.ID,
				wantOK: true,
			},
			{
				name:   "Charlie can not login with lowercase userID",
				userID: strings.ToLower(charlie.ID),
				wantOK: false,
			},
		}

		ctx := context.Background()

		// Inject a dummy application service, so we have a "m.login.application_service"
		// in the login flows
		as := &config.ApplicationService{}
		cfg.AppServiceAPI.Derived.ApplicationServices = []config.ApplicationService{*as}

		t.Run("Supported log-in flows are returned", func(t *testing.T) {
			req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login")
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("failed to get log-in flows: %s", rec.Body.String())
			}

			t.Logf("response: %s", rec.Body.String())
			resp := flows{}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatal(err)
			}

			appServiceFound := false
			passwordFound := false
			ssoFound := false
			for _, flow := range resp.Flows {
				if flow.Type == "m.login.password" {
					passwordFound = true
				} else if flow.Type == "m.login.sso" {
					ssoFound = true
				} else if flow.Type == "m.login.application_service" {
					appServiceFound = true
				} else {
					t.Fatalf("got unknown login flow: %s", flow.Type)
				}
			}
			if !appServiceFound {
				t.Fatal("m.login.application_service missing from login flows")
			}
			if !passwordFound {
				t.Fatal("m.login.password missing from login flows")
			}
			if !ssoFound {
				t.Fatal("m.login.sso missing from login flows")
			}

		})

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/login", test.WithJSONBody(t, map[string]interface{}{
					"type": authtypes.LoginTypePassword,
					"identifier": map[string]interface{}{
						"type": "m.id.user",
						"user": tc.userID,
					},
					"password": password,
				}))
				rec := httptest.NewRecorder()
				routers.Client.ServeHTTP(rec, req)
				if tc.wantOK && rec.Code != http.StatusOK {
					t.Fatalf("failed to login: %s", rec.Body.String())
				}

				t.Logf("Response: %s", rec.Body.String())
				// get the response
				resp := loginResponse{}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatal(err)
				}
				// everything OK
				if !tc.wantOK && resp.AccessToken == "" {
					return
				}
				if tc.wantOK && resp.AccessToken == "" {
					t.Fatalf("expected accessToken after successful login but got none: %+v", resp)
				}

				devicesResp := &uapi.QueryDevicesResponse{}
				if err := userAPI.QueryDevices(ctx, &uapi.QueryDevicesRequest{UserID: resp.UserID}, devicesResp); err != nil {
					t.Fatal(err)
				}
				for _, dev := range devicesResp.Devices {
					// We expect the userID on the device to be the same as resp.UserID
					if dev.UserID != resp.UserID {
						t.Fatalf("unexpected userID on device: %s", dev.UserID)
					}
				}
			})
		}
	})
}
