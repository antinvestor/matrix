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

package userapi_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	api2 "github.com/antinvestor/matrix/appservice/api"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/internal"
	"github.com/antinvestor/matrix/userapi/producers"
	"github.com/antinvestor/matrix/userapi/storage"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/util"
	"golang.org/x/crypto/bcrypt"
)

const (
	serverName = spec.ServerName("example.com")
)

type apiTestOpts struct {
	loginTokenLifetime time.Duration
	serverName         string
}

func MustMakeInternalAPI(ctx context.Context, svc *frame.Service, cfg *config.Matrix, t *testing.T, opts apiTestOpts, testOpts test.DependancyOption) (api.UserInternalAPI, storage.UserDatabase) {
	if opts.loginTokenLifetime == 0 {
		opts.loginTokenLifetime = api.DefaultLoginTokenLifetime * time.Millisecond
	}

	sName := serverName
	if opts.serverName != "" {
		sName = spec.ServerName(opts.serverName)
	}
	cm := sqlutil.NewConnectionManager(svc)
	qm := queueutil.NewQueueManager(svc)

	accountDB, err := storage.NewUserDatabase(ctx, nil, nil, cm, sName, bcrypt.MinCost, config.DefaultOpenIDTokenLifetimeMS, opts.loginTokenLifetime, "")
	if err != nil {
		t.Fatalf("failed to create account Cm: %s", err)
	}

	keyDB, err := storage.NewKeyDatabase(ctx, cm)
	if err != nil {
		t.Fatalf("failed to create key Cm: %s", err)
	}

	cfg.Global.SigningIdentity = fclient.SigningIdentity{
		ServerName: sName,
	}

	syncProducer, err := producers.NewSyncAPI(ctx, &cfg.SyncAPI, accountDB, qm)
	if err != nil {
		t.Fatalf("failed to obtain sync publisher: %s", err)
	}

	cfgKeySrv := cfg.KeyServer
	err = qm.RegisterPublisher(ctx, &cfgKeySrv.Queues.OutputKeyChangeEvent)
	if err != nil {
		util.Log(ctx).WithError(err).Panic("failed to register publisher for key change events")
	}

	keyChangeProducer := &producers.KeyChange{DB: keyDB, Qm: qm, Topic: &cfgKeySrv.Queues.OutputKeyChangeEvent}
	return &internal.UserInternalAPI{
		DB:                accountDB,
		KeyDatabase:       keyDB,
		Config:            &cfg.UserAPI,
		SyncProducer:      syncProducer,
		KeyChangeProducer: keyChangeProducer,
	}, accountDB
}

func TestQueryProfile(t *testing.T) {
	aliceAvatarURL := "mxc://example.com/alice"
	aliceDisplayName := "Alice"

	testCases := []struct {
		userID  string
		wantRes *authtypes.Profile
		wantErr error
	}{
		{
			userID: fmt.Sprintf("@alice:%s", serverName),
			wantRes: &authtypes.Profile{
				Localpart:   "alice",
				DisplayName: aliceDisplayName,
				AvatarURL:   aliceAvatarURL,
				ServerName:  string(serverName),
			},
		},
		{
			userID:  fmt.Sprintf("@bob:%s", serverName),
			wantErr: api2.ErrProfileNotExists,
		},
		{
			userID:  "@alice:wrongdomain.com",
			wantErr: api2.ErrProfileNotExists,
		},
	}

	runCases := func(ctx context.Context, testAPI api.UserInternalAPI, http bool) {
		mode := "monolith"
		if http {
			mode = "HTTP"
		}
		for _, tc := range testCases {

			profile, gotErr := testAPI.QueryProfile(ctx, tc.userID)
			if tc.wantErr == nil && gotErr != nil || tc.wantErr != nil && gotErr == nil {
				t.Errorf("QueryProfile %s error, got %s want %s", mode, gotErr, tc.wantErr)
				continue
			}
			if !reflect.DeepEqual(tc.wantRes, profile) {
				t.Errorf("QueryProfile %s response got %+v want %+v", mode, profile, tc.wantRes)
			}
		}
	}

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		userAPI, accountDB := MustMakeInternalAPI(ctx, svc, cfg, t, apiTestOpts{}, testOpts)

		_, err := accountDB.CreateAccount(ctx, "alice", serverName, "foobar", "", api.AccountTypeUser)
		if err != nil {
			t.Fatalf("failed to make account: %s", err)
		}
		if _, _, err = accountDB.SetAvatarURL(ctx, "alice", serverName, aliceAvatarURL); err != nil {
			t.Fatalf("failed to set avatar url: %s", err)
		}
		if _, _, err = accountDB.SetDisplayName(ctx, "alice", serverName, aliceDisplayName); err != nil {
			t.Fatalf("failed to set display name: %s", err)
		}

		runCases(ctx, userAPI, false)
	})
}

// TestPasswordlessLoginFails ensures that a passwordless account cannot
// be logged into using an arbitrary password (effectively a regression test
// for https://github.com/antinvestor/matrix/issues/2780).
func TestPasswordlessLoginFails(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		userAPI, accountDB := MustMakeInternalAPI(ctx, svc, cfg, t, apiTestOpts{}, testOpts)

		_, err := accountDB.CreateAccount(ctx, "auser", serverName, "", "", api.AccountTypeAppService)
		if err != nil {
			t.Fatalf("failed to make account: %s", err)
		}

		userReq := &api.QueryAccountByPasswordRequest{
			Localpart:         "auser",
			PlaintextPassword: "apassword",
		}
		userRes := &api.QueryAccountByPasswordResponse{}
		if err := userAPI.QueryAccountByPassword(ctx, userReq, userRes); err != nil {
			t.Fatal(err)
		}
		if userRes.Exists || userRes.Account != nil {
			t.Fatalf("QueryAccountByPassword should not return correctly for a passwordless account")
		}
	})
}

func TestLoginToken(t *testing.T) {

	t.Run("tokenLoginFlow", func(t *testing.T) {
		test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
			ctx, svc, cfg := testrig.Init(t, testOpts)
			defer svc.Stop(ctx)

			userAPI, accountDB := MustMakeInternalAPI(ctx, svc, cfg, t, apiTestOpts{}, testOpts)

			_, err := accountDB.CreateAccount(ctx, "auser", serverName, "apassword", "", api.AccountTypeUser)
			if err != nil {
				t.Fatalf("failed to make account: %s", err)
			}

			t.Log("Creating a login token like the LoginSSO callback would...")

			creq := api.PerformLoginTokenCreationRequest{
				Data: api.LoginTokenData{UserID: "@auser:example.com"},
			}
			var cresp api.PerformLoginTokenCreationResponse
			if err = userAPI.PerformLoginTokenCreation(ctx, &creq, &cresp); err != nil {
				t.Fatalf("PerformLoginTokenCreation failed: %v", err)
			}

			if cresp.Metadata.Token == "" {
				t.Errorf("PerformLoginTokenCreation Token: got %q, want non-empty", cresp.Metadata.Token)
			}
			if cresp.Metadata.Expiration.Before(time.Now()) {
				t.Errorf("PerformLoginTokenCreation Expiration: got %v, want non-expired", cresp.Metadata.Expiration)
			}

			t.Log("Querying the login token like /login with m.login.token would...")

			qreq := api.QueryLoginTokenRequest{Token: cresp.Metadata.Token}
			var qresp api.QueryLoginTokenResponse
			if err := userAPI.QueryLoginToken(ctx, &qreq, &qresp); err != nil {
				t.Fatalf("QueryLoginToken failed: %v", err)
			}

			if qresp.Data == nil {
				t.Errorf("QueryLoginToken Data: got %v, want non-nil", qresp.Data)
			} else if want := "@auser:example.com"; qresp.Data.UserID != want {
				t.Errorf("QueryLoginToken UserID: got %q, want %q", qresp.Data.UserID, want)
			}

			t.Log("Deleting the login token like /login with m.login.token would...")

			dreq := api.PerformLoginTokenDeletionRequest{Token: cresp.Metadata.Token}
			var dresp api.PerformLoginTokenDeletionResponse
			if err := userAPI.PerformLoginTokenDeletion(ctx, &dreq, &dresp); err != nil {
				t.Fatalf("PerformLoginTokenDeletion failed: %v", err)
			}
		})
	})

	t.Run("expiredTokenIsNotReturned", func(t *testing.T) {
		test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

			ctx, svc, cfg := testrig.Init(t, testOpts)
			defer svc.Stop(ctx)

			userAPI, _ := MustMakeInternalAPI(ctx, svc, cfg, t, apiTestOpts{loginTokenLifetime: -1 * time.Second}, testOpts)

			creq := api.PerformLoginTokenCreationRequest{
				Data: api.LoginTokenData{UserID: "@auser:example.com"},
			}
			var cresp api.PerformLoginTokenCreationResponse
			if err := userAPI.PerformLoginTokenCreation(ctx, &creq, &cresp); err != nil {
				t.Fatalf("PerformLoginTokenCreation failed: %v", err)
			}

			qreq := api.QueryLoginTokenRequest{Token: cresp.Metadata.Token}
			var qresp api.QueryLoginTokenResponse
			if err := userAPI.QueryLoginToken(ctx, &qreq, &qresp); err != nil {
				t.Fatalf("QueryLoginToken failed: %v", err)
			}

			if qresp.Data != nil {
				t.Errorf("QueryLoginToken Data: got %v, want nil", qresp.Data)
			}
		})
	})

	t.Run("deleteWorks", func(t *testing.T) {
		test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
			ctx, svc, cfg := testrig.Init(t, testOpts)
			defer svc.Stop(ctx)

			userAPI, _ := MustMakeInternalAPI(ctx, svc, cfg, t, apiTestOpts{}, testOpts)

			creq := api.PerformLoginTokenCreationRequest{
				Data: api.LoginTokenData{UserID: "@auser:example.com"},
			}
			var cresp api.PerformLoginTokenCreationResponse
			if err := userAPI.PerformLoginTokenCreation(ctx, &creq, &cresp); err != nil {
				t.Fatalf("PerformLoginTokenCreation failed: %v", err)
			}

			dreq := api.PerformLoginTokenDeletionRequest{Token: cresp.Metadata.Token}
			var dresp api.PerformLoginTokenDeletionResponse
			if err := userAPI.PerformLoginTokenDeletion(ctx, &dreq, &dresp); err != nil {
				t.Fatalf("PerformLoginTokenDeletion failed: %v", err)
			}

			qreq := api.QueryLoginTokenRequest{Token: cresp.Metadata.Token}
			var qresp api.QueryLoginTokenResponse
			if err := userAPI.QueryLoginToken(ctx, &qreq, &qresp); err != nil {
				t.Fatalf("QueryLoginToken failed: %v", err)
			}

			if qresp.Data != nil {
				t.Errorf("QueryLoginToken Data: got %v, want nil", qresp.Data)
			}
		})
	})

	t.Run("deleteUnknownIsNoOp", func(t *testing.T) {
		test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
			ctx, svc, cfg := testrig.Init(t, testOpts)
			defer svc.Stop(ctx)

			userAPI, _ := MustMakeInternalAPI(ctx, svc, cfg, t, apiTestOpts{}, testOpts)

			dreq := api.PerformLoginTokenDeletionRequest{Token: "non-existent token"}
			var dresp api.PerformLoginTokenDeletionResponse
			if err := userAPI.PerformLoginTokenDeletion(ctx, &dreq, &dresp); err != nil {
				t.Fatalf("PerformLoginTokenDeletion failed: %v", err)
			}
		})
	})
}

func TestQueryAccountByLocalpart(t *testing.T) {
	alice := test.NewUser(t)

	localpart, userServername, _ := gomatrixserverlib.SplitID('@', alice.ID)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		userAPI, accountDB := MustMakeInternalAPI(ctx, svc, cfg, t, apiTestOpts{}, testOpts)

		createdAcc, err := accountDB.CreateAccount(ctx, localpart, userServername, "", "", alice.AccountType)
		if err != nil {
			t.Error(err)
		}

		testCases := func(t *testing.T, internalAPI api.UserInternalAPI) {
			// Query existing account
			queryAccResp := &api.QueryAccountByLocalpartResponse{}
			if err = internalAPI.QueryAccountByLocalpart(ctx, &api.QueryAccountByLocalpartRequest{
				Localpart:  localpart,
				ServerName: userServername,
			}, queryAccResp); err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(createdAcc, queryAccResp.Account) {
				t.Fatalf("created and queried accounts don't match:\n%+v vs.\n%+v", createdAcc, queryAccResp.Account)
			}

			// Query non-existent account, this should result in an error
			err = internalAPI.QueryAccountByLocalpart(ctx, &api.QueryAccountByLocalpartRequest{
				Localpart:  "doesnotexist",
				ServerName: userServername,
			}, queryAccResp)

			if err == nil {
				t.Fatalf("expected an error, but got none: %+v", queryAccResp)
			}
		}

		testCases(t, userAPI)
	})
}

func TestAccountData(t *testing.T) {
	alice := test.NewUser(t)

	testCases := []struct {
		name      string
		inputData *api.InputAccountDataRequest
		wantErr   bool
	}{
		{
			name:      "not a local user",
			inputData: &api.InputAccountDataRequest{UserID: "@notlocal:example.com"},
			wantErr:   true,
		},
		{
			name:      "local user missing datatype",
			inputData: &api.InputAccountDataRequest{UserID: alice.ID},
			wantErr:   true,
		},
		{
			name:      "missing json",
			inputData: &api.InputAccountDataRequest{UserID: alice.ID, DataType: "m.push_rules", AccountData: nil},
			wantErr:   true,
		},
		{
			name:      "with json",
			inputData: &api.InputAccountDataRequest{UserID: alice.ID, DataType: "m.push_rules", AccountData: []byte("{}")},
		},
		{
			name:      "room data",
			inputData: &api.InputAccountDataRequest{UserID: alice.ID, DataType: "m.push_rules", AccountData: []byte("{}"), RoomID: "!dummy:test"},
		},
		{
			name:      "ignored users",
			inputData: &api.InputAccountDataRequest{UserID: alice.ID, DataType: "m.ignored_user_list", AccountData: []byte("{}")},
		},
		{
			name:      "m.fully_read",
			inputData: &api.InputAccountDataRequest{UserID: alice.ID, DataType: "m.fully_read", AccountData: []byte("{}")},
		},
	}

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		userAPI, _ := MustMakeInternalAPI(ctx, svc, cfg, t, apiTestOpts{serverName: "test"}, testOpts)

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				res := api.InputAccountDataResponse{}
				err := userAPI.InputAccountData(ctx, tc.inputData, &res)
				if tc.wantErr && err == nil {
					t.Fatalf("expected an error, but got none")
				}
				if !tc.wantErr && err != nil {
					t.Fatalf("expected no error, but got: %s", err)
				}

				// query the data again and compare
				queryRes := api.QueryAccountDataResponse{}
				queryReq := api.QueryAccountDataRequest{
					UserID:   tc.inputData.UserID,
					DataType: tc.inputData.DataType,
					RoomID:   tc.inputData.RoomID,
				}
				err = userAPI.QueryAccountData(ctx, &queryReq, &queryRes)
				if err != nil && !tc.wantErr {
					t.Fatal(err)
				}
				// verify global data
				if tc.inputData.RoomID == "" {
					if !reflect.DeepEqual(tc.inputData.AccountData, queryRes.GlobalAccountData[tc.inputData.DataType]) {
						t.Fatalf("expected accountdata to be %s, got %s", string(tc.inputData.AccountData), string(queryRes.GlobalAccountData[tc.inputData.DataType]))
					}
				} else {
					// verify room data
					if !reflect.DeepEqual(tc.inputData.AccountData, queryRes.RoomAccountData[tc.inputData.RoomID][tc.inputData.DataType]) {
						t.Fatalf("expected accountdata to be %s, got %s", string(tc.inputData.AccountData), string(queryRes.RoomAccountData[tc.inputData.RoomID][tc.inputData.DataType]))
					}
				}
			})
		}
	})
}

func TestDevices(t *testing.T) {

	dupeAccessToken := util.RandomString(8)

	displayName := "testing"

	creationTests := []struct {
		name         string
		inputData    *api.PerformDeviceCreationRequest
		wantErr      bool
		wantNewDevID bool
	}{
		{
			name:      "not a local user",
			inputData: &api.PerformDeviceCreationRequest{Localpart: "test1", ServerName: "notlocal"},
			wantErr:   true,
		},
		{
			name:      "implicit local user",
			inputData: &api.PerformDeviceCreationRequest{Localpart: "test1", AccessToken: util.RandomString(8), NoDeviceListUpdate: true, DeviceDisplayName: &displayName},
		},
		{
			name:      "explicit local user",
			inputData: &api.PerformDeviceCreationRequest{Localpart: "test2", ServerName: "test", AccessToken: util.RandomString(8), NoDeviceListUpdate: true},
		},
		{
			name:      "dupe token - ok",
			inputData: &api.PerformDeviceCreationRequest{Localpart: "test3", ServerName: "test", AccessToken: dupeAccessToken, NoDeviceListUpdate: true},
		},
		{
			name:      "dupe token - not ok",
			inputData: &api.PerformDeviceCreationRequest{Localpart: "test3", ServerName: "test", AccessToken: dupeAccessToken, NoDeviceListUpdate: true},
			wantErr:   true,
		},
		{
			name:      "test3 second device", // used to test deletion later
			inputData: &api.PerformDeviceCreationRequest{Localpart: "test3", ServerName: "test", AccessToken: util.RandomString(8), NoDeviceListUpdate: true},
		},
		{
			name:         "test3 third device", // used to test deletion later
			wantNewDevID: true,
			inputData:    &api.PerformDeviceCreationRequest{Localpart: "test3", ServerName: "test", AccessToken: util.RandomString(8), NoDeviceListUpdate: true},
		},
	}

	deletionTests := []struct {
		name        string
		inputData   *api.PerformDeviceDeletionRequest
		wantErr     bool
		wantDevices int
	}{
		{
			name:      "deletion - not a local user",
			inputData: &api.PerformDeviceDeletionRequest{UserID: "@test:notlocalhost"},
			wantErr:   true,
		},
		{
			name:        "deleting not existing devices should not error",
			inputData:   &api.PerformDeviceDeletionRequest{UserID: "@test1:test", DeviceIDs: []string{"iDontExist"}},
			wantDevices: 1,
		},
		{
			name:        "delete all devices",
			inputData:   &api.PerformDeviceDeletionRequest{UserID: "@test1:test"},
			wantDevices: 0,
		},
		{
			name:        "delete all devices",
			inputData:   &api.PerformDeviceDeletionRequest{UserID: "@test3:test"},
			wantDevices: 0,
		},
	}

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		userAPI, _ := MustMakeInternalAPI(ctx, svc, cfg, t, apiTestOpts{serverName: "test"}, testOpts)

		for _, tc := range creationTests {
			t.Run(tc.name, func(t *testing.T) {
				res := api.PerformDeviceCreationResponse{}
				deviceID := util.RandomString(8)
				tc.inputData.DeviceID = &deviceID
				if tc.wantNewDevID {
					tc.inputData.DeviceID = nil
				}
				err := userAPI.PerformDeviceCreation(ctx, tc.inputData, &res)
				if tc.wantErr && err == nil {
					t.Fatalf("expected an error, but got none")
				}
				if !tc.wantErr && err != nil {
					t.Fatalf("expected no error, but got: %s", err)
				}
				if !res.DeviceCreated {
					return
				}

				queryDevicesRes := api.QueryDevicesResponse{}
				queryDevicesReq := api.QueryDevicesRequest{UserID: res.Device.UserID}
				if err = userAPI.QueryDevices(ctx, &queryDevicesReq, &queryDevicesRes); err != nil {
					t.Fatal(err)
				}
				// We only want to verify one device
				if len(queryDevicesRes.Devices) > 1 {
					return
				}
				res.Device.AccessToken = ""

				// At this point, there should only be one device
				if !reflect.DeepEqual(*res.Device, queryDevicesRes.Devices[0]) {
					t.Fatalf("expected device to be\n%#v, got \n%#v", *res.Device, queryDevicesRes.Devices[0])
				}

				newDisplayName := "new name"
				if tc.inputData.DeviceDisplayName == nil {
					updateRes := api.PerformDeviceUpdateResponse{}
					updateReq := api.PerformDeviceUpdateRequest{
						RequestingUserID: fmt.Sprintf("@%s:%s", tc.inputData.Localpart, "test"),
						DeviceID:         deviceID,
						DisplayName:      &newDisplayName,
					}

					if err = userAPI.PerformDeviceUpdate(ctx, &updateReq, &updateRes); err != nil {
						t.Fatal(err)
					}
				}

				queryDeviceInfosRes := api.QueryDeviceInfosResponse{}
				queryDeviceInfosReq := api.QueryDeviceInfosRequest{DeviceIDs: []string{*tc.inputData.DeviceID}}
				if err = userAPI.QueryDeviceInfos(ctx, &queryDeviceInfosReq, &queryDeviceInfosRes); err != nil {
					t.Fatal(err)
				}
				gotDisplayName := queryDeviceInfosRes.DeviceInfo[*tc.inputData.DeviceID].DisplayName
				if tc.inputData.DeviceDisplayName != nil {
					wantDisplayName := *tc.inputData.DeviceDisplayName
					if wantDisplayName != gotDisplayName {
						t.Fatalf("expected displayName to be %s, got %s", wantDisplayName, gotDisplayName)
					}
				} else {
					wantDisplayName := newDisplayName
					if wantDisplayName != gotDisplayName {
						t.Fatalf("expected displayName to be %s, got %s", wantDisplayName, gotDisplayName)
					}
				}
			})
		}

		for _, tc := range deletionTests {
			t.Run(tc.name, func(t *testing.T) {

				delRes := api.PerformDeviceDeletionResponse{}
				err := userAPI.PerformDeviceDeletion(ctx, tc.inputData, &delRes)
				if tc.wantErr && err == nil {
					t.Fatalf("expected an error, but got none")
				}
				if !tc.wantErr && err != nil {
					t.Fatalf("expected no error, but got: %s", err)
				}
				if tc.wantErr {
					return
				}

				queryDevicesRes := api.QueryDevicesResponse{}
				queryDevicesReq := api.QueryDevicesRequest{UserID: tc.inputData.UserID}
				if err = userAPI.QueryDevices(ctx, &queryDevicesReq, &queryDevicesRes); err != nil {
					t.Fatal(err)
				}

				if len(queryDevicesRes.Devices) != tc.wantDevices {
					t.Fatalf("expected %d devices, got %d", tc.wantDevices, len(queryDevicesRes.Devices))
				}

			})
		}
	})
}

// Tests that the session ID of a device is not reused when reusing the same device ID.
func TestDeviceIDReuse(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		userAPI, _ := MustMakeInternalAPI(ctx, svc, cfg, t, apiTestOpts{serverName: "test"}, testOpts)

		res := api.PerformDeviceCreationResponse{}
		// create a first device
		deviceID := util.RandomString(8)
		req := api.PerformDeviceCreationRequest{Localpart: "alice", ServerName: "test", DeviceID: &deviceID, NoDeviceListUpdate: true}
		err := userAPI.PerformDeviceCreation(ctx, &req, &res)
		if err != nil {
			t.Fatal(err)
		}

		// Do the same request again, we expect a different sessionID
		res2 := api.PerformDeviceCreationResponse{}
		// Set NoDeviceListUpdate to false, to verify we don't send device list updates when
		// reusing the same device ID
		req.NoDeviceListUpdate = false
		err = userAPI.PerformDeviceCreation(ctx, &req, &res2)
		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}

		if res2.Device.SessionID == res.Device.SessionID {
			t.Fatalf("expected a different session ID, but they are the same")
		}

	})
}
