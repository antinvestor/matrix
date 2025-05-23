package routing

import (
	"bytes"
	"context"
	"github.com/antinvestor/matrix/internal/queueutil"
	"net/http"
	"testing"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/appservice"
	"github.com/antinvestor/matrix/federationapi/statistics"
	"github.com/antinvestor/matrix/internal/cacheutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/antinvestor/matrix/userapi"
	uapi "github.com/antinvestor/matrix/userapi/api"
)

var testIsBlacklistedOrBackingOff = func(ctx context.Context, s spec.ServerName) (*statistics.ServerStatistics, error) {
	return &statistics.ServerStatistics{}, nil
}

func TestJoinRoomByIDOrAlias(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	charlie := test.NewUser(t, test.WithAccountType(uapi.AccountTypeGuest))

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		cm := sqlutil.NewConnectionManager(svc)
		caches, err := cacheutil.NewCache(&cfg.Global.Cache)
		if err != nil {
			t.Fatalf("failed to create a cache: %v", err)
		}
		qm := queueutil.NewQueueManager(svc)
		rsAPI := roomserver.NewInternalAPI(ctx, cfg, cm, qm, caches, cacheutil.DisableMetrics)
		rsAPI.SetFederationAPI(ctx, nil, nil) // creates the rs.Inputer etc
		userAPI := userapi.NewInternalAPI(ctx, cfg, cm, qm, rsAPI, nil, nil, cacheutil.DisableMetrics, testIsBlacklistedOrBackingOff)
		asAPI := appservice.NewInternalAPI(ctx, cfg, qm, userAPI, rsAPI)

		// Create the users in the userapi
		for _, u := range []*test.User{alice, bob, charlie} {
			localpart, serverName, _ := gomatrixserverlib.SplitID('@', u.ID)
			userRes := &uapi.PerformAccountCreationResponse{}
			if err = userAPI.PerformAccountCreation(ctx, &uapi.PerformAccountCreationRequest{
				AccountType: u.AccountType,
				Localpart:   localpart,
				ServerName:  serverName,
				Password:    "someRandomPassword",
			}, userRes); err != nil {
				t.Errorf("failed to create account: %s", err)
			}

		}

		aliceDev := &uapi.Device{UserID: alice.ID}
		bobDev := &uapi.Device{UserID: bob.ID}
		charlieDev := &uapi.Device{UserID: charlie.ID, AccountType: uapi.AccountTypeGuest}

		// create a room with disabled guest access and invite Bob
		resp := createRoom(ctx, createRoomRequest{
			Name:          "testing",
			IsDirect:      true,
			Topic:         "testing",
			Visibility:    "public",
			Preset:        spec.PresetPublicChat,
			RoomAliasName: "alias",
			Invite:        []string{bob.ID},
		}, aliceDev, &cfg.ClientAPI, userAPI, rsAPI, asAPI, time.Now())
		crResp, ok := resp.JSON.(createRoomResponse)
		if !ok {
			t.Fatalf("response is not a createRoomResponse: %+v", resp)
		}

		// create a room with guest access enabled and invite Charlie
		resp = createRoom(ctx, createRoomRequest{
			Name:       "testing",
			IsDirect:   true,
			Topic:      "testing",
			Visibility: "public",
			Preset:     spec.PresetPublicChat,
			Invite:     []string{charlie.ID},
		}, aliceDev, &cfg.ClientAPI, userAPI, rsAPI, asAPI, time.Now())
		crRespWithGuestAccess, ok := resp.JSON.(createRoomResponse)
		if !ok {
			t.Fatalf("response is not a createRoomResponse: %+v", resp)
		}

		// Dummy request
		body := &bytes.Buffer{}
		req, err := http.NewRequest(http.MethodPost, "/?server_name=test", body)
		if err != nil {
			t.Fatal(err)
		}

		testCases := []struct {
			name        string
			device      *uapi.Device
			roomID      string
			wantHTTP200 bool
		}{
			{
				name:        "User can join successfully by alias",
				device:      bobDev,
				roomID:      crResp.RoomAlias,
				wantHTTP200: true,
			},
			{
				name:        "User can join successfully by roomID",
				device:      bobDev,
				roomID:      crResp.RoomID,
				wantHTTP200: true,
			},
			{
				name:   "join is forbidden if user is guest",
				device: charlieDev,
				roomID: crResp.RoomID,
			},
			{
				name:   "room does not exist",
				device: aliceDev,
				roomID: "!doesnotexist:test",
			},
			{
				name:   "user from different server",
				device: &uapi.Device{UserID: "@wrong:server"},
				roomID: crResp.RoomAlias,
			},
			{
				name:   "user doesn't exist locally",
				device: &uapi.Device{UserID: "@doesnotexist:test"},
				roomID: crResp.RoomAlias,
			},
			{
				name:   "invalid room ID",
				device: aliceDev,
				roomID: "invalidRoomID",
			},
			{
				name:   "roomAlias does not exist",
				device: aliceDev,
				roomID: "#doesnotexist:test",
			},
			{
				name:   "room with guest_access event",
				device: charlieDev,
				roomID: crRespWithGuestAccess.RoomID,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {

				joinResp := JoinRoomByIDOrAlias(req, tc.device, rsAPI, userAPI, tc.roomID)
				if tc.wantHTTP200 && !joinResp.Is2xx() {
					t.Fatalf("expected join room to succeed, but didn't: %+v", joinResp)
				}
			})
		}
	})
}
