package federationapi_test

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/antinvestor/gomatrix"
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi"
	"github.com/antinvestor/matrix/federationapi/api"
	"github.com/antinvestor/matrix/federationapi/internal"
	"github.com/antinvestor/matrix/federationapi/routing"
	"github.com/antinvestor/matrix/internal/actorutil"
	"github.com/antinvestor/matrix/internal/cacheutil"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	rsapi "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/constants"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
)

type fedRoomserverAPI struct {
	rsapi.FederationRoomserverAPI
	inputRoomEvents   func(ctx context.Context, req *rsapi.InputRoomEventsRequest, res *rsapi.InputRoomEventsResponse)
	queryRoomsForUser func(ctx context.Context, userID spec.UserID, desiredMembership string) ([]spec.RoomID, error)
}

func (f *fedRoomserverAPI) QueryUserIDForSender(ctx context.Context, roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(senderID), true)
}

func (f *fedRoomserverAPI) QuerySenderIDForUser(ctx context.Context, roomID spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	senderID := spec.SenderID(userID.String())
	return &senderID, nil
}

// PerformJoin will call this function
func (f *fedRoomserverAPI) InputRoomEvents(ctx context.Context, req *rsapi.InputRoomEventsRequest, res *rsapi.InputRoomEventsResponse) {
	if f.inputRoomEvents == nil {
		return
	}
	f.inputRoomEvents(ctx, req, res)
}

// keychange consumer calls this
func (f *fedRoomserverAPI) QueryRoomsForUser(ctx context.Context, userID spec.UserID, desiredMembership string) ([]spec.RoomID, error) {
	if f.queryRoomsForUser == nil {
		return nil, nil
	}
	return f.queryRoomsForUser(ctx, userID, desiredMembership)
}

// TODO: This struct isn't generic, only works for TestFederationAPIJoinThenKeyUpdate
type fedClient struct {
	fedClientMutex sync.Mutex
	fclient.FederationClient
	allowJoins []*test.Room
	keys       map[spec.ServerName]struct {
		key   ed25519.PrivateKey
		keyID gomatrixserverlib.KeyID
	}
	t       *testing.T
	sentTxn bool
}

func (f *fedClient) GetServerKeys(ctx context.Context, matrixServer spec.ServerName) (gomatrixserverlib.ServerKeys, error) {
	f.fedClientMutex.Lock()
	defer f.fedClientMutex.Unlock()
	fmt.Println("GetServerKeys:", matrixServer)
	var keys gomatrixserverlib.ServerKeys
	var keyID gomatrixserverlib.KeyID
	var pkey ed25519.PrivateKey
	for srv, data := range f.keys {
		if srv == matrixServer {
			pkey = data.key
			keyID = data.keyID
			break
		}
	}
	if pkey == nil {
		return keys, nil
	}

	keys.ServerName = matrixServer
	keys.ValidUntilTS = spec.AsTimestamp(time.Now().Add(10 * time.Hour))
	publicKey := pkey.Public().(ed25519.PublicKey)
	keys.VerifyKeys = map[gomatrixserverlib.KeyID]gomatrixserverlib.VerifyKey{
		keyID: {
			Key: spec.Base64Bytes(publicKey),
		},
	}
	toSign, err := json.Marshal(keys.ServerKeyFields)
	if err != nil {
		return keys, err
	}

	keys.Raw, err = gomatrixserverlib.SignJSON(
		string(matrixServer), keyID, pkey, toSign,
	)
	if err != nil {
		return keys, err
	}

	return keys, nil
}

func (f *fedClient) MakeJoin(ctx context.Context, origin, s spec.ServerName, roomID, userID string) (res fclient.RespMakeJoin, err error) {
	f.fedClientMutex.Lock()
	defer f.fedClientMutex.Unlock()
	for _, r := range f.allowJoins {
		if r.ID == roomID {
			senderIDString := userID
			res.RoomVersion = r.Version
			res.JoinEvent = gomatrixserverlib.ProtoEvent{
				SenderID:   senderIDString,
				RoomID:     roomID,
				Type:       "m.room.member",
				StateKey:   &senderIDString,
				Content:    json.RawMessage([]byte(`{"membership":"join"}`)),
				PrevEvents: r.ForwardExtremities(),
			}
			var needed gomatrixserverlib.StateNeeded
			needed, err = gomatrixserverlib.StateNeededForProtoEvent(&res.JoinEvent)
			if err != nil {
				f.t.Errorf("StateNeededForEventBuilder: %v", err)
				return
			}
			res.JoinEvent.AuthEvents = r.MustGetAuthEventRefsForEvent(f.t, needed)
			return
		}
	}
	return
}
func (f *fedClient) SendJoin(ctx context.Context, origin, s spec.ServerName, event gomatrixserverlib.PDU) (res fclient.RespSendJoin, err error) {
	f.fedClientMutex.Lock()
	defer f.fedClientMutex.Unlock()
	for _, r := range f.allowJoins {
		if r.ID == event.RoomID().String() {
			r.InsertEvent(f.t, &types.HeaderedEvent{PDU: event})
			f.t.Logf("Join event: %v", event.EventID())
			res.StateEvents = types.NewEventJSONsFromHeaderedEvents(r.CurrentState())
			res.AuthEvents = types.NewEventJSONsFromHeaderedEvents(r.Events())
		}
	}
	return
}

func (f *fedClient) SendTransaction(ctx context.Context, t gomatrixserverlib.Transaction) (res fclient.RespSend, err error) {
	f.fedClientMutex.Lock()
	defer f.fedClientMutex.Unlock()
	for _, edu := range t.EDUs {
		if edu.Type == spec.MDeviceListUpdate {
			f.sentTxn = true
		}
	}
	f.t.Logf("got /send")
	return
}

// Regression test to make sure that /send_join is updating the destination hosts synchronously and
// isn't relying on the roomserver.
func TestFederationAPIJoinThenKeyUpdate(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		testFederationAPIJoinThenKeyUpdate(t, testOpts)
	})
}

func testFederationAPIJoinThenKeyUpdate(t *testing.T, testOpts test.DependancyOption) {
	ctx, svc, cfg := testrig.Init(t, testOpts)
	defer svc.Stop(ctx)

	cm := sqlutil.NewConnectionManager(svc)
	caches, err := cacheutil.NewCache(&cfg.Global.Cache)
	if err != nil {
		t.Fatalf("failed to create a cache: %v", err)
	}
	qm := queueutil.NewQueueManager(svc)

	am, err := actorutil.NewManager(ctx, &cfg.Global.Actors, qm)
	if err != nil {
		t.Fatalf("failed to create an actor manager: %v", err)
	}

	cfg.FederationAPI.PreferDirectFetch = true
	cfg.FederationAPI.KeyPerspectives = nil

	serverA := spec.ServerName("server.a")
	serverAKeyID := gomatrixserverlib.KeyID("ed25519:servera")
	serverAPrivKey := test.PrivateKeyA
	creator := test.NewUser(t, test.WithSigningServer(serverA, serverAKeyID, serverAPrivKey))

	myServer := cfg.Global.ServerName
	myServerKeyID := cfg.Global.KeyID
	myServerPrivKey := cfg.Global.PrivateKey
	joiningUser := test.NewUser(t, test.WithSigningServer(myServer, myServerKeyID, myServerPrivKey))
	fmt.Printf("creator: %v joining user: %v\n", creator.ID, joiningUser.ID)
	room := test.NewRoom(t, creator)

	roomID, err := spec.NewRoomID(room.ID)
	if err != nil {
		t.Fatalf("Invalid room ID: %q", roomID)
	}

	fedRSApi := &fedRoomserverAPI{
		inputRoomEvents: func(ctx context.Context, req *rsapi.InputRoomEventsRequest, res *rsapi.InputRoomEventsResponse) {
			if req.Asynchronous {
				t.Errorf("InputRoomEvents from PerformJoin MUST be synchronous")
			}
		},
		queryRoomsForUser: func(ctx context.Context, userID spec.UserID, desiredMembership string) ([]spec.RoomID, error) {
			if userID.String() == joiningUser.ID && desiredMembership == "join" {
				return []spec.RoomID{*roomID}, nil
			}
			return nil, fmt.Errorf("unexpected queryRoomsForUser: %v, %v", userID, desiredMembership)
		},
	}
	fc := &fedClient{
		allowJoins: []*test.Room{room},
		t:          t,
		keys: map[spec.ServerName]struct {
			key   ed25519.PrivateKey
			keyID gomatrixserverlib.KeyID
		}{
			serverA: {
				key:   serverAPrivKey,
				keyID: serverAKeyID,
			},
			myServer: {
				key:   myServerPrivKey,
				keyID: myServerKeyID,
			},
		},
	}
	fsapi := federationapi.NewInternalAPI(ctx, cfg, cm, qm, am, fc, fedRSApi, caches, nil, false, nil)

	var resp api.PerformJoinResponse
	fsapi.PerformJoin(ctx, &api.PerformJoinRequest{
		RoomID:      room.ID,
		UserID:      joiningUser.ID,
		ServerNames: []spec.ServerName{serverA},
	}, &resp)
	if resp.JoinedVia != serverA {
		t.Errorf("PerformJoin: joined via %v want %v", resp.JoinedVia, serverA)
	}
	if resp.LastError != nil {
		t.Fatalf("PerformJoin: returned error: %+v", *resp.LastError)
	}

	// Inject a keyserver key change event and ensure we try to send it out. If we don't, then the
	// federationapi is incorrectly waiting for an output room event to arrive to update the joined
	// hosts table.
	key := userapi.DeviceMessage{
		Type: userapi.TypeDeviceKeyUpdate,
		DeviceKeys: &userapi.DeviceKeys{
			UserID:      joiningUser.ID,
			DeviceID:    "MY_DEVICE",
			DisplayName: "BLARGLE",
			KeyJSON:     []byte(`{}`),
		},
	}

	msg := &testrig.QMsg{
		Header: map[string]string{
			constants.UserID: key.UserID,
		},
		Data: key,
	}

	err = testrig.MustPublishMsgs(ctx, t, &cfg.KeyServer.Queues.OutputKeyChangeEvent, qm, msg)
	if err != nil {
		t.Fatalf("MustPublishMsgs: failed to publish message: %s", err)
	}
	time.Sleep(500 * time.Millisecond)
	fc.fedClientMutex.Lock()
	defer fc.fedClientMutex.Unlock()
	if !fc.sentTxn {
		t.Fatalf("did not send device list update")
	}
}

// Tests that event IDs with '/' in them (escaped as %2F) are correctly passed to the right handler and don't 404.
// Relevant for v3 rooms and a cause of flakey sytests as the IDs are randomly generated.
func TestRoomsV3URLEscapeDoNot404(t *testing.T) {
	testCases := []struct {
		roomVer   gomatrixserverlib.RoomVersion
		eventJSON string
	}{
		{
			eventJSON: `{"auth_events":[["$Nzfbrhc3oaYVKzGM:localhost",{"sha256":"BCBHOgB4qxLPQkBd6th8ydFSyqjth/LF99VNjYffOQ0"}],["$EZzkD2BH1Gtm5v1D:localhost",{"sha256":"3dLUnDBs8/iC5DMw/ydKtmAqVZtzqqtHpsjsQPk7GJA"}]],"content":{"body":"Test Message"},"depth":11,"event_id":"$mGiPO3oGjQfCkIUw:localhost","hashes":{"sha256":"h+t+4DwIBC9UNyJ3jzyAQAAl4H3yQHVuHrm2S1JZizU"},"origin":"localhost","origin_server_ts":0,"prev_events":[["$tFr64vpiSHdLU0Qr:localhost",{"sha256":"+R07ZrIs4c4tjPFE+tmcYIGUfeLGFI/4e0OITb9uEcM"}]],"room_id":"!roomid:localhost","sender":"@userid:localhost","signatures":{"localhost":{"ed25519:auto":"LYFr/rW9m5/7UKBQMF5qWnG82He4VGsRESUgDmvkn5DrJRyS4TLL/7zl0Lymn3pa3q2yaTO74LQX/CRotqG1BA"}},"type":"m.room.message"}`,
			roomVer:   gomatrixserverlib.RoomVersionV1,
		},
		// single / (handlers which do not UseEncodedPath will fail this test)
		// EventID: $0SFh2WJbjBs3OT+E0yl95giDKo/3Zp52HsHUUk4uPyg
		{
			eventJSON: `{"auth_events":["$x4MKEPRSF6OGlo0qpnsP3BfSmYX5HhVlykOsQH3ECyg","$BcEcbZnlFLB5rxSNSZNBn6fO3jU/TKAJ79wfKyCQLiU"],"content":{"body":"Test Message"},"depth":8,"hashes":{"sha256":"dfK0MBn1RZZqCVJqWsn/MGY7QJHjQcwqF0unOonLCTU"},"origin":"localhost","origin_server_ts":0,"prev_events":["$1SwcZ1XY/Y8yKLjP4DzAOHN5WFBcDAZxb5vFDnW2ubA"],"room_id":"!roomid:localhost","sender":"@userid:localhost","signatures":{"localhost":{"ed25519:auto":"INOjuWMg+GmFkUpmzhMB0bqLNs73mSvwldY1ftYIQ/B3lD9soD2OMG3AF+wgZW/I8xqzY4DOHfbnbUeYPf67BA"}},"type":"m.room.message"}`,
			roomVer:   gomatrixserverlib.RoomVersionV3,
		},
		// multiple /
		// EventID: $OzENBCuVv/fnRAYCeQudIon/84/V5pxtEjQMTgi3emk
		{
			eventJSON: `{"auth_events":["$x4MKEPRSF6OGlo0qpnsP3BfSmYX5HhVlykOsQH3ECyg","$BcEcbZnlFLB5rxSNSZNBn6fO3jU/TKAJ79wfKyCQLiU"],"content":{"body":"Test Message"},"depth":2,"hashes":{"sha256":"U5+WsiJAhiEM88J8HTjuUjPImVGVzDFD3v/WS+jb2f0"},"origin":"localhost","origin_server_ts":0,"prev_events":["$BcEcbZnlFLB5rxSNSZNBn6fO3jU/TKAJ79wfKyCQLiU"],"room_id":"!roomid:localhost","sender":"@userid:localhost","signatures":{"localhost":{"ed25519:auto":"tKS469e9+wdWPEKB/LbBJWQ8vfOOdKgTWER5IwbSAH1CxmLvkCziUsgVu85zfzDSLoUi5mU5FHLiMTC6P/qICw"}},"type":"m.room.message"}`,
			roomVer:   gomatrixserverlib.RoomVersionV3,
		},
		// two slashes (handlers which clean paths before UseEncodedPath will fail this test)
		// EventID: $EmwNBlHoSOVmCZ1cM//yv/OvxB6r4OFEIGSJea7+Amk
		{
			eventJSON: `{"auth_events":["$x4MKEPRSF6OGlo0qpnsP3BfSmYX5HhVlykOsQH3ECyg","$BcEcbZnlFLB5rxSNSZNBn6fO3jU/TKAJ79wfKyCQLiU"],"content":{"body":"Test Message"},"depth":3917,"hashes":{"sha256":"cNAWtlHIegrji0mMA6x1rhpYCccY8W1NsWZqSpJFhjs"},"origin":"localhost","origin_server_ts":0,"prev_events":["$4GDB0bVjkWwS3G4noUZCq5oLWzpBYpwzdMcf7gj24CI"],"room_id":"!roomid:localhost","sender":"@userid:localhost","signatures":{"localhost":{"ed25519:auto":"NKym6Kcy3u9mGUr21Hjfe3h7DfDilDhN5PqztT0QZ4NTZ+8Y7owseLolQVXp+TvNjecvzdDywsXXVvGiuQiWAQ"}},"type":"m.room.message"}`,
			roomVer:   gomatrixserverlib.RoomVersionV3,
		},
	}

	ctx, svc, cfg := testrig.Init(t)
	defer svc.Stop(ctx)

	routers := httputil.NewRouters()

	_, privKey, _ := ed25519.GenerateKey(nil)
	cfg.Global.KeyID = gomatrixserverlib.KeyID("ed25519:auto")
	cfg.Global.ServerName = spec.ServerName("localhost")
	cfg.Global.PrivateKey = privKey
	keyRing := &test.NopJSONVerifier{}
	qm := queueutil.NewQueueManager(svc)
	// TODO: This is pretty fragile, as if anything calls anything on these nils this test will break.
	// Unfortunately, it makes little sense to instantiate these dependencies when we just want to test routing.
	federationapi.AddPublicRoutes(ctx, routers, cfg, qm, nil, nil, keyRing, nil, &internal.FederationInternalAPI{}, cacheutil.DisableMetrics)
	baseURL, cancel := test.ListenAndServe(ctx, t, routers.Federation, true)
	defer cancel()
	serverName := spec.ServerName(strings.TrimPrefix(baseURL, "https://"))

	fedCli := fclient.NewFederationClient(
		cfg.Global.SigningIdentities(),
		fclient.WithSkipVerify(true),
	)

	for _, tc := range testCases {
		ev, err := gomatrixserverlib.MustGetRoomVersion(tc.roomVer).NewEventFromTrustedJSON([]byte(tc.eventJSON), false)
		if err != nil {
			t.Errorf("failed to parse event: %s", err)
		}
		invReq, err := fclient.NewInviteV2Request(ev, nil)
		if err != nil {
			t.Errorf("failed to create invite v2 request: %s", err)
			continue
		}
		_, err = fedCli.SendInviteV2(ctx, cfg.Global.ServerName, serverName, invReq)
		if err == nil {
			t.Errorf("expected an error, got none")
			continue
		}
		gerr, ok := err.(gomatrix.HTTPError)
		if !ok {
			t.Errorf("failed to cast response error as gomatrix.HTTPError: %s", err)
			continue
		}
		t.Logf("Error: %+v", gerr)
		if gerr.Code == 404 {
			t.Errorf("invite event resulted in a 404")
		}
	}
}

func TestNotaryServer(t *testing.T) {
	testCases := []struct {
		name          string
		httpBody      string
		pubKeyRequest *gomatrixserverlib.PublicKeyNotaryLookupRequest
		validateFunc  func(t *testing.T, response util.JSONResponse)
	}{
		{
			name: "empty httpBody",
			validateFunc: func(t *testing.T, resp util.JSONResponse) {
				assert.Equal(t, http.StatusBadRequest, resp.Code)
				nk, ok := resp.JSON.(spec.MatrixError)
				assert.True(t, ok)
				assert.Equal(t, spec.ErrorBadJSON, nk.ErrCode)
			},
		},
		{
			name:     "valid but empty httpBody",
			httpBody: "{}",
			validateFunc: func(t *testing.T, resp util.JSONResponse) {
				want := util.JSONResponse{
					Code: http.StatusOK,
					JSON: routing.NotaryKeysResponse{ServerKeys: []json.RawMessage{}},
				}
				assert.Equal(t, want, resp)
			},
		},
		{
			name:     "request all keys using an empty criteria",
			httpBody: `{"server_keys":{"servera":{}}}`,
			validateFunc: func(t *testing.T, resp util.JSONResponse) {
				assert.Equal(t, http.StatusOK, resp.Code)
				nk, ok := resp.JSON.(routing.NotaryKeysResponse)
				assert.True(t, ok)
				assert.Equal(t, "servera", gjson.GetBytes(nk.ServerKeys[0], "server_name").Str)
				assert.True(t, gjson.GetBytes(nk.ServerKeys[0], "verify_keys.ed25519:someID").Exists())
			},
		},
		{
			name:     "request all keys using null as the criteria",
			httpBody: `{"server_keys":{"servera":null}}`,
			validateFunc: func(t *testing.T, resp util.JSONResponse) {
				assert.Equal(t, http.StatusOK, resp.Code)
				nk, ok := resp.JSON.(routing.NotaryKeysResponse)
				assert.True(t, ok)
				assert.Equal(t, "servera", gjson.GetBytes(nk.ServerKeys[0], "server_name").Str)
				assert.True(t, gjson.GetBytes(nk.ServerKeys[0], "verify_keys.ed25519:someID").Exists())
			},
		},
		{
			name:     "request specific key",
			httpBody: `{"server_keys":{"servera":{"ed25519:someID":{}}}}`,
			validateFunc: func(t *testing.T, resp util.JSONResponse) {
				assert.Equal(t, http.StatusOK, resp.Code)
				nk, ok := resp.JSON.(routing.NotaryKeysResponse)
				assert.True(t, ok)
				assert.Equal(t, "servera", gjson.GetBytes(nk.ServerKeys[0], "server_name").Str)
				assert.True(t, gjson.GetBytes(nk.ServerKeys[0], "verify_keys.ed25519:someID").Exists())
			},
		},
		{
			name:     "request multiple servers",
			httpBody: `{"server_keys":{"servera":{"ed25519:someID":{}},"serverb":{"ed25519:someID":{}}}}`,
			validateFunc: func(t *testing.T, resp util.JSONResponse) {
				assert.Equal(t, http.StatusOK, resp.Code)
				nk, ok := resp.JSON.(routing.NotaryKeysResponse)
				assert.True(t, ok)
				wantServers := map[string]struct{}{
					"servera": {},
					"serverb": {},
				}
				for _, js := range nk.ServerKeys {
					serverName := gjson.GetBytes(js, "server_name").Str
					_, ok = wantServers[serverName]
					assert.True(t, ok, "unexpected servername: %s", serverName)
					delete(wantServers, serverName)
					assert.True(t, gjson.GetBytes(js, "verify_keys.ed25519:someID").Exists())
				}
				if len(wantServers) > 0 {
					t.Fatalf("expected response to also contain: %#v", wantServers)
				}
			},
		},
	}

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		cm := sqlutil.NewConnectionManager(svc)
		caches, err := cacheutil.NewCache(&cfg.Global.Cache)
		if err != nil {
			t.Fatalf("failed to create a cache: %v", err)
		}
		qm := queueutil.NewQueueManager(svc)

		am, err := actorutil.NewManager(ctx, &cfg.Global.Actors, qm)
		if err != nil {
			t.Fatalf("failed to create an actor manager: %v", err)
		}

		fc := &fedClient{
			keys: map[spec.ServerName]struct {
				key   ed25519.PrivateKey
				keyID gomatrixserverlib.KeyID
			}{
				"servera": {
					key:   test.PrivateKeyA,
					keyID: "ed25519:someID",
				},
				"serverb": {
					key:   test.PrivateKeyB,
					keyID: "ed25519:someID",
				},
			},
		}

		fedAPI := federationapi.NewInternalAPI(ctx, cfg, cm, qm, am, fc, nil, caches, nil, true, nil)

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.httpBody))
				req.Host = string(cfg.Global.ServerName)

				resp := routing.NotaryKeys(req, &cfg.FederationAPI, fedAPI, tc.pubKeyRequest)
				// assert that we received the expected response
				tc.validateFunc(t, resp)
			})
		}

	})
}
