package syncapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/producers"
	"github.com/antinvestor/matrix/internal/actorutil"
	"github.com/antinvestor/matrix/internal/cacheutil"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver"
	rsapi "github.com/antinvestor/matrix/roomserver/api"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/syncapi/routing"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
)

type syncRoomserverAPI struct {
	rsapi.SyncRoomserverAPI
	rooms []*test.Room
}

func (s *syncRoomserverAPI) QueryUserIDForSender(ctx context.Context, roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(senderID), true)
}

func (s *syncRoomserverAPI) QuerySenderIDForUser(ctx context.Context, roomID spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	senderID := spec.SenderID(userID.String())
	return &senderID, nil
}

func (s *syncRoomserverAPI) QueryLatestEventsAndState(ctx context.Context, req *rsapi.QueryLatestEventsAndStateRequest, res *rsapi.QueryLatestEventsAndStateResponse) error {
	var room *test.Room
	for _, r := range s.rooms {
		if r.ID == req.RoomID {
			room = r
			break
		}
	}
	if room == nil {
		res.RoomExists = false
		return nil
	}
	res.RoomVersion = room.Version
	return nil // TODO: return state
}

func (s *syncRoomserverAPI) QuerySharedUsers(ctx context.Context, req *rsapi.QuerySharedUsersRequest, res *rsapi.QuerySharedUsersResponse) error {
	res.UserIDsToCount = make(map[string]int)
	return nil
}
func (s *syncRoomserverAPI) QueryBulkStateContent(ctx context.Context, req *rsapi.QueryBulkStateContentRequest, res *rsapi.QueryBulkStateContentResponse) error {
	return nil
}

func (s *syncRoomserverAPI) QueryMembershipForUser(ctx context.Context, req *rsapi.QueryMembershipForUserRequest, res *rsapi.QueryMembershipForUserResponse) error {
	res.IsRoomForgotten = false
	res.RoomExists = true
	return nil
}

func (s *syncRoomserverAPI) QueryMembershipAtEvent(
	ctx context.Context,
	roomID spec.RoomID,
	eventIDs []string,
	senderID spec.SenderID,
) (map[string]*rstypes.HeaderedEvent, error) {
	return map[string]*rstypes.HeaderedEvent{}, nil
}

type syncUserAPI struct {
	userapi.SyncUserAPI
	accounts []userapi.Device
}

func (s *syncUserAPI) QueryAccessToken(ctx context.Context, req *userapi.QueryAccessTokenRequest, res *userapi.QueryAccessTokenResponse) error {
	for _, acc := range s.accounts {
		if acc.AccessToken == req.AccessToken {
			res.Device = &acc
			return nil
		}
	}
	res.Err = "unknown user"
	return nil
}

func (s *syncUserAPI) QueryKeyChanges(ctx context.Context, req *userapi.QueryKeyChangesRequest, res *userapi.QueryKeyChangesResponse) error {
	return nil
}

func (s *syncUserAPI) QueryOneTimeKeys(ctx context.Context, req *userapi.QueryOneTimeKeysRequest, res *userapi.QueryOneTimeKeysResponse) error {
	return nil
}

func (s *syncUserAPI) PerformLastSeenUpdate(ctx context.Context, req *userapi.PerformLastSeenUpdateRequest, res *userapi.PerformLastSeenUpdateResponse) error {
	return nil
}

func TestSyncAPIAccessTokens(t *testing.T) {

	user := test.NewUser(t)
	room := test.NewRoom(t, user)
	alice := userapi.Device{
		ID:          "ALICEID",
		UserID:      user.ID,
		AccessToken: "ALICE_BEARER_TOKEN",
		DisplayName: "Alice",
		AccountType: userapi.AccountTypeUser,
	}

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		routers := httputil.NewRouters()
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

		msgs := toQueueMsgs(t, room.Events()...)

		AddPublicRoutes(ctx, routers, cfg, cm, qm, am, &syncUserAPI{accounts: []userapi.Device{alice}}, &syncRoomserverAPI{rooms: []*test.Room{room}}, caches, cacheutil.DisableMetrics)

		err = testrig.MustPublishMsgs(ctx, t, &cfg.SyncAPI.Queues.OutputRoomEvent, qm, msgs...)
		if err != nil {
			t.Fatalf("failed to publish events: %v", err)
		}

		testCases := []struct {
			name            string
			req             *http.Request
			wantCode        int
			wantJoinedRooms []string
		}{
			{
				name: "missing access token",
				req: test.NewRequest(t, "GET", "/_matrix/client/v3/sync", test.WithQueryParams(map[string]string{
					"timeout": "0",
				})),
				wantCode: 401,
			},
			{
				name: "unknown access token",
				req: test.NewRequest(t, "GET", "/_matrix/client/v3/sync", test.WithQueryParams(map[string]string{
					"access_token": "foo",
					"timeout":      "0",
				})),
				wantCode: 401,
			},
			{
				name: "valid access token",
				req: test.NewRequest(t, "GET", "/_matrix/client/v3/sync", test.WithQueryParams(map[string]string{
					"access_token": alice.AccessToken,
					"timeout":      "0",
				})),
				wantCode:        200,
				wantJoinedRooms: []string{room.ID},
			},
		}

		syncUntil(ctx, t, routers, alice.AccessToken, false, func(syncBody string) bool {
			// wait for the last sent eventID to come down sync
			path := fmt.Sprintf(`rooms.join.%s.timeline.events.#(event_id=="%s")`, room.ID, room.Events()[len(room.Events())-1].EventID())
			return gjson.Get(syncBody, path).Exists()
		})

		for _, tc := range testCases {
			w := httptest.NewRecorder()
			routers.Client.ServeHTTP(w, tc.req)
			if w.Code != tc.wantCode {
				t.Fatalf("%s: got HTTP %d want %d", tc.name, w.Code, tc.wantCode)
			}
			if tc.wantJoinedRooms != nil {
				var res types.Response
				err = json.NewDecoder(w.Body).Decode(&res)
				if err != nil {
					t.Fatalf("%s: failed to decode response body: %s", tc.name, err)
				}
				if len(res.Rooms.Join) != len(tc.wantJoinedRooms) {
					t.Errorf("%s: got %v joined rooms, want %v.\nResponse: %+v", tc.name, len(res.Rooms.Join), len(tc.wantJoinedRooms), res)
				}
				t.Logf("res: %+v", res.Rooms.Join[room.ID])

				gotEventIDs := make([]string, len(res.Rooms.Join[room.ID].Timeline.Events))
				for i, ev := range res.Rooms.Join[room.ID].Timeline.Events {
					gotEventIDs[i] = ev.EventID
				}
				test.AssertEventIDsEqual(t, gotEventIDs, room.Events())
			}
		}
	})
}

func TestSyncAPIEventFormatPowerLevels(t *testing.T) {

	user := test.NewUser(t)
	setRoomVersion := func(t *testing.T, r *test.Room) { r.Version = gomatrixserverlib.RoomVersionPseudoIDs }
	room := test.NewRoom(t, user, setRoomVersion)
	alice := userapi.Device{
		ID:          "ALICEID",
		UserID:      user.ID,
		AccessToken: "ALICE_BEARER_TOKEN",
		DisplayName: "Alice",
		AccountType: userapi.AccountTypeUser,
	}

	room.CreateAndInsert(t, user, spec.MRoomPowerLevels, gomatrixserverlib.PowerLevelContent{
		Users: map[string]int64{
			user.ID: 100,
		},
	}, test.WithStateKey(""))

	routers := httputil.NewRouters()

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

		msgs := toQueueMsgs(t, room.Events()...)
		AddPublicRoutes(ctx, routers, cfg, cm, qm, am, &syncUserAPI{accounts: []userapi.Device{alice}}, &syncRoomserverAPI{rooms: []*test.Room{room}}, caches, cacheutil.DisableMetrics)

		err = testrig.MustPublishMsgs(ctx, t, &cfg.SyncAPI.Queues.OutputRoomEvent, qm, msgs...)
		if err != nil {
			t.Fatalf("failed to publish events: %v", err)
		}

		testCases := []struct {
			name            string
			wantCode        int
			wantJoinedRooms []string
			eventFormat     synctypes.ClientEventFormat
		}{
			{
				name:            "Client format",
				wantCode:        200,
				wantJoinedRooms: []string{room.ID},
				eventFormat:     synctypes.FormatSync,
			},
			{
				name:            "Federation format",
				wantCode:        200,
				wantJoinedRooms: []string{room.ID},
				eventFormat:     synctypes.FormatSyncFederation,
			},
		}

		syncUntil(ctx, t, routers, alice.AccessToken, false, func(syncBody string) bool {
			// wait for the last sent eventID to come down sync
			path := fmt.Sprintf(`rooms.join.%s.timeline.events.#(event_id=="%s")`, room.ID, room.Events()[len(room.Events())-1].EventID())
			return gjson.Get(syncBody, path).Exists()
		})

		for _, tc := range testCases {
			format := ""
			if tc.eventFormat == synctypes.FormatSyncFederation {
				format = "federation"
			}

			w := httptest.NewRecorder()
			routers.Client.ServeHTTP(w, test.NewRequest(t, "GET", "/_matrix/client/v3/sync", test.WithQueryParams(map[string]string{
				"access_token": alice.AccessToken,
				"timeout":      "0",
				"filter":       fmt.Sprintf(`{"event_format":"%s"}`, format),
			})))
			if w.Code != tc.wantCode {
				t.Fatalf("%s: got HTTP %d want %d", tc.name, w.Code, tc.wantCode)
			}
			if tc.wantJoinedRooms != nil {
				var res types.Response
				if err = json.NewDecoder(w.Body).Decode(&res); err != nil {
					t.Fatalf("%s: failed to decode response body: %s", tc.name, err)
				}
				if len(res.Rooms.Join) != len(tc.wantJoinedRooms) {
					t.Errorf("%s: got %v joined rooms, want %v.\nResponse: %+v", tc.name, len(res.Rooms.Join), len(tc.wantJoinedRooms), res)
				}
				t.Logf("res: %+v", res.Rooms.Join[room.ID])

				gotEventIDs := make([]string, len(res.Rooms.Join[room.ID].Timeline.Events))
				for i, ev := range res.Rooms.Join[room.ID].Timeline.Events {
					gotEventIDs[i] = ev.EventID
				}
				test.AssertEventIDsEqual(t, gotEventIDs, room.Events())

				event := room.CreateAndInsert(t, user, spec.MRoomPowerLevels, gomatrixserverlib.PowerLevelContent{
					Users: map[string]int64{
						user.ID:                100,
						"@otheruser:localhost": 50,
					},
				}, test.WithStateKey(""))

				msgs = toQueueMsgs(t, event)
				err = testrig.MustPublishMsgs(ctx, t, &cfg.SyncAPI.Queues.OutputRoomEvent, qm, msgs...)
				if err != nil {
					t.Fatalf("failed to publish events: %v", err)
				}

				syncUntil(ctx, t, routers, alice.AccessToken, false, func(syncBody string) bool {
					// wait for the last sent eventID to come down sync
					path := fmt.Sprintf(`rooms.join.%s.timeline.events.#(event_id=="%s")`, room.ID, room.Events()[len(room.Events())-1].EventID())
					return gjson.Get(syncBody, path).Exists()
				})

				since := res.NextBatch.String()
				w := httptest.NewRecorder()
				routers.Client.ServeHTTP(w, test.NewRequest(t, "GET", "/_matrix/client/v3/sync", test.WithQueryParams(map[string]string{
					"access_token": alice.AccessToken,
					"timeout":      "0",
					"filter":       fmt.Sprintf(`{"event_format":"%s"}`, format),
					"since":        since,
				})))
				if w.Code != 200 {
					t.Errorf("since=%s got HTTP %d want 200", since, w.Code)
				}

				res = *types.NewResponse()
				if err := json.NewDecoder(w.Body).Decode(&res); err != nil {
					t.Errorf("failed to decode response body: %s", err)
				}
				if len(res.Rooms.Join) != 1 {
					t.Fatalf("since=%s got %d joined rooms, want 1", since, len(res.Rooms.Join))
				}
				gotEventIDs = make([]string, len(res.Rooms.Join[room.ID].Timeline.Events))
				for j, ev := range res.Rooms.Join[room.ID].Timeline.Events {
					gotEventIDs[j] = ev.EventID
					if ev.Type == spec.MRoomPowerLevels {
						content := gomatrixserverlib.PowerLevelContent{}
						err := json.Unmarshal(ev.Content, &content)
						if err != nil {
							t.Errorf("failed to unmarshal power level content: %s", err)
						}
						otherUserLevel := content.UserLevel("@otheruser:localhost")
						if otherUserLevel != 50 {
							t.Errorf("Expected user PL of %d but got %d", 50, otherUserLevel)
						}
					}
				}
				events := []*rstypes.HeaderedEvent{room.Events()[len(room.Events())-1]}
				test.AssertEventIDsEqual(t, gotEventIDs, events)
			}
		}

	})
}

// Tests what happens when we create a room and then /sync before all events from /createRoom have
// been sent to the syncapi
func TestSyncAPICreateRoomSyncEarly(t *testing.T) {

	// t.Skip("Skipped, possibly fixed")
	user := test.NewUser(t)
	room := test.NewRoom(t, user)
	alice := userapi.Device{
		ID:          "ALICEID",
		UserID:      user.ID,
		AccessToken: "ALICE_BEARER_TOKEN",
		DisplayName: "Alice",
		AccountType: userapi.AccountTypeUser,
	}

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		routers := httputil.NewRouters()
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

		// order is:
		// m.room.create
		// m.room.member
		// m.room.power_levels
		// m.room.join_rules
		// m.room.history_visibility
		msgs := toQueueMsgs(t, room.Events()...)
		sinceTokens := make([]string, len(msgs))
		AddPublicRoutes(ctx, routers, cfg, cm, qm, am, &syncUserAPI{accounts: []userapi.Device{alice}}, &syncRoomserverAPI{rooms: []*test.Room{room}}, caches, cacheutil.DisableMetrics)

		for i, msg := range msgs {

			err = testrig.MustPublishMsgs(ctx, t, &cfg.SyncAPI.Queues.OutputRoomEvent, qm, msg)
			if err != nil {
				t.Fatalf("failed to publish events: %v", err)
			}

			time.Sleep(100 * time.Millisecond)
			w := httptest.NewRecorder()
			routers.Client.ServeHTTP(w, test.NewRequest(t, "GET", "/_matrix/client/v3/sync", test.WithQueryParams(map[string]string{
				"access_token": alice.AccessToken,
				"timeout":      "0",
			})))
			if w.Code != 200 {
				t.Errorf("got HTTP %d want 200", w.Code)
				continue
			}
			var res types.Response
			if err = json.NewDecoder(w.Body).Decode(&res); err != nil {
				t.Errorf("failed to decode response body: %s", err)
			}
			sinceTokens[i] = res.NextBatch.String()
			if i == 0 { // create event does not produce a room section
				if res.Rooms != nil && len(res.Rooms.Join) != 0 {
					t.Fatalf("i=%v got %d joined rooms, want 0", i, len(res.Rooms.Join))
				}
			} else { // we should have that room somewhere
				if res.Rooms != nil && len(res.Rooms.Join) != 1 {
					t.Fatalf("i=%v got %d joined rooms, want 1", i, len(res.Rooms.Join))
				}
			}
		}

		// sync with no token "" and with the penultimate token and this should neatly return room events in the timeline block
		sinceTokens = append([]string{""}, sinceTokens[:len(sinceTokens)-1]...)

		for i, since := range sinceTokens {
			w := httptest.NewRecorder()
			routers.Client.ServeHTTP(w, test.NewRequest(t, "GET", "/_matrix/client/v3/sync", test.WithQueryParams(map[string]string{
				"access_token": alice.AccessToken,
				"timeout":      "0",
				"since":        since,
			})))
			if w.Code != 200 {
				t.Errorf("since=%s got HTTP %d want 200", since, w.Code)
			}
			var res types.Response
			if err = json.NewDecoder(w.Body).Decode(&res); err != nil {
				t.Errorf("failed to decode response body: %s", err)
			}
			if len(res.Rooms.Join) != 1 {
				t.Fatalf("since=%s got %d joined rooms, want 1", since, len(res.Rooms.Join))
			}
			// t.Logf("since=%s res state:%+v res timeline:%+v", since, res.Rooms.Join[room.ID].State.Events, res.Rooms.Join[room.ID].Timeline.Events)
			gotEventIDs := make([]string, len(res.Rooms.Join[room.ID].Timeline.Events))
			for j, ev := range res.Rooms.Join[room.ID].Timeline.Events {
				gotEventIDs[j] = ev.EventID
			}

			if i < 2 {
				t.Logf("since; matching %v for got : %v and want : %v", since, len(gotEventIDs), len(room.Events()))
				test.AssertEventIDsEqual(t, gotEventIDs, room.Events())
			} else {
				t.Logf("since; matching %v for got : %v and want : %v", since, len(gotEventIDs), len(room.Events()[i:]))
				test.AssertEventIDsEqual(t, gotEventIDs, room.Events()[i:])
			}

		}
	})
}

// Test that if we hit /sync we get back presence: online, regardless of whether messages get delivered
// via NATS. Regression test for a flakey test "User sees their own presence in a sync"
func TestSyncAPIUpdatePresenceImmediately(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		testSyncAPIUpdatePresenceImmediately(t, testOpts)
	})
}

func testSyncAPIUpdatePresenceImmediately(t *testing.T, testOpts test.DependancyOption) {
	user := test.NewUser(t)
	alice := userapi.Device{
		ID:          "ALICEID",
		UserID:      user.ID,
		AccessToken: "ALICE_BEARER_TOKEN",
		DisplayName: "Alice",
		AccountType: userapi.AccountTypeUser,
	}

	ctx, svc, cfg := testrig.Init(t, testOpts)
	defer svc.Stop(ctx)

	routers := httputil.NewRouters()
	cm := sqlutil.NewConnectionManager(svc)
	caches, err := cacheutil.NewCache(&cfg.Global.Cache)
	if err != nil {
		t.Fatalf("failed to create a cache: %v", err)
	}
	cfg.Global.Presence.EnableOutbound = true
	cfg.Global.Presence.EnableInbound = true
	qm := queueutil.NewQueueManager(svc)

	am, err := actorutil.NewManager(ctx, &cfg.Global.Actors, qm)
	if err != nil {
		t.Fatalf("failed to create an actor manager: %v", err)
	}

	AddPublicRoutes(ctx, routers, cfg, cm, qm, am, &syncUserAPI{accounts: []userapi.Device{alice}}, &syncRoomserverAPI{}, caches, cacheutil.DisableMetrics)
	w := httptest.NewRecorder()
	routers.Client.ServeHTTP(w, test.NewRequest(t, "GET", "/_matrix/client/v3/sync", test.WithQueryParams(map[string]string{
		"access_token": alice.AccessToken,
		"timeout":      "0",
		"set_presence": "online",
	})))
	if w.Code != 200 {
		t.Fatalf("got HTTP %d want %d", w.Code, 200)
	}
	var res types.Response
	if err := json.NewDecoder(w.Body).Decode(&res); err != nil {
		t.Errorf("failed to decode response body: %s", err)
	}
	if len(res.Presence.Events) != 1 {
		t.Fatalf("expected 1 presence events, got: %+v", res.Presence.Events)
	}
	if res.Presence.Events[0].Sender != alice.UserID {
		t.Errorf("sender: got %v want %v", res.Presence.Events[0].Sender, alice.UserID)
	}
	if res.Presence.Events[0].Type != "m.presence" {
		t.Errorf("type: got %v want %v", res.Presence.Events[0].Type, "m.presence")
	}
	if gjson.ParseBytes(res.Presence.Events[0].Content).Get("presence").Str != "online" {
		t.Errorf("content: not online,  got %v", res.Presence.Events[0].Content)
	}

}

// This is mainly what Sytest is doing in "test_history_visibility"
func TestMessageHistoryVisibility(t *testing.T) {

	type result struct {
		seeWithoutJoin bool
		seeBeforeJoin  bool
		seeAfterInvite bool
	}

	// create the users
	alice := test.NewUser(t)
	aliceDev := userapi.Device{
		ID:          "ALICEID",
		UserID:      alice.ID,
		AccessToken: "ALICE_BEARER_TOKEN",
		DisplayName: "ALICE",
	}

	bob := test.NewUser(t)

	bobDev := userapi.Device{
		ID:          "BOBID",
		UserID:      bob.ID,
		AccessToken: "BOD_BEARER_TOKEN",
		DisplayName: "BOB",
	}

	// check guest and normal user accounts
	for _, accType := range []userapi.AccountType{userapi.AccountTypeGuest, userapi.AccountTypeUser} {
		testCases := []struct {
			historyVisibility gomatrixserverlib.HistoryVisibility
			wantResult        result
		}{
			{
				historyVisibility: gomatrixserverlib.HistoryVisibilityWorldReadable,
				wantResult: result{
					seeWithoutJoin: true,
					seeBeforeJoin:  true,
					seeAfterInvite: true,
				},
			},
			{
				historyVisibility: gomatrixserverlib.HistoryVisibilityShared,
				wantResult: result{
					seeWithoutJoin: false,
					seeBeforeJoin:  true,
					seeAfterInvite: true,
				},
			},
			{
				historyVisibility: gomatrixserverlib.HistoryVisibilityInvited,
				wantResult: result{
					seeWithoutJoin: false,
					seeBeforeJoin:  false,
					seeAfterInvite: true,
				},
			},
			{
				historyVisibility: gomatrixserverlib.HistoryVisibilityJoined,
				wantResult: result{
					seeWithoutJoin: false,
					seeBeforeJoin:  false,
					seeAfterInvite: false,
				},
			},
		}

		bobDev.AccountType = accType
		userType := "guest"
		if accType == userapi.AccountTypeUser {
			userType = "real user"
		}

		for _, tc := range testCases {
			testname := fmt.Sprintf("%s - %s", tc.historyVisibility, userType)
			t.Run(testname, func(t *testing.T) {

				test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
					ctx, svc, cfg := testrig.Init(t, testOpts)
					defer svc.Stop(ctx)

					cfg.ClientAPI.RateLimiting = config.RateLimiting{Enabled: false}
					routers := httputil.NewRouters()
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

					// Use the actual internal roomserver API
					rsAPI := roomserver.NewInternalAPI(ctx, cfg, cm, qm, caches, am, cacheutil.DisableMetrics)
					rsAPI.SetFederationAPI(ctx, nil, nil)
					AddPublicRoutes(ctx, routers, cfg, cm, qm, am, &syncUserAPI{accounts: []userapi.Device{aliceDev, bobDev}}, rsAPI, caches, cacheutil.DisableMetrics)

					// create a room with the given visibility
					room := test.NewRoom(t, alice, test.RoomHistoryVisibility(tc.historyVisibility))

					// send the events/messages to NATS to create the rooms
					beforeJoinBody := fmt.Sprintf("Before invite in a %s room", tc.historyVisibility)
					beforeJoinEv := room.CreateAndInsert(t, alice, "m.room.message", map[string]interface{}{"body": beforeJoinBody})
					eventsToSend := append(room.Events(), beforeJoinEv)
					if err = rsapi.SendEvents(ctx, rsAPI, rsapi.KindNew, eventsToSend, "test", "test", "test", nil, false); err != nil {
						t.Fatalf("failed to send events: %v", err)
					}
					syncUntil(ctx, t, routers, aliceDev.AccessToken, false,
						func(syncBody string) bool {
							path := fmt.Sprintf(`rooms.join.%s.timeline.events.#(content.body=="%s")`, room.ID, beforeJoinBody)
							return gjson.Get(syncBody, path).Exists()
						},
					)

					// There is only one event, we expect only to be able to see this, if the room is world_readable
					w := httptest.NewRecorder()
					routers.Client.ServeHTTP(w, test.NewRequest(t, "GET", fmt.Sprintf("/_matrix/client/v3/rooms/%s/messages", room.ID), test.WithQueryParams(map[string]string{
						"access_token": bobDev.AccessToken,
						"dir":          "b",
						"filter":       `{"lazy_load_members":true}`, // check that lazy loading doesn't break history visibility
					})))
					if w.Code != 200 {
						t.Logf("%s", w.Body.String())
						t.Fatalf("got HTTP %d want %d", w.Code, 200)
					}
					// We only care about the returned events at this point
					var res struct {
						Chunk []synctypes.ClientEvent `json:"chunk"`
					}
					if err = json.NewDecoder(w.Body).Decode(&res); err != nil {
						t.Errorf("failed to decode response body: %s", err)
					}

					verifyEventVisible(t, tc.wantResult.seeWithoutJoin, beforeJoinEv, res.Chunk)

					// Create invite, a message, join the room and create another message.
					inviteEv := room.CreateAndInsert(t, alice, "m.room.member", map[string]interface{}{"membership": "invite"}, test.WithStateKey(bob.ID))
					afterInviteEv := room.CreateAndInsert(t, alice, "m.room.message", map[string]interface{}{"body": fmt.Sprintf("After invite in a %s room", tc.historyVisibility)})
					joinEv := room.CreateAndInsert(t, bob, "m.room.member", map[string]interface{}{"membership": "join"}, test.WithStateKey(bob.ID))
					afterJoinBody := fmt.Sprintf("After join in a %s room", tc.historyVisibility)
					msgEv := room.CreateAndInsert(t, alice, "m.room.message", map[string]interface{}{"body": afterJoinBody})

					eventsToSend = append([]*rstypes.HeaderedEvent{}, inviteEv, afterInviteEv, joinEv, msgEv)

					if err = rsapi.SendEvents(ctx, rsAPI, rsapi.KindNew, eventsToSend, "test", "test", "test", nil, false); err != nil {
						t.Fatalf("failed to send events: %v", err)
					}
					syncUntil(ctx, t, routers, aliceDev.AccessToken, false,
						func(syncBody string) bool {
							path := fmt.Sprintf(`rooms.join.%s.timeline.events.#(content.body=="%s")`, room.ID, afterJoinBody)
							return gjson.Get(syncBody, path).Exists()
						},
					)

					// Verify the messages after/before invite are visible or not
					w = httptest.NewRecorder()
					routers.Client.ServeHTTP(w, test.NewRequest(t, "GET", fmt.Sprintf("/_matrix/client/v3/rooms/%s/messages", room.ID), test.WithQueryParams(map[string]string{
						"access_token": bobDev.AccessToken,
						"dir":          "b",
					})))
					if w.Code != 200 {
						t.Logf("%s", w.Body.String())
						t.Fatalf("got HTTP %d want %d", w.Code, 200)
					}
					if err = json.NewDecoder(w.Body).Decode(&res); err != nil {
						t.Errorf("failed to decode response body: %s", err)
					}
					// verify results
					verifyEventVisible(t, tc.wantResult.seeBeforeJoin, beforeJoinEv, res.Chunk)
					verifyEventVisible(t, tc.wantResult.seeAfterInvite, afterInviteEv, res.Chunk)
				})
			})
		}

	}
}

func verifyEventVisible(t *testing.T, wantVisible bool, wantVisibleEvent *rstypes.HeaderedEvent, chunk []synctypes.ClientEvent) {
	t.Helper()
	if wantVisible {
		for _, ev := range chunk {
			if ev.EventID == wantVisibleEvent.EventID() {
				return
			}
		}
		t.Fatalf("expected to see event %s but didn't: %+v", wantVisibleEvent.EventID(), chunk)
	} else {
		for _, ev := range chunk {
			if ev.EventID == wantVisibleEvent.EventID() {
				t.Fatalf("expected not to see event %s: %+v", wantVisibleEvent.EventID(), string(ev.Content))
			}
		}
	}
}

func TestGetMembership(t *testing.T) {
	alice := test.NewUser(t)

	aliceDev := userapi.Device{
		ID:          "ALICEID",
		UserID:      alice.ID,
		AccessToken: "ALICE_BEARER_TOKEN",
		DisplayName: "Alice",
		AccountType: userapi.AccountTypeUser,
	}

	bob := test.NewUser(t)
	bobDev := userapi.Device{
		ID:          "BOBID",
		UserID:      bob.ID,
		AccessToken: "notjoinedtoanyrooms",
	}

	testCases := []struct {
		name             string
		roomID           string
		additionalEvents func(t *testing.T, room *test.Room)
		request          func(t *testing.T, room *test.Room) *http.Request
		wantOK           bool
		wantMemberCount  int
		useSleep         bool // :/
	}{
		{
			name: "/members - Alice joined",
			request: func(t *testing.T, room *test.Room) *http.Request {
				return test.NewRequest(t, "GET", fmt.Sprintf("/_matrix/client/v3/rooms/%s/members", room.ID), test.WithQueryParams(map[string]string{
					"access_token": aliceDev.AccessToken,
				}))
			},
			wantOK:          true,
			wantMemberCount: 1,
		},
		{
			name: "/members - Bob never joined",
			request: func(t *testing.T, room *test.Room) *http.Request {
				return test.NewRequest(t, "GET", fmt.Sprintf("/_matrix/client/v3/rooms/%s/members", room.ID), test.WithQueryParams(map[string]string{
					"access_token": bobDev.AccessToken,
				}))
			},
			wantOK: false,
		},
		{
			name: "Alice leaves before Bob joins, should not be able to see Bob",
			request: func(t *testing.T, room *test.Room) *http.Request {
				return test.NewRequest(t, "GET", fmt.Sprintf("/_matrix/client/v3/rooms/%s/members", room.ID), test.WithQueryParams(map[string]string{
					"access_token": aliceDev.AccessToken,
				}))
			},
			additionalEvents: func(t *testing.T, room *test.Room) {
				room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]interface{}{
					"membership": "leave",
				}, test.WithStateKey(alice.ID))
				room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]interface{}{
					"membership": "join",
				}, test.WithStateKey(bob.ID))
			},
			useSleep:        true,
			wantOK:          true,
			wantMemberCount: 1,
		},
		{
			name: "Alice leaves after Bob joins, should be able to see Bob",
			request: func(t *testing.T, room *test.Room) *http.Request {
				return test.NewRequest(t, "GET", fmt.Sprintf("/_matrix/client/v3/rooms/%s/members", room.ID), test.WithQueryParams(map[string]string{
					"access_token": aliceDev.AccessToken,
				}))
			},
			additionalEvents: func(t *testing.T, room *test.Room) {
				room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]interface{}{
					"membership": "join",
				}, test.WithStateKey(bob.ID))
				room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]interface{}{
					"membership": "leave",
				}, test.WithStateKey(alice.ID))
			},
			useSleep:        true,
			wantOK:          true,
			wantMemberCount: 2,
		},
		{
			name: "'at' specified, returns memberships before Bob joins",
			request: func(t *testing.T, room *test.Room) *http.Request {
				return test.NewRequest(t, "GET", fmt.Sprintf("/_matrix/client/v3/rooms/%s/members", room.ID), test.WithQueryParams(map[string]string{
					"access_token": aliceDev.AccessToken,
					"at":           "t2_5",
				}))
			},
			additionalEvents: func(t *testing.T, room *test.Room) {
				room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]interface{}{
					"membership": "join",
				}, test.WithStateKey(bob.ID))
			},
			useSleep:        true,
			wantOK:          true,
			wantMemberCount: 1,
		},
		{
			name: "'membership=leave' specified, returns no memberships",
			request: func(t *testing.T, room *test.Room) *http.Request {
				return test.NewRequest(t, "GET", fmt.Sprintf("/_matrix/client/v3/rooms/%s/members", room.ID), test.WithQueryParams(map[string]string{
					"access_token": aliceDev.AccessToken,
					"membership":   "leave",
				}))
			},
			wantOK:          true,
			wantMemberCount: 0,
		},
		{
			name: "'not_membership=join' specified, returns no memberships",
			request: func(t *testing.T, room *test.Room) *http.Request {
				return test.NewRequest(t, "GET", fmt.Sprintf("/_matrix/client/v3/rooms/%s/members", room.ID), test.WithQueryParams(map[string]string{
					"access_token":   aliceDev.AccessToken,
					"not_membership": "join",
				}))
			},
			wantOK:          true,
			wantMemberCount: 0,
		},
		{
			name: "'not_membership=leave' & 'membership=join' specified, returns correct memberships",
			request: func(t *testing.T, room *test.Room) *http.Request {
				return test.NewRequest(t, "GET", fmt.Sprintf("/_matrix/client/v3/rooms/%s/members", room.ID), test.WithQueryParams(map[string]string{
					"access_token":   aliceDev.AccessToken,
					"not_membership": "leave",
					"membership":     "join",
				}))
			},
			additionalEvents: func(t *testing.T, room *test.Room) {
				room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]interface{}{
					"membership": "join",
				}, test.WithStateKey(bob.ID))
				room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]interface{}{
					"membership": "leave",
				}, test.WithStateKey(bob.ID))
			},
			wantOK:          true,
			wantMemberCount: 1,
		},
		{
			name: "non-existent room ID",
			request: func(t *testing.T, room *test.Room) *http.Request {
				return test.NewRequest(t, "GET", fmt.Sprintf("/_matrix/client/v3/rooms/%s/members", "!notavalidroom:test"), test.WithQueryParams(map[string]string{
					"access_token": aliceDev.AccessToken,
				}))
			},
			wantOK: false,
		},
	}

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		routers := httputil.NewRouters()
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

		// Use an actual roomserver for this
		rsAPI := roomserver.NewInternalAPI(ctx, cfg, cm, qm, caches, am, cacheutil.DisableMetrics)
		rsAPI.SetFederationAPI(ctx, nil, nil)

		AddPublicRoutes(ctx, routers, cfg, cm, qm, am, &syncUserAPI{accounts: []userapi.Device{aliceDev, bobDev}}, rsAPI, caches, cacheutil.DisableMetrics)

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				room := test.NewRoom(t, alice)
				t.Cleanup(func() {
					t.Logf("running cleanup for %s", tc.name)
				})
				// inject additional events
				if tc.additionalEvents != nil {
					tc.additionalEvents(t, room)
				}
				if err = rsapi.SendEvents(ctx, rsAPI, rsapi.KindNew, room.Events(), "test", "test", "test", nil, false); err != nil {
					t.Fatalf("failed to send events: %v", err)
				}

				// wait for the events to come down sync
				if tc.useSleep {
					time.Sleep(time.Millisecond * 100)
				} else {
					syncUntil(ctx, t, routers, aliceDev.AccessToken, false, func(syncBody string) bool {
						// wait for the last sent eventID to come down sync
						path := fmt.Sprintf(`rooms.join.%s.timeline.events.#(event_id=="%s")`, room.ID, room.Events()[len(room.Events())-1].EventID())
						return gjson.Get(syncBody, path).Exists()
					})
				}

				w := httptest.NewRecorder()
				routers.Client.ServeHTTP(w, tc.request(t, room))
				if w.Code != 200 && tc.wantOK {
					t.Logf("%s", w.Body.String())
					t.Fatalf("got HTTP %d want %d", w.Code, 200)
				}
				t.Logf("[%s] Resp: %s", tc.name, w.Body.String())

				// check we got the expected events
				if tc.wantOK {
					memberCount := len(gjson.GetBytes(w.Body.Bytes(), "chunk").Array())
					if memberCount != tc.wantMemberCount {
						t.Fatalf("expected %d members, got %d", tc.wantMemberCount, memberCount)
					}
				}
			})
		}
	})
}

func TestSendToDevice(t *testing.T) {

	user := test.NewUser(t)
	alice := userapi.Device{
		ID:          "ALICEID",
		UserID:      user.ID,
		AccessToken: "ALICE_BEARER_TOKEN",
		DisplayName: "Alice",
		AccountType: userapi.AccountTypeUser,
	}

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		routers := httputil.NewRouters()
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

		AddPublicRoutes(ctx, routers, cfg, cm, qm, am, &syncUserAPI{accounts: []userapi.Device{alice}}, &syncRoomserverAPI{}, caches, cacheutil.DisableMetrics)

		cfgSyncAPI := cfg.SyncAPI
		err = qm.RegisterPublisher(ctx, &cfgSyncAPI.Queues.OutputSendToDeviceEvent)
		if err != nil {
			t.Fatalf("failed to register publisher: %v", err)
		}

		producer := producers.SyncAPIProducer{
			TopicSendToDeviceEvent: cfgSyncAPI.Queues.OutputSendToDeviceEvent.Ref(),
			Qm:                     qm,
		}

		msgCounter := 0

		testCases := []struct {
			name              string
			since             string
			want              []string
			sendMessagesCount int
		}{
			{
				name: "initial sync, no messages",
				want: []string{},
			},
			{
				name:              "initial sync, one new message",
				sendMessagesCount: 1,
				want: []string{
					"message 1",
				},
			},
			{
				name:              "initial sync, two new messages", // we didn't advance the since token, so we'll receive two messages
				sendMessagesCount: 1,
				want: []string{
					"message 1",
					"message 2",
				},
			},
			{
				name:  "incremental sync, one message", // this deletes message 1, as we advanced the since token
				since: types.StreamingToken{SendToDevicePosition: 1}.String(),
				want: []string{
					"message 2",
				},
			},
			{
				name:  "failed incremental sync, one message", // didn't advance since, so still the same message
				since: types.StreamingToken{SendToDevicePosition: 1}.String(),
				want: []string{
					"message 2",
				},
			},
			{
				name:  "incremental sync, no message",                         // this should delete message 2
				since: types.StreamingToken{SendToDevicePosition: 2}.String(), // next_batch from previous sync
				want:  []string{},
			},
			{
				name:              "incremental sync, three new messages",
				since:             types.StreamingToken{SendToDevicePosition: 2}.String(),
				sendMessagesCount: 3,
				want: []string{
					"message 3", // message 2 was deleted in the previous test
					"message 4",
					"message 5",
				},
			},
			{
				name: "initial sync, three messages", // we expect three messages, as we didn't go beyond "2"
				want: []string{
					"message 3",
					"message 4",
					"message 5",
				},
			},
			{
				name:  "incremental sync, no messages", // advance the sync token, no new messages
				since: types.StreamingToken{SendToDevicePosition: 5}.String(),
				want:  []string{},
			},
		}

		for _, tc := range testCases {
			// Send to-device messages of type "m.dendrite.test" with content `{"dummy":"message $counter"}`
			for i := 0; i < tc.sendMessagesCount; i++ {
				msgCounter++
				msg := json.RawMessage(fmt.Sprintf(`{"dummy":"message %d"}`, msgCounter))
				err = producer.SendToDevice(ctx, user.ID, user.ID, alice.ID, "m.dendrite.test", msg)
				if err != nil {
					t.Fatalf("unable to send to device message: %v", err)
				}
			}

			syncUntil(ctx, t, routers, alice.AccessToken,
				len(tc.want) == 0,
				func(body string) bool {
					return gjson.Get(body, fmt.Sprintf(`to_device.events.#(content.dummy=="message %d")`, msgCounter)).Exists()
				},
			)

			// Execute a /sync request, recording the response
			w := httptest.NewRecorder()
			routers.Client.ServeHTTP(w, test.NewRequest(t, "GET", "/_matrix/client/v3/sync", test.WithQueryParams(map[string]string{
				"access_token": alice.AccessToken,
				"since":        tc.since,
			})))

			// Extract the to_device.events, # gets all values of an array, in this case a string slice with "message $counter" entries
			events := gjson.Get(w.Body.String(), "to_device.events.#.content.dummy").Array()
			got := make([]string, len(events))
			for i := range events {
				got[i] = events[i].String()
			}

			// Ensure the messages we received are as we expect them to be
			if !reflect.DeepEqual(got, tc.want) {
				t.Logf("[%s|since=%s]: Sync: %s", tc.name, tc.since, w.Body.String())
				t.Fatalf("[%s|since=%s]: got: %+v, want: %+v", tc.name, tc.since, got, tc.want)
			}
		}
	})
}

func TestContext(t *testing.T) {

	tests := []struct {
		name             string
		roomID           string
		eventID          string
		params           map[string]string
		wantError        bool
		wantStateLength  int
		wantBeforeLength int
		wantAfterLength  int
	}{
		{
			name: "invalid filter",
			params: map[string]string{
				"filter": "{",
			},
			wantError: true,
		},
		{
			name: "invalid limit",
			params: map[string]string{
				"limit": "abc",
			},
			wantError: true,
		},
		{
			name: "high limit",
			params: map[string]string{
				"limit": "100000",
			},
		},
		{
			name: "fine limit",
			params: map[string]string{
				"limit": "10",
			},
		},
		{
			name:            "last event without lazy loading",
			wantStateLength: 5,
		},
		{
			name: "last event with lazy loading",
			params: map[string]string{
				"filter": `{"lazy_load_members":true}`,
			},
			wantStateLength: 1,
		},
		{
			name:      "invalid room",
			roomID:    "!doesnotexist",
			wantError: true,
		},
		{
			name:      "invalid eventID",
			eventID:   "$doesnotexist",
			wantError: true,
		},
		{
			name: "state is limited",
			params: map[string]string{
				"limit": "1",
			},
			wantStateLength: 1,
		},
		{
			name:             "events are not limited",
			wantBeforeLength: 5,
		},
		{
			name: "all events are limited",
			params: map[string]string{
				"limit": "1",
			},
			wantStateLength:  1,
			wantBeforeLength: 1,
			wantAfterLength:  1,
		},
	}

	user := test.NewUser(t)
	alice := userapi.Device{
		ID:          "ALICEID",
		UserID:      user.ID,
		AccessToken: "ALICE_BEARER_TOKEN",
		DisplayName: "Alice",
		AccountType: userapi.AccountTypeUser,
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
				ctx, svc, cfg := testrig.Init(t, testOpts)
				defer svc.Stop(ctx)

				routers := httputil.NewRouters()
				cm := sqlutil.NewConnectionManager(svc)
				caches, err := cacheutil.NewCache(&cfg.Global.Cache)
				if err != nil {
					t.Fatalf("failed to create a cache: %v", err)
				}

				// Use an actual roomserver for this
				qm := queueutil.NewQueueManager(svc)
				am, err := actorutil.NewManager(ctx, &cfg.Global.Actors, qm)
				if err != nil {
					t.Fatalf("failed to create an actor manager: %v", err)
				}
				rsAPI := roomserver.NewInternalAPI(ctx, cfg, cm, qm, caches, am, cacheutil.DisableMetrics)
				rsAPI.SetFederationAPI(ctx, nil, nil)

				AddPublicRoutes(ctx, routers, cfg, cm, qm, am, &syncUserAPI{accounts: []userapi.Device{alice}}, rsAPI, caches, cacheutil.DisableMetrics)

				room := test.NewRoom(t, user)

				room.CreateAndInsert(t, user, "m.room.message", map[string]interface{}{"body": "hello world 1!"})
				room.CreateAndInsert(t, user, "m.room.message", map[string]interface{}{"body": "hello world 2!"})
				thirdMsg := room.CreateAndInsert(t, user, "m.room.message", map[string]interface{}{"body": "hello world3!"})
				room.CreateAndInsert(t, user, "m.room.message", map[string]interface{}{"body": "hello world4!"})

				if err = rsapi.SendEvents(ctx, rsAPI, rsapi.KindNew, room.Events(), "test", "test", "test", nil, false); err != nil {
					t.Fatalf("failed to send events: %v", err)
				}

				syncUntil(ctx, t, routers, alice.AccessToken, false, func(syncBody string) bool {
					// wait for the last sent eventID to come down sync
					path := fmt.Sprintf(`rooms.join.%s.timeline.events.#(event_id=="%s")`, room.ID, thirdMsg.EventID())
					return gjson.Get(syncBody, path).Exists()
				})

				params := map[string]string{
					"access_token": alice.AccessToken,
				}
				w := httptest.NewRecorder()
				// test overrides
				roomID := room.ID
				if tc.roomID != "" {
					roomID = tc.roomID
				}
				eventID := thirdMsg.EventID()
				if tc.eventID != "" {
					eventID = tc.eventID
				}
				requestPath := fmt.Sprintf("/_matrix/client/v3/rooms/%s/context/%s", roomID, eventID)
				if tc.params != nil {
					for k, v := range tc.params {
						params[k] = v
					}
				}
				routers.Client.ServeHTTP(w, test.NewRequest(t, "GET", requestPath, test.WithQueryParams(params)))

				if tc.wantError && w.Code == 200 {
					t.Fatalf("Expected an error, but got none")
				}
				t.Log(w.Body.String())
				resp := routing.ContextRespsonse{}
				if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
					t.Fatal(err)
				}
				if tc.wantStateLength > 0 && tc.wantStateLength != len(resp.State) {
					t.Fatalf("expected %d state events, got %d", tc.wantStateLength, len(resp.State))
				}
				if tc.wantBeforeLength > 0 && tc.wantBeforeLength != len(resp.EventsBefore) {
					t.Fatalf("expected %d before events, got %d", tc.wantBeforeLength, len(resp.EventsBefore))
				}
				if tc.wantAfterLength > 0 && tc.wantAfterLength != len(resp.EventsAfter) {
					t.Fatalf("expected %d after events, got %d", tc.wantAfterLength, len(resp.EventsAfter))
				}

				if !tc.wantError && resp.Event.EventID != eventID {
					t.Fatalf("unexpected eventID %s, expected %s", resp.Event.EventID, eventID)
				}
			})
		})
	}
}

func TestUpdateRelations(t *testing.T) {
	testCases := []struct {
		name         string
		eventContent map[string]interface{}
		eventType    string
	}{
		{
			name: "empty event content should not error",
		},
		{
			name: "unable to unmarshal event should not error",
			eventContent: map[string]interface{}{
				"m.relates_to": map[string]interface{}{
					"event_id": map[string]interface{}{}, // this should be a string and not struct
				},
			},
		},
		{
			name: "empty event ID is ignored",
			eventContent: map[string]interface{}{
				"m.relates_to": map[string]interface{}{
					"event_id": "",
				},
			},
		},
		{
			name: "empty rel_type is ignored",
			eventContent: map[string]interface{}{
				"m.relates_to": map[string]interface{}{
					"event_id": "$randomEventID",
					"rel_type": "",
				},
			},
		},
		{
			name:      "redactions are ignored",
			eventType: spec.MRoomRedaction,
			eventContent: map[string]interface{}{
				"m.relates_to": map[string]interface{}{
					"event_id": "$randomEventID",
					"rel_type": "m.replace",
				},
			},
		},
		{
			name: "valid event is correctly written",
			eventContent: map[string]interface{}{
				"m.relates_to": map[string]interface{}{
					"event_id": "$randomEventID",
					"rel_type": "m.replace",
				},
			},
		},
	}

	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		cm := sqlutil.NewConnectionManager(svc)

		db, err := storage.NewSyncServerDatabase(ctx, cm)
		if err != nil {
			t.Fatal(err)
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				evType := "m.room.message"
				if tc.eventType != "" {
					evType = tc.eventType
				}
				ev := room.CreateEvent(t, alice, evType, tc.eventContent)
				err = db.UpdateRelations(ctx, ev)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

func TestRemoveEditedEventFromSearchIndex(t *testing.T) {
	user := test.NewUser(t)
	alice := userapi.Device{
		ID:          "ALICEID",
		UserID:      user.ID,
		AccessToken: "ALICE_BEARER_TOKEN",
		DisplayName: "Alice",
		AccountType: userapi.AccountTypeUser,
	}

	routers := httputil.NewRouters()

	ctx, svc, cfg := testrig.Init(t)
	defer svc.Stop(ctx)

	cfg.SyncAPI.Fulltext.Enabled = true

	cm := sqlutil.NewConnectionManager(svc)
	caches, err := cacheutil.NewCache(&cfg.Global.Cache)
	if err != nil {
		t.Fatalf("failed to create a cache: %v", err)
	}

	// Use an actual roomserver for this
	qm := queueutil.NewQueueManager(svc)
	am, err := actorutil.NewManager(ctx, &cfg.Global.Actors, qm)
	if err != nil {
		t.Fatalf("failed to create an actor manager: %v", err)
	}

	rsAPI := roomserver.NewInternalAPI(ctx, cfg, cm, qm, caches, am, cacheutil.DisableMetrics)
	rsAPI.SetFederationAPI(ctx, nil, nil)

	room := test.NewRoom(t, user)
	AddPublicRoutes(ctx, routers, cfg, cm, qm, am, &syncUserAPI{accounts: []userapi.Device{alice}}, &syncRoomserverAPI{rooms: []*test.Room{room}}, caches, cacheutil.DisableMetrics)

	if err = rsapi.SendEvents(ctx, rsAPI, rsapi.KindNew, room.Events(), "test", "test", "test", nil, false); err != nil {
		t.Fatalf("failed to send events: %v", err)
	}

	ev1 := room.CreateAndInsert(t, user, "m.room.message", map[string]interface{}{"body": "first"})
	ev2 := room.CreateAndInsert(t, user, "m.room.message", map[string]interface{}{
		"body": " * first",
		"m.new_content": map[string]interface{}{
			"body":    "first",
			"msgtype": "m.text",
		},
		"m.relates_to": map[string]interface{}{
			"event_id": ev1.EventID(),
			"rel_type": "m.replace",
		},
	})
	events := []*rstypes.HeaderedEvent{ev1, ev2}

	for _, e := range events {
		roomEvents := append([]*rstypes.HeaderedEvent{}, e)
		if err = rsapi.SendEvents(ctx, rsAPI, rsapi.KindNew, roomEvents, "test", "test", "test", nil, false); err != nil {
			t.Fatalf("failed to send events: %v", err)
		}

		syncUntil(ctx, t, routers, alice.AccessToken, false, func(syncBody string) bool {
			// wait for the last sent eventID to come down sync
			path := fmt.Sprintf(`rooms.join.%s.timeline.events.#(event_id=="%s")`, room.ID, e.EventID())

			return gjson.Get(syncBody, path).Exists()
		})

		// We search that event is the only one nad is the exact event we sent
		searchResult := searchRequest(t, routers.Client, alice.AccessToken, "first", []string{room.ID})
		results := gjson.GetBytes(searchResult, fmt.Sprintf(`search_categories.room_events.groups.room_id.%s.results`, room.ID))
		assert.True(t, results.Exists(), "Should be a search response")
		assert.Equal(t, 1, len(results.Array()), "Should be exactly one result")
		assert.Equal(t, e.EventID(), results.Array()[0].String(), "Should be only found exact event")
	}
}

func searchRequest(t *testing.T, router *mux.Router, accessToken, searchTerm string, roomList []string) []byte {
	t.Helper()
	w := httptest.NewRecorder()
	rq := test.NewRequest(t, "POST", "/_matrix/client/v3/search", test.WithQueryParams(map[string]string{
		"access_token": accessToken,
	}), test.WithJSONBody(t, map[string]interface{}{
		"search_categories": map[string]interface{}{
			"room_events": map[string]interface{}{
				"filters":     roomList,
				"search_term": searchTerm,
			},
		},
	}))

	router.ServeHTTP(w, rq)
	assert.Equal(t, 200, w.Code)
	defer w.Result().Body.Close()
	body, err := io.ReadAll(w.Result().Body)
	assert.NoError(t, err)
	return body
}
func syncUntil(ctx context.Context, t *testing.T,
	routers httputil.Routers, accessToken string,
	skip bool,
	checkFunc func(syncBody string) bool,
) {
	t.Helper()
	if checkFunc == nil {
		t.Fatalf("No checkFunc defined")
	}
	if skip {
		return
	}
	// loop on /sync until we receive the last send message or timeout after 5 seconds, since we don't know if the message made it
	// to the syncAPI when hitting /sync
	done := make(chan bool)
	go func() {
		for {
			w := httptest.NewRecorder()
			routers.Client.ServeHTTP(w, test.NewRequest(t, "GET", "/_matrix/client/v3/sync", test.WithQueryParams(map[string]string{
				"access_token": accessToken,
				"timeout":      "1000",
			})))
			if checkFunc(w.Body.String()) {
				done <- true
				close(done)
				return
			}
		}

	}()

	select {
	case <-ctx.Done():
	case <-done:
	case <-time.After(time.Second * 5):
		t.Fatalf("Timed out waiting for messages")
	}
}

func toQueueMsgs(t *testing.T, input ...*rstypes.HeaderedEvent) []*testrig.QMsg {
	result := make([]*testrig.QMsg, len(input))

	for i, ev := range input {
		var addsStateIDs []string
		if ev.StateKey() != nil {
			addsStateIDs = append(addsStateIDs, ev.EventID())
		}

		roomID := ev.RoomID()

		result[i] = testrig.NewOutputEventMsg(t, &roomID, rsapi.OutputEvent{
			Type: rsapi.OutputTypeNewRoomEvent,
			NewRoomEvent: &rsapi.OutputNewRoomEvent{
				Event:             ev,
				AddsStateEventIDs: addsStateIDs,
				HistoryVisibility: ev.Visibility,
			},
		})
	}
	return result
}
