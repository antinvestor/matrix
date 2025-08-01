package appservice_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/antinvestor/apis/go/common"
	notificationv1 "github.com/antinvestor/apis/go/notification/v1"
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/appservice"
	"github.com/antinvestor/matrix/appservice/api"
	"github.com/antinvestor/matrix/appservice/consumers"
	"github.com/antinvestor/matrix/clientapi"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/federationapi/statistics"
	"github.com/antinvestor/matrix/internal/actorutil"
	"github.com/antinvestor/matrix/internal/cacheutil"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver"
	rsapi "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/syncapi"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/antinvestor/matrix/userapi"
	uapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/util"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var testIsBlacklistedOrBackingOff = func(ctx context.Context, s spec.ServerName) (*statistics.ServerStatistics, error) {
	return &statistics.ServerStatistics{}, nil
}

func TestAppserviceInternalAPI(t *testing.T) {

	// Set expected results
	existingProtocol := "irc"
	wantLocationResponse := []api.ASLocationResponse{{Protocol: existingProtocol, Fields: []byte("{}")}}
	wantUserResponse := []api.ASUserResponse{{Protocol: existingProtocol, Fields: []byte("{}")}}
	wantProtocolResponse := api.ASProtocolResponse{Instances: []api.ProtocolInstance{{Fields: []byte("{}")}}}
	wantProtocolResult := map[string]api.ASProtocolResponse{
		existingProtocol: wantProtocolResponse,
	}

	// create a dummy AS url, handling some cases
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "location"):
			// Check if we've got an existing protocol, if so, return a proper response.
			if r.URL.Path[len(r.URL.Path)-len(existingProtocol):] == existingProtocol {
				if err := json.NewEncoder(w).Encode(wantLocationResponse); err != nil {
					t.Fatalf("failed to encode response: %s", err)
				}
				return
			}
			if err := json.NewEncoder(w).Encode([]api.ASLocationResponse{}); err != nil {
				t.Fatalf("failed to encode response: %s", err)
			}
			return
		case strings.Contains(r.URL.Path, "user"):
			if r.URL.Path[len(r.URL.Path)-len(existingProtocol):] == existingProtocol {
				if err := json.NewEncoder(w).Encode(wantUserResponse); err != nil {
					t.Fatalf("failed to encode response: %s", err)
				}
				return
			}
			if err := json.NewEncoder(w).Encode([]api.UserResponse{}); err != nil {
				t.Fatalf("failed to encode response: %s", err)
			}
			return
		case strings.Contains(r.URL.Path, "protocol"):
			if r.URL.Path[len(r.URL.Path)-len(existingProtocol):] == existingProtocol {
				if err := json.NewEncoder(w).Encode(wantProtocolResponse); err != nil {
					t.Fatalf("failed to encode response: %s", err)
				}
				return
			}
			if err := json.NewEncoder(w).Encode(nil); err != nil {
				t.Fatalf("failed to encode response: %s", err)
			}
			return
		default:
			t.Logf("hit location: %s", r.URL.Path)
		}
	}))

	// The test cases to run
	runCases := func(t *testing.T, testAPI api.AppServiceInternalAPI) {
		t.Run("UserIDExists", func(t *testing.T) {
			testUserIDExists(t, testAPI, "@as-testing:test", true)
			testUserIDExists(t, testAPI, "@as1-testing:test", false)
		})

		t.Run("AliasExists", func(t *testing.T) {
			testAliasExists(t, testAPI, "@asroom-testing:test", true)
			testAliasExists(t, testAPI, "@asroom1-testing:test", false)
		})

		t.Run("Locations", func(t *testing.T) {
			testLocations(t, testAPI, existingProtocol, wantLocationResponse)
			testLocations(t, testAPI, "abc", nil)
		})

		t.Run("User", func(t *testing.T) {
			testUser(t, testAPI, existingProtocol, wantUserResponse)
			testUser(t, testAPI, "abc", nil)
		})

		t.Run("Protocols", func(t *testing.T) {
			testProtocol(t, testAPI, existingProtocol, wantProtocolResult)
			testProtocol(t, testAPI, existingProtocol, wantProtocolResult) // tests the cache
			testProtocol(t, testAPI, "", wantProtocolResult)               // tests getting all protocols
			testProtocol(t, testAPI, "abc", nil)
		})
	}

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		// Create a dummy application service
		as := &config.ApplicationService{
			ID:              "someID",
			URL:             srv.URL,
			ASToken:         "",
			HSToken:         "",
			SenderLocalpart: "senderLocalPart",
			NamespaceMap: map[string][]config.ApplicationServiceNamespace{
				"users":   {{RegexpObject: regexp.MustCompile("as-.*")}},
				"aliases": {{RegexpObject: regexp.MustCompile("asroom-.*")}},
			},
			Protocols: []string{existingProtocol},
		}
		as.CreateHTTPClient(cfg.AppServiceAPI.DisableTLSValidation)
		cfg.AppServiceAPI.Derived.ApplicationServices = []config.ApplicationService{*as}

		caches, err := cacheutil.NewCache(&cfg.Global.Cache)
		if err != nil {
			t.Fatalf("failed to create a cache: %v", err)
		}

		// Create required internal APIs
		qm := queueutil.NewQueueManager(svc)
		am, err := actorutil.NewManager(ctx, &cfg.Global.Actors, qm)
		if err != nil {
			t.Fatalf("failed to create an actor manager: %v", err)
		}

		cm := sqlutil.NewConnectionManager(svc)

		rsAPI := roomserver.NewInternalAPI(ctx, cfg, cm, qm, caches, am, cacheutil.DisableMetrics)
		rsAPI.SetFederationAPI(ctx, nil, nil)
		usrAPI := userapi.NewInternalAPI(ctx, cfg, cm, qm, am, rsAPI, nil, nil, nil, cacheutil.DisableMetrics, testIsBlacklistedOrBackingOff)
		asAPI := appservice.NewInternalAPI(ctx, cfg, qm, usrAPI, rsAPI, nil)

		runCases(t, asAPI)
	})
}

func TestAppserviceInternalAPI_UnixSocket_Simple(t *testing.T) {

	// Set expected results
	existingProtocol := "irc"
	wantLocationResponse := []api.ASLocationResponse{{Protocol: existingProtocol, Fields: []byte("{}")}}
	wantUserResponse := []api.ASUserResponse{{Protocol: existingProtocol, Fields: []byte("{}")}}
	wantProtocolResponse := api.ASProtocolResponse{Instances: []api.ProtocolInstance{{Fields: []byte("{}")}}}

	// create a dummy AS url, handling some cases
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "location"):
			// Check if we've got an existing protocol, if so, return a proper response.
			if r.URL.Path[len(r.URL.Path)-len(existingProtocol):] == existingProtocol {
				if err := json.NewEncoder(w).Encode(wantLocationResponse); err != nil {
					t.Fatalf("failed to encode response: %s", err)
				}
				return
			}
			if err := json.NewEncoder(w).Encode([]api.ASLocationResponse{}); err != nil {
				t.Fatalf("failed to encode response: %s", err)
			}
			return
		case strings.Contains(r.URL.Path, "user"):
			if r.URL.Path[len(r.URL.Path)-len(existingProtocol):] == existingProtocol {
				if err := json.NewEncoder(w).Encode(wantUserResponse); err != nil {
					t.Fatalf("failed to encode response: %s", err)
				}
				return
			}
			if err := json.NewEncoder(w).Encode([]api.UserResponse{}); err != nil {
				t.Fatalf("failed to encode response: %s", err)
			}
			return
		case strings.Contains(r.URL.Path, "protocol"):
			if r.URL.Path[len(r.URL.Path)-len(existingProtocol):] == existingProtocol {
				if err := json.NewEncoder(w).Encode(wantProtocolResponse); err != nil {
					t.Fatalf("failed to encode response: %s", err)
				}
				return
			}
			if err := json.NewEncoder(w).Encode(nil); err != nil {
				t.Fatalf("failed to encode response: %s", err)
			}
			return
		default:
			t.Logf("hit location: %s", r.URL.Path)
		}
	}))

	tmpDir := t.TempDir()
	socket := path.Join(tmpDir, "socket")
	l, err := net.Listen("unix", socket)
	assert.NoError(t, err)
	_ = srv.Listener.Close()
	srv.Listener = l
	srv.Start()
	defer srv.Close()

	ctx, svc, cfg := testrig.Init(t)
	defer svc.Stop(ctx)

	// Create a dummy application service
	as := &config.ApplicationService{
		ID:              "someID",
		URL:             fmt.Sprintf("unix://%s", socket),
		ASToken:         "",
		HSToken:         "",
		SenderLocalpart: "senderLocalPart",
		NamespaceMap: map[string][]config.ApplicationServiceNamespace{
			"users":   {{RegexpObject: regexp.MustCompile("as-.*")}},
			"aliases": {{RegexpObject: regexp.MustCompile("asroom-.*")}},
		},
		Protocols: []string{existingProtocol},
	}
	as.CreateHTTPClient(cfg.AppServiceAPI.DisableTLSValidation)
	cfg.AppServiceAPI.Derived.ApplicationServices = []config.ApplicationService{*as}

	caches, err := cacheutil.NewCache(&cfg.Global.Cache)
	if err != nil {
		t.Fatalf("failed to create a cache: %v", err)
	}
	// Create required internal APIs
	qm := queueutil.NewQueueManager(svc)
	am, err := actorutil.NewManager(ctx, &cfg.Global.Actors, qm)
	if err != nil {
		t.Fatalf("failed to create an actor manager: %v", err)
	}
	cm := sqlutil.NewConnectionManager(svc)
	rsAPI := roomserver.NewInternalAPI(ctx, cfg, cm, qm, caches, am, cacheutil.DisableMetrics)
	rsAPI.SetFederationAPI(ctx, nil, nil)
	usrAPI := userapi.NewInternalAPI(ctx, cfg, cm, qm, am, rsAPI, nil, nil, nil, cacheutil.DisableMetrics, testIsBlacklistedOrBackingOff)
	asAPI := appservice.NewInternalAPI(ctx, cfg, qm, usrAPI, rsAPI, nil)

	t.Run("UserIDExists", func(t *testing.T) {
		testUserIDExists(t, asAPI, "@as-testing:test", true)
		testUserIDExists(t, asAPI, "@as1-testing:test", false)
	})

}

func testUserIDExists(t *testing.T, asAPI api.AppServiceInternalAPI, userID string, wantExists bool) {
	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	userResp := &api.UserIDExistsResponse{}

	if err := asAPI.UserIDExists(ctx, &api.UserIDExistsRequest{
		UserID: userID,
	}, userResp); err != nil {
		t.Errorf("failed to get userID: %s", err)
	}
	if userResp.UserIDExists != wantExists {
		t.Errorf("unexpected result for UserIDExists(%s): %v, expected %v", userID, userResp.UserIDExists, wantExists)
	}
}

func testAliasExists(t *testing.T, asAPI api.AppServiceInternalAPI, alias string, wantExists bool) {
	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	aliasResp := &api.RoomAliasExistsResponse{}

	if err := asAPI.RoomAliasExists(ctx, &api.RoomAliasExistsRequest{
		Alias: alias,
	}, aliasResp); err != nil {
		t.Errorf("failed to get alias: %s", err)
	}
	if aliasResp.AliasExists != wantExists {
		t.Errorf("unexpected result for RoomAliasExists(%s): %v, expected %v", alias, aliasResp.AliasExists, wantExists)
	}
}

func testLocations(t *testing.T, asAPI api.AppServiceInternalAPI, proto string, wantResult []api.ASLocationResponse) {
	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	locationResp := &api.LocationResponse{}

	if err := asAPI.Locations(ctx, &api.LocationRequest{
		Protocol: proto,
	}, locationResp); err != nil {
		t.Errorf("failed to get locations: %s", err)
	}
	if !reflect.DeepEqual(locationResp.Locations, wantResult) {
		t.Errorf("unexpected result for Locations(%s): %+v, expected %+v", proto, locationResp.Locations, wantResult)
	}
}

func testUser(t *testing.T, asAPI api.AppServiceInternalAPI, proto string, wantResult []api.ASUserResponse) {
	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	userResp := &api.UserResponse{}

	if err := asAPI.User(ctx, &api.UserRequest{
		Protocol: proto,
	}, userResp); err != nil {
		t.Errorf("failed to get user: %s", err)
	}
	if !reflect.DeepEqual(userResp.Users, wantResult) {
		t.Errorf("unexpected result for User(%s): %+v, expected %+v", proto, userResp.Users, wantResult)
	}
}

func testProtocol(t *testing.T, asAPI api.AppServiceInternalAPI, proto string, wantResult map[string]api.ASProtocolResponse) {
	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	protoResp := &api.ProtocolResponse{}

	if err := asAPI.Protocols(ctx, &api.ProtocolRequest{
		Protocol: proto,
	}, protoResp); err != nil {
		t.Errorf("failed to get Protocols: %s", err)
	}
	if !reflect.DeepEqual(protoResp.Protocols, wantResult) {
		t.Errorf("unexpected result for Protocols(%s): %+v, expected %+v", proto, protoResp.Protocols[proto], wantResult)
	}
}

// Tests that the roomserver consumer only receives one invite
func TestRoomserverConsumerOneInvite(t *testing.T) {

	alice := test.NewUser(t)
	bob := test.NewUser(t)
	room := test.NewRoom(t, alice)

	// Invite Bob
	room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]interface{}{
		"membership": "invite",
	}, test.WithStateKey(bob.ID))

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		cm := sqlutil.NewConnectionManager(svc)
		qm := queueutil.NewQueueManager(svc)
		am, err := actorutil.NewManager(ctx, &cfg.Global.Actors, qm)
		if err != nil {
			t.Fatalf("failed to create an actor manager: %v", err)
		}

		evChan := make(chan struct{})
		// create a dummy AS url, handling the events
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var txn consumers.ApplicationServiceTransaction
			err = json.NewDecoder(r.Body).Decode(&txn)
			if err != nil {
				t.Fatal(err)
			}
			for _, ev := range txn.Events {
				if ev.Type != spec.MRoomMember {
					continue
				}
				// Usually we would check the event content for the membership, but since
				// we only invited bob, this should be fine for this test.
				if ev.StateKey != nil && *ev.StateKey == bob.ID {
					evChan <- struct{}{}
				}
			}
		}))
		defer srv.Close()

		as := &config.ApplicationService{
			ID:              "someID",
			URL:             srv.URL,
			ASToken:         "",
			HSToken:         "",
			SenderLocalpart: "senderLocalPart",
			NamespaceMap: map[string][]config.ApplicationServiceNamespace{
				"users":   {{RegexpObject: regexp.MustCompile(bob.ID)}},
				"aliases": {{RegexpObject: regexp.MustCompile(room.ID)}},
			},
		}
		as.CreateHTTPClient(cfg.AppServiceAPI.DisableTLSValidation)

		// Create a dummy application service
		cfg.AppServiceAPI.Derived.ApplicationServices = []config.ApplicationService{*as}

		caches, err := cacheutil.NewCache(&cfg.Global.Cache)
		if err != nil {
			t.Fatalf("failed to create a cache: %v", err)
		}
		// Create required internal APIs
		rsAPI := roomserver.NewInternalAPI(ctx, cfg, cm, qm, caches, am, cacheutil.DisableMetrics)
		rsAPI.SetFederationAPI(ctx, nil, nil)
		usrAPI := userapi.NewInternalAPI(ctx, cfg, cm, qm, am, rsAPI, nil, nil, nil, cacheutil.DisableMetrics, testIsBlacklistedOrBackingOff)
		// start the consumer
		appservice.NewInternalAPI(ctx, cfg, qm, usrAPI, rsAPI, nil)

		// Create the room
		if err = rsapi.SendEvents(ctx, rsAPI, rsapi.KindNew, room.Events(), "test", "test", "test", nil, false); err != nil {
			t.Fatalf("failed to send events: %v", err)
		}
		var seenInvitesForBob int
	waitLoop:
		for {
			select {
			case <-time.After(time.Millisecond * 50): // wait for the AS to process the events
				break waitLoop
			case <-evChan:
				seenInvitesForBob++
				if seenInvitesForBob != 1 {
					t.Fatalf("received unexpected invites: %d", seenInvitesForBob)
				}
			}
		}
		close(evChan)
	})
}

// Note: If this test panics, it is because we timed out waiting for the
// join event to come through to the appservice and we close the Cm/shutdown Matrix.
// This makes syncAPI unhappy, as it is unable to write to the database.
func TestOutputAppserviceEvent(t *testing.T) {

	t.Skip("test is flacky in CI")
	alice := test.NewUser(t)
	bob := test.NewUser(t)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		cm := sqlutil.NewConnectionManager(svc)
		qm := queueutil.NewQueueManager(svc)
		am, err := actorutil.NewManager(ctx, &cfg.Global.Actors, qm)
		if err != nil {
			t.Fatalf("failed to create an actor manager: %v", err)
		}

		evChan := make(chan struct{})
		t.Cleanup(func() { close(evChan) })

		caches, err := cacheutil.NewCache(&cfg.Global.Cache)
		if err != nil {
			t.Fatalf("failed to create a cache: %v", err)
		}
		// Create required internal APIs
		rsAPI := roomserver.NewInternalAPI(ctx, cfg, cm, qm, caches, am, cacheutil.DisableMetrics)
		rsAPI.SetFederationAPI(ctx, nil, nil)

		// Create the router, so we can hit `/joined_members`
		routers := httputil.NewRouters()

		accessTokens := map[*test.User]userDevice{
			bob: {},
		}

		usrAPI := userapi.NewInternalAPI(ctx, cfg, cm, qm, am, rsAPI, nil, nil, nil, cacheutil.DisableMetrics, testIsBlacklistedOrBackingOff)
		rsAPI.SetUserAPI(ctx, usrAPI)

		clientapi.AddPublicRoutes(ctx, routers, cfg, qm, nil, rsAPI, nil, nil, nil, usrAPI, nil, nil, nil, nil, cacheutil.DisableMetrics)
		createAccessTokens(t, accessTokens, usrAPI, ctx, routers)

		room := test.NewRoom(t, alice)

		// Invite Bob
		room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]interface{}{
			"membership": "invite",
		}, test.WithStateKey(bob.ID))

		// create a dummy AS url, handling the events
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			var txn consumers.ApplicationServiceTransaction
			err = json.NewDecoder(r.Body).Decode(&txn)
			if err != nil {
				t.Fatal(err)
			}
			for _, ev := range txn.Events {
				if ev.Type != spec.MRoomMember {
					continue
				}
				if ev.StateKey != nil && *ev.StateKey == bob.ID {
					membership := gjson.GetBytes(ev.Content, "membership").Str
					t.Logf("Processing membership: %s", membership)
					switch membership {
					case spec.Invite:
						// Accept the invite
						joinEv := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]interface{}{
							"membership": "join",
						}, test.WithStateKey(bob.ID))

						err = rsapi.SendEvents(ctx, rsAPI, rsapi.KindNew, []*types.HeaderedEvent{joinEv}, "test", "test", "test", nil, false)
						if err != nil {
							t.Fatalf("failed to send events: %v", err)
						}
					case spec.Join: // the AS has received the join event, now hit `/joined_members` to validate that
						rec := httptest.NewRecorder()
						req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/rooms/"+room.ID+"/joined_members", nil)
						req.Header.Set("Authorization", "Bearer "+accessTokens[bob].accessToken)
						routers.Client.ServeHTTP(rec, req)
						if rec.Code != http.StatusOK {
							t.Fatalf("expected HTTP 200, got %d: %s", rec.Code, rec.Body.String())
						}

						// Both Alice and Bob should be joined. If not, we have a race condition
						if !gjson.GetBytes(rec.Body.Bytes(), "joined."+alice.ID).Exists() {
							t.Errorf("Alice is not joined to the room") // in theory should not happen
						}
						if !gjson.GetBytes(rec.Body.Bytes(), "joined."+bob.ID).Exists() {
							t.Errorf("Bob is not joined to the room")
						}
						evChan <- struct{}{}
					default:
						t.Fatalf("Unexpected membership: %s", membership)
					}
				}
			}
		}))
		defer srv.Close()

		as := &config.ApplicationService{
			ID:              "someID",
			URL:             srv.URL,
			ASToken:         "",
			HSToken:         "",
			SenderLocalpart: "senderLocalPart",
			NamespaceMap: map[string][]config.ApplicationServiceNamespace{
				"users":   {{RegexpObject: regexp.MustCompile(bob.ID)}},
				"aliases": {{RegexpObject: regexp.MustCompile(room.ID)}},
			},
		}
		as.CreateHTTPClient(cfg.AppServiceAPI.DisableTLSValidation)

		// Create a dummy application service
		cfg.AppServiceAPI.Derived.ApplicationServices = []config.ApplicationService{*as}

		// Start the syncAPI to have `/joined_members` available
		syncapi.AddPublicRoutes(ctx, routers, cfg, cm, qm, am, usrAPI, rsAPI, caches, cacheutil.DisableMetrics)

		// start the consumer
		appservice.NewInternalAPI(ctx, cfg, qm, usrAPI, rsAPI, nil)

		// Create the room
		err = rsapi.SendEvents(ctx, rsAPI, rsapi.KindNew, room.Events(), "test", "test", "test", nil, false)
		if err != nil {
			t.Fatalf("failed to send events: %v", err)
		}

		select {
		// Pretty generous timeout duration...
		case <-time.After(time.Second * 10):
			// wait for the AS to process the events
			t.Errorf("Timed out waiting for join event")
		case <-evChan:
		}
	})
}

func TestDistributedOutputAppserviceEvent(t *testing.T) {

	alice := test.NewUser(t)
	bob := test.NewUser(t)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		cm := sqlutil.NewConnectionManager(svc)
		qm := queueutil.NewQueueManager(svc)
		am, err := actorutil.NewManager(ctx, &cfg.Global.Actors, qm)
		if err != nil {
			t.Fatalf("failed to create an actor manager: %v", err)
		}

		evChan := make(chan struct{})
		t.Cleanup(func() { close(evChan) })

		caches, err := cacheutil.NewCache(&cfg.Global.Cache)
		if err != nil {
			t.Fatalf("failed to create a cache: %v", err)
		}
		// Create required internal APIs
		rsAPI := roomserver.NewInternalAPI(ctx, cfg, cm, qm, caches, am, cacheutil.DisableMetrics)
		rsAPI.SetFederationAPI(ctx, nil, nil)

		// Create the router, so we can hit `/joined_members`
		routers := httputil.NewRouters()

		accessTokens := map[*test.User]userDevice{
			bob: {},
		}

		usrAPI := userapi.NewInternalAPI(ctx, cfg, cm, qm, am, rsAPI, nil, nil, nil, cacheutil.DisableMetrics, testIsBlacklistedOrBackingOff)
		rsAPI.SetUserAPI(ctx, usrAPI)

		clientapi.AddPublicRoutes(ctx, routers, cfg, qm, nil, rsAPI, nil, nil, nil, usrAPI, nil, nil, nil, nil, cacheutil.DisableMetrics)
		createAccessTokens(t, accessTokens, usrAPI, ctx, routers)

		room := test.NewRoom(t, alice)

		// Invite Bob
		room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{
			"membership": "invite",
		}, test.WithStateKey(bob.ID))

		room.CreateAndInsert(t, alice, "com.antinvestor.messages", map[string]any{
			"command": "test message",
		})

		as := &config.ApplicationService{
			ID:              "someID",
			URL:             "antinvestor.com",
			IsDistributed:   true,
			SenderLocalpart: "senderLocalPart",
			NamespaceMap: map[string][]config.ApplicationServiceNamespace{
				"users":   {{RegexpObject: regexp.MustCompile(bob.ID)}},
				"aliases": {{RegexpObject: regexp.MustCompile(room.ID)}},
			},
		}

		// Create a dummy application service
		cfg.AppServiceAPI.Derived.ApplicationServices = []config.ApplicationService{*as}

		// Start the syncAPI to have `/joined_members` available
		syncapi.AddPublicRoutes(ctx, routers, cfg, cm, qm, am, usrAPI, rsAPI, caches, cacheutil.DisableMetrics)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		nCliMock := notificationv1.NewMockNotificationServiceClient(ctrl)

		// Mock the Receive method to return a channel of responses
		nCliMock.EXPECT().Receive(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, req *notificationv1.ReceiveRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[notificationv1.ReceiveResponse], error) {

				// Signal the test that the method was called
				evChan <- struct{}{}

				// Create a mock StreamingClient that will be immediately closed
				// This simulates a successful stream with no errors
				return &mockNotificationReceiveClient{}, nil
			}).AnyTimes()

		notificationCli := notificationv1.Init(&common.GrpcClientBase{}, nCliMock)

		// start the consumer
		appservice.NewInternalAPI(ctx, cfg, qm, usrAPI, rsAPI, notificationCli)

		claims := frame.AuthenticationClaims{
			Ext:         nil,
			TenantID:    "tenant1",
			PartitionID: "partition2",
			AccessID:    "access3",
			ContactID:   "contact5",
			DeviceID:    "device6",
			ServiceName: "matrix",
		}

		authCtx := claims.ClaimsToContext(ctx)

		// Create the room, this triggers the AS to receive an invite for Bob.
		err = rsapi.SendEvents(authCtx, rsAPI, rsapi.KindNew, room.Events(), "test", "test", "test", nil, false)
		if err != nil {
			t.Fatalf("failed to send events: %v", err)
		}

		select {
		// Pretty generous timeout duration...
		case <-time.After(time.Second * 10):
			// wait for the AS to process the events
			t.Errorf("Timed out waiting for join event")
		case <-evChan:
		}

	})
}

type mockNotificationReceiveClient struct {
	recvCalled bool
}

func (m *mockNotificationReceiveClient) Recv() (*notificationv1.ReceiveResponse, error) {
	if !m.recvCalled {
		m.recvCalled = true
		return &notificationv1.ReceiveResponse{}, nil
	}
	return nil, io.EOF
}

func (m *mockNotificationReceiveClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockNotificationReceiveClient) Trailer() metadata.MD {
	return nil
}

func (m *mockNotificationReceiveClient) CloseSend() error {
	return nil
}

func (m *mockNotificationReceiveClient) Context() context.Context {
	return context.Background()
}

func (m *mockNotificationReceiveClient) SendMsg(interface{}) error {
	return nil
}

func (m *mockNotificationReceiveClient) RecvMsg(interface{}) error {
	return nil
}

type userDevice struct {
	accessToken string
	deviceID    string
	password    string
}

func createAccessTokens(t *testing.T, accessTokens map[*test.User]userDevice, userAPI uapi.UserInternalAPI, ctx context.Context, routers httputil.Routers) {
	t.Helper()
	for u := range accessTokens {
		localpart, serverName, _ := gomatrixserverlib.SplitID('@', u.ID)
		userRes := &uapi.PerformAccountCreationResponse{}
		password := util.RandomString(8)
		if err := userAPI.PerformAccountCreation(ctx, &uapi.PerformAccountCreationRequest{
			AccountType: u.AccountType,
			Localpart:   localpart,
			ServerName:  serverName,
			Password:    password,
		}, userRes); err != nil {
			t.Errorf("failed to create account: %s", err)
		}
		req := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/login", test.WithJSONBody(t, map[string]interface{}{
			"type": authtypes.LoginTypePassword,
			"identifier": map[string]interface{}{
				"type": "m.id.user",
				"user": u.ID,
			},
			"password": password,
		}))
		rec := httptest.NewRecorder()
		routers.Client.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("failed to login: %s", rec.Body.String())
		}
		accessTokens[u] = userDevice{
			accessToken: gjson.GetBytes(rec.Body.Bytes(), "access_token").String(),
			deviceID:    gjson.GetBytes(rec.Body.Bytes(), "device_id").String(),
			password:    password,
		}
	}
}
