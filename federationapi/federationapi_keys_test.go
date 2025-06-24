package federationapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/api"
	"github.com/antinvestor/matrix/federationapi/routing"
	"github.com/antinvestor/matrix/internal/actorutil"
	"github.com/antinvestor/matrix/internal/cacheutil"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/pitabwire/frame"
)

type server struct {
	name      spec.ServerName           // server name
	validity  time.Duration             // key validity duration from now
	config    *config.FederationAPI     // skeleton config, from TestMain
	fedclient fclient.FederationClient  // uses MockRoundTripper
	cache     *cacheutil.Caches         // server-specific cache
	api       api.FederationInternalAPI // server-specific server key API
}

func (s *server) renew() {
	// This updates the validity period to be an hour in the
	// future, which is particularly useful in server A and
	// server C's cases which have validity either as now or
	// in the past.
	s.validity = time.Hour
	s.config.Global.KeyValidityPeriod = s.validity
}

var (
	serverKeyID = gomatrixserverlib.KeyID("ed25519:auto")
	serverA     = &server{name: "a.com", validity: time.Duration(0)} // expires now
	serverB     = &server{name: "b.com", validity: time.Hour}        // expires in an hour
	serverC     = &server{name: "c.com", validity: -time.Hour}       // expired an hour ago
)

var servers = map[string]*server{
	"a.com": serverA,
	"b.com": serverB,
	"c.com": serverC,
}

type MockRoundTripper struct{}

func (m *MockRoundTripper) RoundTrip(req *http.Request) (res *http.Response, err error) {
	// Check if the request is looking for keys from a server that
	// we know about in the test. The only reason this should go wrong
	// is if the test is broken.
	s, ok := servers[req.Host]
	if !ok {
		return nil, fmt.Errorf("server not known: %s", req.Host)
	}

	// We're intercepting /matrix/key/v2/server requests here, so check
	// that the URL supplied in the request is for that.
	if req.URL.Path != "/_matrix/key/v2/server" {
		return nil, fmt.Errorf("unexpected request path: %s", req.URL.Path)
	}

	// Get the keys and JSON-ify them.
	keys := routing.LocalKeys(s.config, spec.ServerName(req.Host))
	body, err := json.MarshalIndent(keys.JSON, "", "  ")
	if err != nil {
		return nil, err
	}

	// And respond.
	res = &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
	return
}

func createFederationDbKeys(ctx context.Context, t *testing.T, svc *frame.Service, cfg0 *config.Matrix) {

	for _, s := range servers {
		var err error
		// Make a copy of the configuration to avoid modifying the original
		cfg := *cfg0

		cm := sqlutil.NewConnectionManager(svc)

		globalCfg := cfg.Global
		// Draw up just enough Matrix config for the server key
		// API to work.
		globalCfg.ServerName = s.name

		// Generate a new key.
		_, globalCfg.PrivateKey, err = ed25519.GenerateKey(nil)
		if err != nil {
			t.Fatalf("can't generate identity key: %v", err)
		}

		globalCfg.KeyID = serverKeyID
		globalCfg.KeyValidityPeriod = s.validity
		cfg.FederationAPI.KeyPerspectives = nil

		cfg.FederationAPI.Global = &globalCfg
		cfg.KeyServer.Global = &globalCfg

		s.config = &cfg.FederationAPI

		s.cache, err = cacheutil.NewCache(&config.CacheOptions{
			CacheURI: globalCfg.Cache.CacheURI,
		})
		if err != nil {
			t.Fatalf("can't create cache : %v", err)
		}

		qm := queueutil.NewQueueManager(svc)

		am, err := actorutil.NewManager(ctx, &cfg.Global.Actors, qm)
		if err != nil {
			t.Fatalf("failed to create an actor manager: %v", err)
		}

		// Create a transport which redirects federation requests to
		// the mock round tripper. Since we're not *really* listening for
		// federation requests then this will return the key instead.
		transport := &http.Transport{}
		transport.RegisterProtocol("matrix", &MockRoundTripper{})

		// Create the federation client.
		s.fedclient = createFederationClient(s)

		// Finally, build the server key APIs.

		s.api = NewInternalAPI(ctx, &cfg, cm, qm, am, s.fedclient, nil, s.cache, nil, true, nil)
	}
}

func createFederationClient(s *server) fclient.FederationClient {
	// Create a transport which redirects federation requests to
	// the mock round tripper. Since we're not *really* listening for
	// federation requests then this will return the key instead.
	transport := &http.Transport{}
	transport.RegisterProtocol("matrix", &MockRoundTripper{})

	// Create the federation client.
	return fclient.NewFederationClient(
		s.config.Global.SigningIdentities(),
		fclient.WithTransport(transport),
	)
}

func TestServersRequestOwnKeys(t *testing.T) {
	// Each server will request its own keys. There's no reason
	// for this to fail as each server should know its own keys.
	ctx, svc, cfg := testrig.Init(t)
	defer svc.Stop(ctx)

	createFederationDbKeys(ctx, t, svc, cfg)

	for name, s := range servers {

		req := gomatrixserverlib.PublicKeyLookupRequest{
			ServerName: s.name,
			KeyID:      serverKeyID,
		}

		res, err := s.api.FetchKeys(
			ctx,
			map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp{
				req: spec.AsTimestamp(time.Now()),
			},
		)
		if err != nil {
			t.Fatalf("server could not fetch own key: %s", err)
		}

		if _, ok := res[req]; !ok {
			t.Fatalf("server didn't return its own key in the results")
		}
		t.Logf("%s's key expires at %s\n", name, res[req].ValidUntilTS.Time())
	}
}

func TestRenewalBehaviour(t *testing.T) {
	// Server A will request Server C's key but their validity period
	// is an hour in the past. We'll retrieve the key as, even though it's
	// past its validity, it will be able to verify past events.
	ctx, svc, cfg := testrig.Init(t)
	defer svc.Stop(ctx)

	createFederationDbKeys(ctx, t, svc, cfg)

	req := gomatrixserverlib.PublicKeyLookupRequest{
		ServerName: serverC.name,
		KeyID:      serverKeyID,
	}

	res, err := serverA.api.FetchKeys(
		ctx,
		map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp{
			req: spec.AsTimestamp(time.Now()),
		},
	)
	if err != nil {
		t.Fatalf("server A failed to retrieve server C key: %s", err)
	}
	if len(res) != 1 {
		t.Fatalf("server C should have returned one key but instead returned %d keys", len(res))
	}
	if _, ok := res[req]; !ok {
		t.Fatalf("server C isn't included in the key fetch response")
	}

	originalValidity := res[req].ValidUntilTS

	// We're now going to kick server C into renewing its key. Since we're
	// happy at this point that the key that we already have is from the past
	// then repeating a key fetch should cause us to try and renew the key.
	// If so, then the new key will end up in our cache.
	serverC.renew()

	res, err = serverA.api.FetchKeys(
		ctx,
		map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp{
			req: spec.AsTimestamp(time.Now()),
		},
	)
	if err != nil {
		t.Fatalf("server A failed to retrieve server C key: %s", err)
	}
	if len(res) != 1 {
		t.Fatalf("server C should have returned one key but instead returned %d keys", len(res))
	}
	if _, ok := res[req]; !ok {
		t.Fatalf("server C isn't included in the key fetch response")
	}

	currentValidity := res[req].ValidUntilTS

	if originalValidity == currentValidity {
		t.Fatalf("server C key should have renewed but didn't")
	}
}
