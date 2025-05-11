package storage_test

import (
	"context"
	"github.com/pitabwire/frame"
	"reflect"
	"testing"
	"time"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage"
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/pitabwire/util"
	"github.com/stretchr/testify/assert"
)

func mustCreateFederationDatabase(ctx context.Context, svc *frame.Service, cfg *config.Dendrite, t *testing.T, _ test.DependancyOption) storage.Database {

	caches, err := caching.NewCache(&cfg.Global.Cache)
	if err != nil {
		t.Fatalf("Could not create cache from options %s", err)
	}

	cm := sqlutil.NewConnectionManager(svc)
	db, err := storage.NewDatabase(ctx, cm, caches, func(server spec.ServerName) bool { return server == "localhost" })
	if err != nil {
		t.Fatalf("NewDatabase returned %s", err)
	}
	return db
}

func TestExpireEDUs(t *testing.T) {
	var expireEDUTypes = map[string]time.Duration{
		spec.MReceipt: 0,
	}

	destinations := map[spec.ServerName]struct{}{"localhost": {}}
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		db := mustCreateFederationDatabase(ctx, svc, cfg, t, testOpts)

		// insert some data
		for i := 0; i < 100; i++ {
			receipt, err := db.StoreJSON(ctx, "{}")
			assert.NoError(t, err)

			err = db.AssociateEDUWithDestinations(ctx, destinations, receipt, spec.MReceipt, expireEDUTypes)
			assert.NoError(t, err)
		}
		// add data without expiry
		receipt, err := db.StoreJSON(ctx, "{}")
		assert.NoError(t, err)

		// m.read_marker gets the default expiry of 24h, so won't be deleted further down in this test
		err = db.AssociateEDUWithDestinations(ctx, destinations, receipt, "m.read_marker", expireEDUTypes)
		assert.NoError(t, err)

		// Delete expired EDUs
		err = db.DeleteExpiredEDUs(ctx)
		assert.NoError(t, err)

		// verify the data is gone
		data, err := db.GetPendingEDUs(ctx, "localhost", 100)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(data))

		// check that m.direct_to_device is never expired
		receipt, err = db.StoreJSON(ctx, "{}")
		assert.NoError(t, err)

		err = db.AssociateEDUWithDestinations(ctx, destinations, receipt, spec.MDirectToDevice, expireEDUTypes)
		assert.NoError(t, err)

		err = db.DeleteExpiredEDUs(ctx)
		assert.NoError(t, err)

		// We should get two EDUs, the m.read_marker and the m.direct_to_device
		data, err = db.GetPendingEDUs(ctx, "localhost", 100)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(data))
	})
}

func TestOutboundPeeking(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	_, serverName, _ := gomatrixserverlib.SplitID('@', alice.ID)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateFederationDatabase(ctx, svc, cfg, t, testOpts)

		peekID := util.RandomString(8)
		var renewalInterval int64 = 1000

		// Add outbound peek
		if err := db.AddOutboundPeek(ctx, serverName, room.ID, peekID, renewalInterval); err != nil {
			t.Fatal(err)
		}

		// select the newly inserted peek
		outboundPeek1, err := db.GetOutboundPeek(ctx, serverName, room.ID, peekID)
		if err != nil {
			t.Fatal(err)
		}

		// Assert fields are set as expected
		if outboundPeek1.PeekID != peekID {
			t.Fatalf("unexpected outbound peek ID: %s, want %s", outboundPeek1.PeekID, peekID)
		}
		if outboundPeek1.RoomID != room.ID {
			t.Fatalf("unexpected outbound peek room ID: %s, want %s", outboundPeek1.RoomID, peekID)
		}
		if outboundPeek1.ServerName != serverName {
			t.Fatalf("unexpected outbound peek servername: %s, want %s", outboundPeek1.ServerName, serverName)
		}
		if outboundPeek1.RenewalInterval != renewalInterval {
			t.Fatalf("unexpected outbound peek renewal interval: %d, want %d", outboundPeek1.RenewalInterval, renewalInterval)
		}
		// Renew the peek
		if err = db.RenewOutboundPeek(ctx, serverName, room.ID, peekID, 2000); err != nil {
			t.Fatal(err)
		}

		// verify the values changed
		outboundPeek2, err := db.GetOutboundPeek(ctx, serverName, room.ID, peekID)
		if err != nil {
			t.Fatal(err)
		}
		if reflect.DeepEqual(outboundPeek1, outboundPeek2) {
			t.Fatal("expected a change peek, but they are the same")
		}
		if outboundPeek1.ServerName != outboundPeek2.ServerName {
			t.Fatalf("unexpected servername change: %s -> %s", outboundPeek1.ServerName, outboundPeek2.ServerName)
		}
		if outboundPeek1.RoomID != outboundPeek2.RoomID {
			t.Fatalf("unexpected roomID change: %s -> %s", outboundPeek1.RoomID, outboundPeek2.RoomID)
		}

		// insert some peeks
		peekIDs := []string{peekID}
		for i := 0; i < 5; i++ {
			peekID = util.RandomString(8)
			if err = db.AddOutboundPeek(ctx, serverName, room.ID, peekID, 1000); err != nil {
				t.Fatal(err)
			}
			peekIDs = append(peekIDs, peekID)
		}

		// Now select them
		outboundPeeks, err := db.GetOutboundPeeks(ctx, room.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(outboundPeeks) != len(peekIDs) {
			t.Fatalf("inserted %d peeks, selected %d", len(peekIDs), len(outboundPeeks))
		}
		gotPeekIDs := make([]string, 0, len(outboundPeeks))
		for _, p := range outboundPeeks {
			gotPeekIDs = append(gotPeekIDs, p.PeekID)
		}
		assert.ElementsMatch(t, gotPeekIDs, peekIDs)
	})
}

func TestInboundPeeking(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	_, serverName, _ := gomatrixserverlib.SplitID('@', alice.ID)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		db := mustCreateFederationDatabase(ctx, svc, cfg, t, testOpts)

		peekID := util.RandomString(8)
		var renewalInterval int64 = 1000

		// Add inbound peek
		if err := db.AddInboundPeek(ctx, serverName, room.ID, peekID, renewalInterval); err != nil {
			t.Fatal(err)
		}

		// select the newly inserted peek
		inboundPeek1, err := db.GetInboundPeek(ctx, serverName, room.ID, peekID)
		if err != nil {
			t.Fatal(err)
		}

		// Assert fields are set as expected
		if inboundPeek1.PeekID != peekID {
			t.Fatalf("unexpected inbound peek ID: %s, want %s", inboundPeek1.PeekID, peekID)
		}
		if inboundPeek1.RoomID != room.ID {
			t.Fatalf("unexpected inbound peek room ID: %s, want %s", inboundPeek1.RoomID, peekID)
		}
		if inboundPeek1.ServerName != serverName {
			t.Fatalf("unexpected inbound peek servername: %s, want %s", inboundPeek1.ServerName, serverName)
		}
		if inboundPeek1.RenewalInterval != renewalInterval {
			t.Fatalf("unexpected inbound peek renewal interval: %d, want %d", inboundPeek1.RenewalInterval, renewalInterval)
		}
		// Renew the peek
		if err = db.RenewInboundPeek(ctx, serverName, room.ID, peekID, 2000); err != nil {
			t.Fatal(err)
		}

		// verify the values changed
		inboundPeek2, err := db.GetInboundPeek(ctx, serverName, room.ID, peekID)
		if err != nil {
			t.Fatal(err)
		}
		if reflect.DeepEqual(inboundPeek1, inboundPeek2) {
			t.Fatal("expected a change peek, but they are the same")
		}
		if inboundPeek1.ServerName != inboundPeek2.ServerName {
			t.Fatalf("unexpected servername change: %s -> %s", inboundPeek1.ServerName, inboundPeek2.ServerName)
		}
		if inboundPeek1.RoomID != inboundPeek2.RoomID {
			t.Fatalf("unexpected roomID change: %s -> %s", inboundPeek1.RoomID, inboundPeek2.RoomID)
		}

		// insert some peeks
		peekIDs := []string{peekID}
		for i := 0; i < 5; i++ {
			peekID = util.RandomString(8)
			if err = db.AddInboundPeek(ctx, serverName, room.ID, peekID, 1000); err != nil {
				t.Fatal(err)
			}
			peekIDs = append(peekIDs, peekID)
		}

		// Now select them
		inboundPeeks, err := db.GetInboundPeeks(ctx, room.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(inboundPeeks) != len(peekIDs) {
			t.Fatalf("inserted %d peeks, selected %d", len(peekIDs), len(inboundPeeks))
		}
		gotPeekIDs := make([]string, 0, len(inboundPeeks))
		for _, p := range inboundPeeks {
			gotPeekIDs = append(gotPeekIDs, p.PeekID)
		}
		assert.ElementsMatch(t, gotPeekIDs, peekIDs)
	})
}

func TestServersAssumedOffline(t *testing.T) {
	server1 := spec.ServerName("server1")
	server2 := spec.ServerName("server2")

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		db := mustCreateFederationDatabase(ctx, svc, cfg, t, testOpts)

		// Set server1 & server2 as assumed offline.
		err := db.SetServerAssumedOffline(ctx, server1)
		assert.Nil(t, err)
		err = db.SetServerAssumedOffline(ctx, server2)
		assert.Nil(t, err)

		// Ensure both servers are assumed offline.
		isOffline, err := db.IsServerAssumedOffline(ctx, server1)
		assert.Nil(t, err)
		assert.True(t, isOffline)
		isOffline, err = db.IsServerAssumedOffline(ctx, server2)
		assert.Nil(t, err)
		assert.True(t, isOffline)

		// Set server1 as not assumed offline.
		err = db.RemoveServerAssumedOffline(ctx, server1)
		assert.Nil(t, err)

		// Ensure both servers have correct state.
		isOffline, err = db.IsServerAssumedOffline(ctx, server1)
		assert.Nil(t, err)
		assert.False(t, isOffline)
		isOffline, err = db.IsServerAssumedOffline(ctx, server2)
		assert.Nil(t, err)
		assert.True(t, isOffline)

		// Re-set server1 as assumed offline.
		err = db.SetServerAssumedOffline(ctx, server1)
		assert.Nil(t, err)

		// Ensure server1 is assumed offline.
		isOffline, err = db.IsServerAssumedOffline(ctx, server1)
		assert.Nil(t, err)
		assert.True(t, isOffline)

		err = db.RemoveAllServersAssumedOffline(ctx)
		assert.Nil(t, err)

		// Ensure both servers have correct state.
		isOffline, err = db.IsServerAssumedOffline(ctx, server1)
		assert.Nil(t, err)
		assert.False(t, isOffline)
		isOffline, err = db.IsServerAssumedOffline(ctx, server2)
		assert.Nil(t, err)
		assert.False(t, isOffline)
	})
}

func TestRelayServersStored(t *testing.T) {
	server := spec.ServerName("server")
	relayServer1 := spec.ServerName("relayserver1")
	relayServer2 := spec.ServerName("relayserver2")

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateFederationDatabase(ctx, svc, cfg, t, testOpts)

		err := db.P2PAddRelayServersForServer(ctx, server, []spec.ServerName{relayServer1})
		assert.Nil(t, err)

		relayServers, err := db.P2PGetRelayServersForServer(ctx, server)
		assert.Nil(t, err)
		assert.Equal(t, relayServer1, relayServers[0])

		err = db.P2PRemoveRelayServersForServer(ctx, server, []spec.ServerName{relayServer1})
		assert.Nil(t, err)

		relayServers, err = db.P2PGetRelayServersForServer(ctx, server)
		assert.Nil(t, err)
		assert.Zero(t, len(relayServers))

		err = db.P2PAddRelayServersForServer(ctx, server, []spec.ServerName{relayServer1, relayServer2})
		assert.Nil(t, err)

		relayServers, err = db.P2PGetRelayServersForServer(ctx, server)
		assert.Nil(t, err)
		assert.Equal(t, relayServer1, relayServers[0])
		assert.Equal(t, relayServer2, relayServers[1])

		err = db.P2PRemoveAllRelayServersForServer(ctx, server)
		assert.Nil(t, err)

		relayServers, err = db.P2PGetRelayServersForServer(ctx, server)
		assert.Nil(t, err)
		assert.Zero(t, len(relayServers))
	})
}
