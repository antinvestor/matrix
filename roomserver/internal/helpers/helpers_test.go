package helpers

import (
	"context"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/stretchr/testify/assert"

	"github.com/antinvestor/matrix/roomserver/types"

	"github.com/antinvestor/matrix/roomserver/storage"
	"github.com/antinvestor/matrix/test"
)

func mustCreateDatabase(t *testing.T, _ test.DependancyOption) (context.Context, storage.Database, func()) {
	ctx := testrig.NewContext(t)
	conStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	cacheConnStr, closeCache, err := test.PrepareRedisDataSourceConnection(ctx)
	if err != nil {
		t.Fatalf("Could not create redis container %s", err)
	}

	caches, err := caching.NewCache(&config.CacheOptions{
		ConnectionString: cacheConnStr,
	})
	if err != nil {
		t.Fatalf("failed to create a cache: %v", err)
	}
	cm := sqlutil.NewConnectionManager(ctx, config.DatabaseOptions{ConnectionString: conStr})
	db, err := storage.Open(ctx, cm, &config.DatabaseOptions{ConnectionString: conStr}, caches)
	if err != nil {
		t.Fatalf("failed to create Database: %v", err)
	}
	return ctx, db, func() {
		closeCache()
		closeDb()
	}
}

func TestIsInvitePendingWithoutNID(t *testing.T) {

	alice := test.NewUser(t)
	bob := test.NewUser(t)
	room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
	_ = bob
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, db, closeDb := mustCreateDatabase(t, testOpts)
		defer closeDb()

		// store all events
		var authNIDs []types.EventNID
		for _, x := range room.Events() {

			roomInfo, err := db.GetOrCreateRoomInfo(ctx, x.PDU)
			assert.NoError(t, err)
			assert.NotNil(t, roomInfo)

			eventTypeNID, err := db.GetOrCreateEventTypeNID(ctx, x.Type())
			assert.NoError(t, err)
			assert.Greater(t, eventTypeNID, types.EventTypeNID(0))

			eventStateKeyNID, err := db.GetOrCreateEventStateKeyNID(ctx, x.StateKey())
			assert.NoError(t, err)

			evNID, _, err := db.StoreEvent(ctx, x.PDU, roomInfo, eventTypeNID, eventStateKeyNID, authNIDs, false)
			assert.NoError(t, err)
			authNIDs = append(authNIDs, evNID)
		}

		// Alice should have no pending invites and should have a NID
		pendingInvite, _, _, _, err := IsInvitePending(ctx, db, room.ID, spec.SenderID(alice.ID))
		assert.NoError(t, err, "failed to get pending invites")
		assert.False(t, pendingInvite, "unexpected pending invite")

		// Bob should have no pending invites and receive a new NID
		pendingInvite, _, _, _, err = IsInvitePending(ctx, db, room.ID, spec.SenderID(bob.ID))
		assert.NoError(t, err, "failed to get pending invites")
		assert.False(t, pendingInvite, "unexpected pending invite")
	})
}
