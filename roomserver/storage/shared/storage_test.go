package shared_test

import (
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/cacheutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/shared"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/pitabwire/frame"
	"github.com/stretchr/testify/assert"
	ed255192 "golang.org/x/crypto/ed25519"
)

func mustCreateRoomServerDatabase(ctx context.Context, svc *frame.Service, cfg *config.Matrix, t *testing.T, _ test.DependancyOption) *shared.Database {
	t.Helper()

	cache, err := cacheutil.NewCache(&cfg.Global.Cache)

	if err != nil {
		t.Fatalf("Could not create cache %s", err)
	}

	cm := sqlutil.NewConnectionManager(svc)

	var membershipTable tables.Membership
	var stateKeyTable tables.EventStateKeys
	var userRoomKeys tables.UserRoomKeys
	var roomsTable tables.Rooms

	roomsTable, err = postgres.NewPostgresRoomsTable(ctx, cm)
	assert.NoError(t, err)
	membershipTable, err = postgres.NewPostgresMembershipTable(ctx, cm)
	assert.NoError(t, err)
	stateKeyTable, err = postgres.NewPostgresEventStateKeysTable(ctx, cm)
	assert.NoError(t, err)
	userRoomKeys, err = postgres.NewPostgresUserRoomKeysTable(ctx, cm)

	assert.NoError(t, err)

	err = cm.Migrate(ctx)
	if err != nil {
		t.Fatalf("failed to migrate table: %s", err)
	}

	evDb := shared.EventDatabase{
		Cm:                  cm,
		EventStateKeysTable: stateKeyTable, Cache: cache}

	return &shared.Database{
		EventDatabase:    evDb,
		MembershipTable:  membershipTable,
		UserRoomKeyTable: userRoomKeys,
		RoomsTable:       roomsTable,
		Cache:            cache,
	}
}

func Test_GetLeftUsers(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	charlie := test.NewUser(t)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		db := mustCreateRoomServerDatabase(ctx, svc, cfg, t, testOpts)

		// Create dummy entries
		for _, user := range []*test.User{alice, bob, charlie} {
			nid, err := db.EventStateKeysTable.InsertEventStateKeyNID(ctx, user.ID)
			assert.NoError(t, err)
			err = db.MembershipTable.InsertMembership(ctx, 1, nid, true)
			assert.NoError(t, err)
			// We must update the membership with a non-zero event NID or it will get filtered out in later queries
			membershipNID := tables.MembershipStateLeaveOrBan
			if user == alice {
				membershipNID = tables.MembershipStateJoin
			}
			_, err = db.MembershipTable.UpdateMembership(ctx, 1, nid, nid, membershipNID, 1, false)
			assert.NoError(t, err)
		}

		// Now try to get the left users, this should be Bob and Charlie, since they have a "leave" membership
		expectedUserIDs := []string{bob.ID, charlie.ID}
		leftUsers, err := db.GetLeftUsers(ctx, []string{alice.ID, bob.ID, charlie.ID})
		assert.NoError(t, err)
		assert.ElementsMatch(t, expectedUserIDs, leftUsers)
	})
}

func TestUserRoomKeys(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)

	userID, err := spec.NewUserID(alice.ID, true)
	assert.NoError(t, err)
	roomID, err := spec.NewRoomID(room.ID)
	assert.NoError(t, err)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		db := mustCreateRoomServerDatabase(ctx, svc, cfg, t, testOpts)

		// create a room NID so we can query the room
		_, err = db.RoomsTable.InsertRoomNID(ctx, roomID.String(), gomatrixserverlib.RoomVersionV10)
		assert.NoError(t, err)
		doesNotExist, err := spec.NewRoomID("!doesnotexist:localhost")
		assert.NoError(t, err)
		_, err = db.RoomsTable.InsertRoomNID(ctx, doesNotExist.String(), gomatrixserverlib.RoomVersionV10)
		assert.NoError(t, err)

		_, key, err := ed25519.GenerateKey(nil)
		assert.NoError(t, err)

		gotKey, err := db.InsertUserRoomPrivatePublicKey(ctx, *userID, *roomID, key)
		assert.NoError(t, err)
		assert.Equal(t, gotKey, key)

		// again, this shouldn't result in an error, but return the existing key
		_, key2, err := ed25519.GenerateKey(nil)
		assert.NoError(t, err)
		gotKey, err = db.InsertUserRoomPrivatePublicKey(ctx, *userID, *roomID, key2)
		assert.NoError(t, err)
		assert.Equal(t, gotKey, key)

		gotKey, err = db.SelectUserRoomPrivateKey(ctx, *userID, *roomID)
		assert.NoError(t, err)
		assert.Equal(t, key, gotKey)
		pubKey, err := db.SelectUserRoomPublicKey(ctx, *userID, *roomID)
		assert.NoError(t, err)
		assert.Equal(t, key.Public(), pubKey)

		// Key doesn't exist, we shouldn't get anything back
		gotKey, err = db.SelectUserRoomPrivateKey(ctx, *userID, *doesNotExist)
		assert.NoError(t, err)
		assert.Nil(t, gotKey)
		pubKey, err = db.SelectUserRoomPublicKey(ctx, *userID, *doesNotExist)
		assert.NoError(t, err)
		assert.Nil(t, pubKey)

		queryUserIDs := map[spec.RoomID][]ed25519.PublicKey{
			*roomID: {key.Public().(ed25519.PublicKey)},
		}

		userIDs, err := db.SelectUserIDsForPublicKeys(ctx, queryUserIDs)
		assert.NoError(t, err)
		wantKeys := map[spec.RoomID]map[string]string{
			*roomID: {
				spec.Base64Bytes(key.Public().(ed25519.PublicKey)).Encode(): userID.String(),
			},
		}
		assert.Equal(t, wantKeys, userIDs)

		// insert key that came in over federation
		var gotPublicKey, key4 ed255192.PublicKey
		key4, _, err = ed25519.GenerateKey(nil)
		assert.NoError(t, err)
		gotPublicKey, err = db.InsertUserRoomPublicKey(ctx, *userID, *doesNotExist, key4)
		assert.NoError(t, err)
		assert.Equal(t, key4, gotPublicKey)

		// test invalid room
		reallyDoesNotExist, err := spec.NewRoomID("!reallydoesnotexist:localhost")
		assert.NoError(t, err)
		_, err = db.InsertUserRoomPublicKey(ctx, *userID, *reallyDoesNotExist, key4)
		assert.Error(t, err)
		_, err = db.InsertUserRoomPrivatePublicKey(ctx, *userID, *reallyDoesNotExist, key)
		assert.Error(t, err)
	})
}

func TestAssignRoomNID(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)

	roomID, err := spec.NewRoomID(room.ID)
	assert.NoError(t, err)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		db := mustCreateRoomServerDatabase(ctx, svc, cfg, t, testOpts)

		nid, err := db.AssignRoomNID(ctx, *roomID, room.Version)
		assert.NoError(t, err)
		assert.Greater(t, nid, types.EventNID(0))

		_, err = db.AssignRoomNID(ctx, spec.RoomID{}, "notaroomversion")
		assert.Error(t, err)
	})
}
