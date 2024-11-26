package tables_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

func mustCreateMembershipTable(t *testing.T, _ test.DependancyOption) (tab tables.Membership, stateKeyTab tables.EventStateKeys, close func()) {
	t.Helper()

	ctx := context.TODO()
	connStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: connStr,
	}, sqlutil.NewExclusiveWriter())
	assert.NoError(t, err)
	err = postgres.CreateEventStateKeysTable(db)
	assert.NoError(t, err)
	err = postgres.CreateMembershipTable(db)
	assert.NoError(t, err)
	tab, err = postgres.PrepareMembershipTable(db)
	assert.NoError(t, err)
	stateKeyTab, err = postgres.PrepareEventStateKeysTable(db)

	assert.NoError(t, err)

	return tab, stateKeyTab, closeDb
}

func TestMembershipTable(t *testing.T) {
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		tab, stateKeyTab, closeFn := mustCreateMembershipTable(t, testOpts)
		defer closeFn()

		userNIDs := make([]types.EventStateKeyNID, 0, 10)
		for i := 0; i < 10; i++ {
			stateKeyNID, err := stateKeyTab.InsertEventStateKeyNID(ctx, nil, fmt.Sprintf("@dummy%d:localhost", i))
			assert.NoError(t, err)
			userNIDs = append(userNIDs, stateKeyNID)
			// This inserts a left user to the room
			err = tab.InsertMembership(ctx, nil, 1, stateKeyNID, true)
			assert.NoError(t, err)
			// We must update the membership with a non-zero event NID or it will get filtered out in later queries
			_, err = tab.UpdateMembership(ctx, nil, 1, stateKeyNID, userNIDs[0], tables.MembershipStateLeaveOrBan, 1, false)
			assert.NoError(t, err)
		}

		// ... so this should be false
		inRoom, err := tab.SelectLocalServerInRoom(ctx, nil, 1)
		assert.NoError(t, err)
		assert.False(t, inRoom)

		changed, err := tab.UpdateMembership(ctx, nil, 1, userNIDs[0], userNIDs[0], tables.MembershipStateJoin, 1, false)
		assert.NoError(t, err)
		assert.True(t, changed)

		// ... should now be true
		inRoom, err = tab.SelectLocalServerInRoom(ctx, nil, 1)
		assert.NoError(t, err)
		assert.True(t, inRoom)

		userJoinedToRooms, err := tab.SelectJoinedUsersSetForRooms(ctx, nil, []types.RoomNID{1}, userNIDs, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(userJoinedToRooms))

		// Get all left/banned users
		eventNIDs, err := tab.SelectMembershipsFromRoomAndMembership(ctx, nil, 1, tables.MembershipStateLeaveOrBan, true)
		assert.NoError(t, err)
		assert.Equal(t, 9, len(eventNIDs))

		_, membershipState, forgotten, err := tab.SelectMembershipFromRoomAndTarget(ctx, nil, 1, userNIDs[5])
		assert.NoError(t, err)
		assert.False(t, forgotten)
		assert.Equal(t, tables.MembershipStateLeaveOrBan, membershipState)

		// Get all members, regardless of state
		members, err := tab.SelectMembershipsFromRoom(ctx, nil, 1, true)
		assert.NoError(t, err)
		assert.Equal(t, 10, len(members))

		// Get correct user
		roomNIDs, err := tab.SelectRoomsWithMembership(ctx, nil, userNIDs[1], tables.MembershipStateLeaveOrBan)
		assert.NoError(t, err)
		assert.Equal(t, []types.RoomNID{1}, roomNIDs)

		// User is not joined to room
		roomNIDs, err = tab.SelectRoomsWithMembership(ctx, nil, userNIDs[5], tables.MembershipStateJoin)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(roomNIDs))

		// Forget room
		err = tab.UpdateForgetMembership(ctx, nil, 1, userNIDs[0], true)
		assert.NoError(t, err)

		// should now return true
		_, _, forgotten, err = tab.SelectMembershipFromRoomAndTarget(ctx, nil, 1, userNIDs[0])
		assert.NoError(t, err)
		assert.True(t, forgotten)

		serverInRoom, err := tab.SelectServerInRoom(ctx, nil, 1, "localhost")
		assert.NoError(t, err)
		assert.True(t, serverInRoom)

		serverInRoom, err = tab.SelectServerInRoom(ctx, nil, 1, "notJoined")
		assert.NoError(t, err)
		assert.False(t, serverInRoom)

		// get all users we know about; should be only one user, since no other user joined the room
		knownUsers, err := tab.SelectKnownUsers(ctx, nil, userNIDs[0], "localhost", 2)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(knownUsers))

		// get users we share a room with, given their userNID
		joinedUsers, err := tab.SelectJoinedUsers(ctx, nil, userNIDs)
		assert.NoError(t, err)
		// Only userNIDs[0] is actually joined, so we only expect this userNID
		assert.Equal(t, userNIDs[:1], joinedUsers)
	})
}
