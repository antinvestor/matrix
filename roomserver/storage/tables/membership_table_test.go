package tables_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pitabwire/frame"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

func mustCreateMembershipTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) (tab tables.Membership, stateKeyTab tables.EventStateKeys) {
	t.Helper()

	cm := sqlutil.NewConnectionManager(svc)

	tab, err := postgres.NewPostgresMembershipTable(ctx, cm)
	assert.NoError(t, err)
	stateKeyTab, err = postgres.NewPostgresEventStateKeysTable(ctx, cm)
	assert.NoError(t, err)

	err = cm.Migrate(ctx)
	if err != nil {
		t.Fatalf("failed to migrate table: %s", err)
	}

	return tab, stateKeyTab
}

func TestMembershipTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		tab, stateKeyTab := mustCreateMembershipTable(ctx, svc, t, testOpts)

		userNIDs := make([]types.EventStateKeyNID, 0, 10)
		for i := 0; i < 10; i++ {
			stateKeyNID, err := stateKeyTab.InsertEventStateKeyNID(ctx, fmt.Sprintf("@dummy%d:localhost", i))
			assert.NoError(t, err)
			userNIDs = append(userNIDs, stateKeyNID)
			// This inserts a left user to the room
			err = tab.InsertMembership(ctx, 1, stateKeyNID, true)
			assert.NoError(t, err)
			// We must update the membership with a non-zero event NID or it will get filtered out in later queries
			_, err = tab.UpdateMembership(ctx, 1, stateKeyNID, userNIDs[0], tables.MembershipStateLeaveOrBan, 1, false)
			assert.NoError(t, err)
		}

		// ... so this should be false
		inRoom, err := tab.SelectLocalServerInRoom(ctx, 1)
		assert.NoError(t, err)
		assert.False(t, inRoom)

		changed, err := tab.UpdateMembership(ctx, 1, userNIDs[0], userNIDs[0], tables.MembershipStateJoin, 1, false)
		assert.NoError(t, err)
		assert.True(t, changed)

		// ... should now be true
		inRoom, err = tab.SelectLocalServerInRoom(ctx, 1)
		assert.NoError(t, err)
		assert.True(t, inRoom)

		userJoinedToRooms, err := tab.SelectJoinedUsersSetForRooms(ctx, []types.RoomNID{1}, userNIDs, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(userJoinedToRooms))

		// Get all left/banned users
		eventNIDs, err := tab.SelectMembershipsFromRoomAndMembership(ctx, 1, tables.MembershipStateLeaveOrBan, true)
		assert.NoError(t, err)
		assert.Equal(t, 9, len(eventNIDs))

		_, membershipState, forgotten, err := tab.SelectMembershipFromRoomAndTarget(ctx, 1, userNIDs[5])
		assert.NoError(t, err)
		assert.False(t, forgotten)
		assert.Equal(t, tables.MembershipStateLeaveOrBan, membershipState)

		// Get all members, regardless of state
		members, err := tab.SelectMembershipsFromRoom(ctx, 1, true)
		assert.NoError(t, err)
		assert.Equal(t, 10, len(members))

		// Get correct user
		roomNIDs, err := tab.SelectRoomsWithMembership(ctx, userNIDs[1], tables.MembershipStateLeaveOrBan)
		assert.NoError(t, err)
		assert.Equal(t, []types.RoomNID{1}, roomNIDs)

		// User is not joined to room
		roomNIDs, err = tab.SelectRoomsWithMembership(ctx, userNIDs[5], tables.MembershipStateJoin)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(roomNIDs))

		// Forget room
		err = tab.UpdateForgetMembership(ctx, 1, userNIDs[0], true)
		assert.NoError(t, err)

		// should now return true
		_, _, forgotten, err = tab.SelectMembershipFromRoomAndTarget(ctx, 1, userNIDs[0])
		assert.NoError(t, err)
		assert.True(t, forgotten)

		serverInRoom, err := tab.SelectServerInRoom(ctx, 1, "localhost")
		assert.NoError(t, err)
		assert.True(t, serverInRoom)

		serverInRoom, err = tab.SelectServerInRoom(ctx, 1, "notJoined")
		assert.NoError(t, err)
		assert.False(t, serverInRoom)

		// get all users we know about; should be only one user, since no other user joined the room
		knownUsers, err := tab.SelectKnownUsers(ctx, userNIDs[0], "localhost", 2)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(knownUsers))

		// get users we share a room with, given their userNID
		joinedUsers, err := tab.SelectJoinedUsers(ctx, userNIDs)
		assert.NoError(t, err)
		// Only userNIDs[0] is actually joined, so we only expect this userNID
		assert.Equal(t, userNIDs[:1], joinedUsers)
	})
}
