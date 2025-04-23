package tables_test

import (
	"context"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/pitabwire/util"
	"github.com/stretchr/testify/assert"

	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/test"
)

func mustCreateInviteTable(t *testing.T, ctx context.Context, dep test.DependancyOption) (tables.Invites, func()) {
	t.Helper()

	db, closeDb := migrateDatabase(ctx, t, dep)
	tab, err := postgres.NewPostgresInvitesTable(ctx, db)

	assert.NoError(t, err)

	return tab, closeDb
}

func TestInviteTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx := testrig.NewContext(t)
		tab, closeFn := mustCreateInviteTable(t, ctx, testOpts)
		defer closeFn()
		eventID1 := util.RandomString(16)
		roomNID := types.RoomNID(1)
		targetUserNID, senderUserNID := types.EventStateKeyNID(1), types.EventStateKeyNID(2)
		newInvite, err := tab.InsertInviteEvent(ctx, nil, eventID1, roomNID, targetUserNID, senderUserNID, []byte(""))
		assert.NoError(t, err)
		assert.True(t, newInvite)

		// Try adding the same invite again
		newInvite, err = tab.InsertInviteEvent(ctx, nil, eventID1, roomNID, targetUserNID, senderUserNID, []byte(""))
		assert.NoError(t, err)
		assert.False(t, newInvite)

		// Add another invite for this room
		eventID2 := util.RandomString(16)
		newInvite, err = tab.InsertInviteEvent(ctx, nil, eventID2, roomNID, targetUserNID, senderUserNID, []byte(""))
		assert.NoError(t, err)
		assert.True(t, newInvite)

		// Add another invite for a different user
		eventID := util.RandomString(16)
		newInvite, err = tab.InsertInviteEvent(ctx, nil, eventID, types.RoomNID(3), targetUserNID, senderUserNID, []byte(""))
		assert.NoError(t, err)
		assert.True(t, newInvite)

		stateKeyNIDs, eventIDs, _, err := tab.SelectInviteActiveForUserInRoom(ctx, nil, targetUserNID, roomNID)
		assert.NoError(t, err)
		assert.Equal(t, []string{eventID1, eventID2}, eventIDs)
		assert.Equal(t, []types.EventStateKeyNID{2, 2}, stateKeyNIDs)

		// retire the invite
		retiredEventIDs, err := tab.UpdateInviteRetired(ctx, nil, roomNID, targetUserNID)
		assert.NoError(t, err)
		assert.Equal(t, []string{eventID1, eventID2}, retiredEventIDs)

		// This should now be empty
		stateKeyNIDs, eventIDs, _, err = tab.SelectInviteActiveForUserInRoom(ctx, nil, targetUserNID, roomNID)
		assert.NoError(t, err)
		assert.Empty(t, eventIDs)
		assert.Empty(t, stateKeyNIDs)

		// Non-existent targetUserNID
		stateKeyNIDs, eventIDs, _, err = tab.SelectInviteActiveForUserInRoom(ctx, nil, types.EventStateKeyNID(10), roomNID)
		assert.NoError(t, err)
		assert.Empty(t, stateKeyNIDs)
		assert.Empty(t, eventIDs)
	})
}
