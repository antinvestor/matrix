package tables_test

import (
	"context"
	"github.com/pitabwire/frame"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/pitabwire/util"
	"github.com/stretchr/testify/assert"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/test"
)

func mustCreateInviteTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) tables.Invites {
	t.Helper()

	cm := sqlutil.NewConnectionManager(svc)
	var tab tables.Invites
	tab, err := postgres.NewPostgresInvitesTable(ctx, cm)
	assert.NoError(t, err)

	return tab
}

func TestInviteTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		tab := mustCreateInviteTable(ctx, svc, t, testOpts)

		eventID1 := util.RandomString(16)
		roomNID := types.RoomNID(1)
		targetUserNID, senderUserNID := types.EventStateKeyNID(1), types.EventStateKeyNID(2)
		newInvite, err := tab.InsertInviteEvent(ctx, eventID1, roomNID, targetUserNID, senderUserNID, []byte(""))
		assert.NoError(t, err)
		assert.True(t, newInvite)

		// Try adding the same invite again
		newInvite, err = tab.InsertInviteEvent(ctx, eventID1, roomNID, targetUserNID, senderUserNID, []byte(""))
		assert.NoError(t, err)
		assert.False(t, newInvite)

		// Add another invite for this room
		eventID2 := util.RandomString(16)
		newInvite, err = tab.InsertInviteEvent(ctx, eventID2, roomNID, targetUserNID, senderUserNID, []byte(""))
		assert.NoError(t, err)
		assert.True(t, newInvite)

		// Add another invite for a different user
		eventID := util.RandomString(16)
		newInvite, err = tab.InsertInviteEvent(ctx, eventID, types.RoomNID(3), targetUserNID, senderUserNID, []byte(""))
		assert.NoError(t, err)
		assert.True(t, newInvite)

		stateKeyNIDs, eventIDs, _, err := tab.SelectInviteActiveForUserInRoom(ctx, targetUserNID, roomNID)
		assert.NoError(t, err)
		assert.Equal(t, []string{eventID1, eventID2}, eventIDs)
		assert.Equal(t, []types.EventStateKeyNID{2, 2}, stateKeyNIDs)

		// retire the invite
		retiredEventIDs, err := tab.UpdateInviteRetired(ctx, roomNID, targetUserNID)
		assert.NoError(t, err)
		assert.Equal(t, []string{eventID1, eventID2}, retiredEventIDs)

		// This should now be empty
		stateKeyNIDs, eventIDs, _, err = tab.SelectInviteActiveForUserInRoom(ctx, targetUserNID, roomNID)
		assert.NoError(t, err)
		assert.Empty(t, eventIDs)
		assert.Empty(t, stateKeyNIDs)

		// Non-existent targetUserNID
		stateKeyNIDs, eventIDs, _, err = tab.SelectInviteActiveForUserInRoom(ctx, types.EventStateKeyNID(10), roomNID)
		assert.NoError(t, err)
		assert.Empty(t, stateKeyNIDs)
		assert.Empty(t, eventIDs)
	})
}
