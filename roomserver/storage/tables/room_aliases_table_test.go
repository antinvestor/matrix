package tables_test

import (
	"context"
	"testing"

	"github.com/pitabwire/frame"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

func mustCreateRoomAliasesTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) tables.RoomAliases {
	t.Helper()

	cm := sqlutil.NewConnectionManager(svc)

	tab, err := postgres.NewPostgresRoomAliasesTable(ctx, cm)
	assert.NoError(t, err)

	err = cm.Migrate(ctx)
	if err != nil {
		t.Fatalf("failed to migrate table: %s", err)
	}
	return tab
}

func TestRoomAliasesTable(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	room2 := test.NewRoom(t, alice)
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		tab := mustCreateRoomAliasesTable(ctx, svc, t, testOpts)

		alias, alias2, alias3 := "#alias:localhost", "#alias2:localhost", "#alias3:localhost"
		// insert aliases
		err := tab.InsertRoomAlias(ctx, alias, room.ID, alice.ID)
		assert.NoError(t, err)

		err = tab.InsertRoomAlias(ctx, alias2, room.ID, alice.ID)
		assert.NoError(t, err)

		err = tab.InsertRoomAlias(ctx, alias3, room2.ID, alice.ID)
		assert.NoError(t, err)

		// verify we can get the roomID for the alias
		roomID, err := tab.SelectRoomIDFromAlias(ctx, alias)
		assert.NoError(t, err)
		assert.Equal(t, room.ID, roomID)

		// .. and the creator
		creator, err := tab.SelectCreatorIDFromAlias(ctx, alias)
		assert.NoError(t, err)
		assert.Equal(t, alice.ID, creator)

		creator, err = tab.SelectCreatorIDFromAlias(ctx, "#doesntexist:localhost")
		assert.NoError(t, err)
		assert.Equal(t, "", creator)

		roomID, err = tab.SelectRoomIDFromAlias(ctx, "#doesntexist:localhost")
		assert.NoError(t, err)
		assert.Equal(t, "", roomID)

		// get all aliases for a room
		aliases, err := tab.SelectAliasesFromRoomID(ctx, room.ID)
		assert.NoError(t, err)
		assert.Equal(t, []string{alias, alias2}, aliases)

		// delete an alias and verify it's deleted
		err = tab.DeleteRoomAlias(ctx, alias2)
		assert.NoError(t, err)

		aliases, err = tab.SelectAliasesFromRoomID(ctx, room.ID)
		assert.NoError(t, err)
		assert.Equal(t, []string{alias}, aliases)

		// deleting the same alias should be a no-op
		err = tab.DeleteRoomAlias(ctx, alias2)
		assert.NoError(t, err)

		// Delete non-existent alias should be a no-op
		err = tab.DeleteRoomAlias(ctx, "#doesntexist:localhost")
		assert.NoError(t, err)
	})
}
