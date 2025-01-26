package tables_test

import (
	"context"
	"testing"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

func mustCreateRoomAliasesTable(t *testing.T, _ test.DependancyOption) (tab tables.RoomAliases, closeDb func()) {
	t.Helper()
	ctx := context.TODO()
	connStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString:   connStr,
		MaxOpenConnections: 10,
	}, sqlutil.NewExclusiveWriter())
	assert.NoError(t, err)
	err = postgres.CreateRoomAliasesTable(db)
	assert.NoError(t, err)
	tab, err = postgres.PrepareRoomAliasesTable(db)

	assert.NoError(t, err)

	return tab, closeDb
}

func TestRoomAliasesTable(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	room2 := test.NewRoom(t, alice)
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		tab, closeFn := mustCreateRoomAliasesTable(t, testOpts)
		defer closeFn()
		alias, alias2, alias3 := "#alias:localhost", "#alias2:localhost", "#alias3:localhost"
		// insert aliases
		err := tab.InsertRoomAlias(ctx, nil, alias, room.ID, alice.ID)
		assert.NoError(t, err)

		err = tab.InsertRoomAlias(ctx, nil, alias2, room.ID, alice.ID)
		assert.NoError(t, err)

		err = tab.InsertRoomAlias(ctx, nil, alias3, room2.ID, alice.ID)
		assert.NoError(t, err)

		// verify we can get the roomID for the alias
		roomID, err := tab.SelectRoomIDFromAlias(ctx, nil, alias)
		assert.NoError(t, err)
		assert.Equal(t, room.ID, roomID)

		// .. and the creator
		creator, err := tab.SelectCreatorIDFromAlias(ctx, nil, alias)
		assert.NoError(t, err)
		assert.Equal(t, alice.ID, creator)

		creator, err = tab.SelectCreatorIDFromAlias(ctx, nil, "#doesntexist:localhost")
		assert.NoError(t, err)
		assert.Equal(t, "", creator)

		roomID, err = tab.SelectRoomIDFromAlias(ctx, nil, "#doesntexist:localhost")
		assert.NoError(t, err)
		assert.Equal(t, "", roomID)

		// get all aliases for a room
		aliases, err := tab.SelectAliasesFromRoomID(ctx, nil, room.ID)
		assert.NoError(t, err)
		assert.Equal(t, []string{alias, alias2}, aliases)

		// delete an alias and verify it's deleted
		err = tab.DeleteRoomAlias(ctx, nil, alias2)
		assert.NoError(t, err)

		aliases, err = tab.SelectAliasesFromRoomID(ctx, nil, room.ID)
		assert.NoError(t, err)
		assert.Equal(t, []string{alias}, aliases)

		// deleting the same alias should be a no-op
		err = tab.DeleteRoomAlias(ctx, nil, alias2)
		assert.NoError(t, err)

		// Delete non-existent alias should be a no-op
		err = tab.DeleteRoomAlias(ctx, nil, "#doesntexist:localhost")
		assert.NoError(t, err)
	})
}
