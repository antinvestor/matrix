package tables_test

import (
	"context"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/pitabwire/util"
	"github.com/stretchr/testify/assert"
)

func mustCreateRoomsTable(ctx context.Context, t *testing.T, _ test.DependancyOption) (tab tables.Rooms, close func()) {
	t.Helper()

	connStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString:   connStr,
		MaxOpenConnections: 10,
	}, sqlutil.NewExclusiveWriter())
	assert.NoError(t, err)
	err = postgres.CreateRoomsTable(ctx, db)
	assert.NoError(t, err)
	tab, err = postgres.PrepareRoomsTable(ctx, db)

	assert.NoError(t, err)

	return tab, closeDb
}

func TestRoomsTable(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		tab, closeFn := mustCreateRoomsTable(ctx, t, testOpts)
		defer closeFn()

		wantRoomNID, err := tab.InsertRoomNID(ctx, nil, room.ID, room.Version)
		assert.NoError(t, err)

		// Create dummy room
		_, err = tab.InsertRoomNID(ctx, nil, util.RandomString(16), room.Version)
		assert.NoError(t, err)

		gotRoomNID, err := tab.SelectRoomNID(ctx, nil, room.ID)
		assert.NoError(t, err)
		assert.Equal(t, wantRoomNID, gotRoomNID)

		// Ensure non existent roomNID errors
		roomNID, err := tab.SelectRoomNID(ctx, nil, "!doesnotexist:localhost")
		assert.Error(t, err)
		assert.Equal(t, types.RoomNID(0), roomNID)

		roomInfo, err := tab.SelectRoomInfo(ctx, nil, room.ID)
		assert.NoError(t, err)
		expected := &types.RoomInfo{
			RoomNID:     wantRoomNID,
			RoomVersion: room.Version,
		}
		expected.SetIsStub(true) // there are no latestEventNIDs
		assert.Equal(t, expected, roomInfo)

		roomInfo, err = tab.SelectRoomInfo(ctx, nil, "!doesnotexist:localhost")
		assert.NoError(t, err)
		assert.Nil(t, roomInfo)

		roomVersions, err := tab.SelectRoomVersionsForRoomNIDs(ctx, nil, []types.RoomNID{wantRoomNID, 1337})
		assert.NoError(t, err)
		assert.Equal(t, roomVersions[wantRoomNID], room.Version)
		// Room does not exist
		_, ok := roomVersions[1337]
		assert.False(t, ok)

		roomIDs, err := tab.BulkSelectRoomIDs(ctx, nil, []types.RoomNID{wantRoomNID, 1337})
		assert.NoError(t, err)
		assert.Equal(t, []string{room.ID}, roomIDs)

		roomNIDs, err := tab.BulkSelectRoomNIDs(ctx, nil, []string{room.ID, "!doesnotexist:localhost"})
		assert.NoError(t, err)
		assert.Equal(t, []types.RoomNID{wantRoomNID}, roomNIDs)

		wantEventNIDs := []types.EventNID{1, 2, 3}
		lastEventSentNID := types.EventNID(3)
		stateSnapshotNID := types.StateSnapshotNID(1)
		// make the room "usable"
		err = tab.UpdateLatestEventNIDs(ctx, nil, wantRoomNID, wantEventNIDs, lastEventSentNID, stateSnapshotNID)
		assert.NoError(t, err)

		roomInfo, err = tab.SelectRoomInfo(ctx, nil, room.ID)
		assert.NoError(t, err)
		expected = &types.RoomInfo{
			RoomNID:     wantRoomNID,
			RoomVersion: room.Version,
		}
		expected.SetStateSnapshotNID(1)
		assert.Equal(t, expected, roomInfo)

		eventNIDs, snapshotNID, err := tab.SelectLatestEventNIDs(ctx, nil, wantRoomNID)
		assert.NoError(t, err)
		assert.Equal(t, wantEventNIDs, eventNIDs)
		assert.Equal(t, types.StateSnapshotNID(1), snapshotNID)

		// Again, doesn't exist
		_, _, err = tab.SelectLatestEventNIDs(ctx, nil, 1337)
		assert.Error(t, err)

		eventNIDs, eventNID, snapshotNID, err := tab.SelectLatestEventsNIDsForUpdate(ctx, nil, wantRoomNID)
		assert.NoError(t, err)
		assert.Equal(t, wantEventNIDs, eventNIDs)
		assert.Equal(t, types.EventNID(3), eventNID)
		assert.Equal(t, types.StateSnapshotNID(1), snapshotNID)
	})
}
