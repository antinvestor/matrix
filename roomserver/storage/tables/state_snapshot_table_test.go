package tables_test

import (
	"context"
	"testing"

	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/stretchr/testify/assert"
)

func mustCreateStateSnapshotTable(ctx context.Context, t *testing.T, dep test.DependancyOption) (tab tables.StateSnapshot, close func()) {
	t.Helper()

	db, closeDb := migrateDatabase(ctx, t, dep)

	tab, err := postgres.NewPostgresStateSnapshotTable(ctx, db)
	assert.NoError(t, err)

	return tab, closeDb
}

func TestStateSnapshotTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx := testrig.NewContext(t)
		tab, closeDb := mustCreateStateSnapshotTable(ctx, t, testOpts)
		defer closeDb()

		// generate some dummy data
		var stateBlockNIDs types.StateBlockNIDs
		for i := 0; i < 100; i++ {
			stateBlockNIDs = append(stateBlockNIDs, types.StateBlockNID(i))
		}
		stateNID, err := tab.InsertState(ctx, nil, 1, stateBlockNIDs)
		assert.NoError(t, err)
		assert.Equal(t, types.StateSnapshotNID(1), stateNID)

		// verify ON CONFLICT; Note: this updates the sequence!
		stateNID, err = tab.InsertState(ctx, nil, 1, stateBlockNIDs)
		assert.NoError(t, err)
		assert.Equal(t, types.StateSnapshotNID(1), stateNID)

		// create a second snapshot
		var stateBlockNIDs2 types.StateBlockNIDs
		for i := 100; i < 150; i++ {
			stateBlockNIDs2 = append(stateBlockNIDs2, types.StateBlockNID(i))
		}

		stateNID, err = tab.InsertState(ctx, nil, 1, stateBlockNIDs2)
		assert.NoError(t, err)
		// StateSnapshotNID is now 3, since the DO UPDATE SET statement incremented the sequence
		assert.Equal(t, types.StateSnapshotNID(3), stateNID)

		nidLists, err := tab.BulkSelectStateBlockNIDs(ctx, nil, []types.StateSnapshotNID{1, 3})
		assert.NoError(t, err)
		assert.Equal(t, stateBlockNIDs, types.StateBlockNIDs(nidLists[0].StateBlockNIDs))
		assert.Equal(t, stateBlockNIDs2, types.StateBlockNIDs(nidLists[1].StateBlockNIDs))

		// check we get an error if the state snapshot does not exist
		_, err = tab.BulkSelectStateBlockNIDs(ctx, nil, []types.StateSnapshotNID{2})
		assert.Error(t, err)

		// create a second snapshot
		for i := 0; i < 65555; i++ {
			stateBlockNIDs2 = append(stateBlockNIDs2, types.StateBlockNID(i))
		}
		_, err = tab.InsertState(ctx, nil, 1, stateBlockNIDs2)
		assert.NoError(t, err)
	})
}
