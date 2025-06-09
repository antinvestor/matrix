package tables_test

import (
	"context"
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

func mustCreateStateSnapshotTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) tables.StateSnapshot {
	t.Helper()

	cm := sqlutil.NewConnectionManager(svc)

	// for the PostgreSQL history visibility optimisation to work,
	// we also need some other tables to exist
	_, err := postgres.NewPostgresEventStateKeysTable(ctx, cm)
	assert.NoError(t, err)
	_, err = postgres.NewPostgresEventsTable(ctx, cm)
	assert.NoError(t, err)
	_, err = postgres.NewPostgresEventJSONTable(ctx, cm)
	assert.NoError(t, err)
	_, err = postgres.NewPostgresStateBlockTable(ctx, cm)
	assert.NoError(t, err)
	// ... and then the snapshot table itself
	tab, err := postgres.NewPostgresStateSnapshotTable(ctx, cm)
	assert.NoError(t, err)

	err = cm.Migrate(ctx)
	if err != nil {
		t.Fatal("failed to migrate table: %s", err)
	}

	return tab
}

func TestStateSnapshotTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		tab := mustCreateStateSnapshotTable(ctx, svc, t, testOpts)

		// generate some dummy data
		var stateBlockNIDs types.StateBlockNIDs
		for i := 0; i < 100; i++ {
			stateBlockNIDs = append(stateBlockNIDs, types.StateBlockNID(i))
		}
		stateNID, err := tab.InsertState(ctx, 1, stateBlockNIDs)
		assert.NoError(t, err)
		assert.Equal(t, types.StateSnapshotNID(1), stateNID)

		// verify ON CONFLICT; Note: this updates the sequence!
		stateNID, err = tab.InsertState(ctx, 1, stateBlockNIDs)
		assert.NoError(t, err)
		assert.Equal(t, types.StateSnapshotNID(1), stateNID)

		// create a second snapshot
		var stateBlockNIDs2 types.StateBlockNIDs
		for i := 100; i < 150; i++ {
			stateBlockNIDs2 = append(stateBlockNIDs2, types.StateBlockNID(i))
		}

		stateNID, err = tab.InsertState(ctx, 1, stateBlockNIDs2)
		assert.NoError(t, err)
		// StateSnapshotNID is now 3, since the DO UPDATE SET statement incremented the sequence
		assert.Equal(t, types.StateSnapshotNID(3), stateNID)

		nidLists, err := tab.BulkSelectStateBlockNIDs(ctx, []types.StateSnapshotNID{1, 3})
		assert.NoError(t, err)
		assert.Equal(t, stateBlockNIDs, types.StateBlockNIDs(nidLists[0].StateBlockNIDs))
		assert.Equal(t, stateBlockNIDs2, types.StateBlockNIDs(nidLists[1].StateBlockNIDs))

		// check we get an error if the state snapshot does not exist
		_, err = tab.BulkSelectStateBlockNIDs(ctx, []types.StateSnapshotNID{2})
		assert.Error(t, err)

		// create a second snapshot
		for i := 0; i < 65555; i++ {
			stateBlockNIDs2 = append(stateBlockNIDs2, types.StateBlockNID(i))
		}
		_, err = tab.InsertState(ctx, 1, stateBlockNIDs2)
		assert.NoError(t, err)
	})
}
