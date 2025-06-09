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

func mustCreateStateBlockTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) tables.StateBlock {
	t.Helper()

	cm := sqlutil.NewConnectionManager(svc)

	tab, err := postgres.NewPostgresStateBlockTable(ctx, cm)
	assert.NoError(t, err)

	err = cm.Migrate(ctx)
	if err != nil {
		t.Fatalf("failed to migrate table: %s", err)
	}

	return tab
}

func TestStateBlockTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		tab := mustCreateStateBlockTable(ctx, svc, t, testOpts)

		// generate some dummy data
		var entries types.StateEntries
		for i := 0; i < 100; i++ {
			entry := types.StateEntry{
				EventNID: types.EventNID(i),
			}
			entries = append(entries, entry)
		}
		stateBlockNID, err := tab.BulkInsertStateData(ctx, entries)
		assert.NoError(t, err)
		assert.Equal(t, types.StateBlockNID(1), stateBlockNID)

		// generate a different hash, to get a new StateBlockNID
		var entries2 types.StateEntries
		for i := 100; i < 300; i++ {
			entry := types.StateEntry{
				EventNID: types.EventNID(i),
			}
			entries2 = append(entries2, entry)
		}
		stateBlockNID, err = tab.BulkInsertStateData(ctx, entries2)
		assert.NoError(t, err)
		assert.Equal(t, types.StateBlockNID(2), stateBlockNID)

		eventNIDs, err := tab.BulkSelectStateBlockEntries(ctx, types.StateBlockNIDs{1, 2})
		assert.NoError(t, err)
		assert.Equal(t, len(entries), len(eventNIDs[0]))
		assert.Equal(t, len(entries2), len(eventNIDs[1]))

		// try to get a StateBlockNID which does not exist
		_, err = tab.BulkSelectStateBlockEntries(ctx, types.StateBlockNIDs{5})
		assert.Error(t, err)

		// This should return an error, since we can only retrieve 1 StateBlock
		_, err = tab.BulkSelectStateBlockEntries(ctx, types.StateBlockNIDs{1, 5})
		assert.Error(t, err)

		for i := 0; i < 65555; i++ {
			entry := types.StateEntry{
				EventNID: types.EventNID(i),
			}
			entries2 = append(entries2, entry)
		}
		stateBlockNID, err = tab.BulkInsertStateData(ctx, entries2)
		assert.NoError(t, err)
		assert.Equal(t, types.StateBlockNID(3), stateBlockNID)
	})
}
