package tables_test

import (
	"context"
	"testing"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

func mustCreateStateBlockTable(t *testing.T, _ test.DependancyOption) (tab tables.StateBlock, close func()) {
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
	err = postgres.CreateStateBlockTable(db)
	assert.NoError(t, err)
	tab, err = postgres.PrepareStateBlockTable(db)

	assert.NoError(t, err)

	return tab, closeDb
}

func TestStateBlockTable(t *testing.T) {
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		tab, closeFn := mustCreateStateBlockTable(t, testOpts)
		defer closeFn()

		// generate some dummy data
		var entries types.StateEntries
		for i := 0; i < 100; i++ {
			entry := types.StateEntry{
				EventNID: types.EventNID(i),
			}
			entries = append(entries, entry)
		}
		stateBlockNID, err := tab.BulkInsertStateData(ctx, nil, entries)
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
		stateBlockNID, err = tab.BulkInsertStateData(ctx, nil, entries2)
		assert.NoError(t, err)
		assert.Equal(t, types.StateBlockNID(2), stateBlockNID)

		eventNIDs, err := tab.BulkSelectStateBlockEntries(ctx, nil, types.StateBlockNIDs{1, 2})
		assert.NoError(t, err)
		assert.Equal(t, len(entries), len(eventNIDs[0]))
		assert.Equal(t, len(entries2), len(eventNIDs[1]))

		// try to get a StateBlockNID which does not exist
		_, err = tab.BulkSelectStateBlockEntries(ctx, nil, types.StateBlockNIDs{5})
		assert.Error(t, err)

		// This should return an error, since we can only retrieve 1 StateBlock
		_, err = tab.BulkSelectStateBlockEntries(ctx, nil, types.StateBlockNIDs{1, 5})
		assert.Error(t, err)

		for i := 0; i < 65555; i++ {
			entry := types.StateEntry{
				EventNID: types.EventNID(i),
			}
			entries2 = append(entries2, entry)
		}
		stateBlockNID, err = tab.BulkInsertStateData(ctx, nil, entries2)
		assert.NoError(t, err)
		assert.Equal(t, types.StateBlockNID(3), stateBlockNID)
	})
}
