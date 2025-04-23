package tables_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/antinvestor/matrix/roomserver/storage"
	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

func migrateDatabase(ctx context.Context, t *testing.T, testOpts test.DependancyOption) (*sql.DB, func()) {

	cfg, closeDB := testrig.CreateConfig(ctx, t, testOpts)
	cm := sqlutil.NewConnectionManager(ctx, cfg.Global.DatabaseOptions)
	_, err := storage.Open(ctx, cm, &cfg.RoomServer.Database, nil)
	if err != nil {
		t.Fatalf("failed to create sync DB: %s", err)
	}

	db, err := sqlutil.Open(&cfg.RoomServer.Database, sqlutil.NewExclusiveWriter())
	assert.NoError(t, err)

	return db, closeDB
}

func mustCreateEventJSONTable(ctx context.Context, t *testing.T, dep test.DependancyOption) (tables.EventJSON, context.Context, func()) {
	t.Helper()

	db, closeDb := migrateDatabase(ctx, t, dep)

	tab, err := postgres.NewPostgresEventJSONTable(ctx, db)

	assert.NoError(t, err)

	return tab, ctx, closeDb
}

func Test_EventJSONTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx := testrig.NewContext(t)
		tab, ctx, closeDb := mustCreateEventJSONTable(ctx, t, testOpts)
		defer closeDb()

		// create some dummy data
		for i := 0; i < 10; i++ {
			err := tab.InsertEventJSON(
				ctx, nil, types.EventNID(i),
				[]byte(fmt.Sprintf(`{"value":%d"}`, i)),
			)
			assert.NoError(t, err)
		}

		tests := []struct {
			name      string
			args      []types.EventNID
			wantCount int
		}{
			{
				name:      "select subset of existing NIDs",
				args:      []types.EventNID{1, 2, 3, 4, 5},
				wantCount: 5,
			},
			{
				name:      "select subset of existing/non-existing NIDs",
				args:      []types.EventNID{1, 2, 12, 50},
				wantCount: 2,
			},
			{
				name:      "select single existing NID",
				args:      []types.EventNID{1},
				wantCount: 1,
			},
			{
				name:      "select single non-existing NID",
				args:      []types.EventNID{13},
				wantCount: 0,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				// select a subset of the data
				values, err := tab.BulkSelectEventJSON(ctx, nil, tc.args)
				assert.NoError(t, err)
				assert.Equal(t, tc.wantCount, len(values))
				for i, v := range values {
					assert.Equal(t, v.EventNID, types.EventNID(i+1))
					assert.Equal(t, []byte(fmt.Sprintf(`{"value":%d"}`, i+1)), v.EventJSON)
				}
			})
		}
	})
}
