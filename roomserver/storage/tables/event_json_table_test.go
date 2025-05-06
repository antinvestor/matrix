package tables_test

import (
	"context"
	"fmt"
	"github.com/pitabwire/frame"
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

func migrateDatabase(ctx context.Context, svc *frame.Service, t *testing.T) *sqlutil.Connections {

	cm := sqlutil.NewConnectionManager(svc)
	_, err := storage.Open(ctx, cm, nil)
	if err != nil {
		t.Fatalf("failed to create sync DB: %s", err)
	}

	return cm
}

func mustCreateEventJSONTable(ctx context.Context, svc *frame.Service, t *testing.T, dep test.DependancyOption) tables.EventJSON {
	t.Helper()

	cm := migrateDatabase(ctx, svc, t)

	return postgres.NewPostgresEventJSONTable(cm)

}

func Test_EventJSONTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		tab := mustCreateEventJSONTable(ctx, svc, t, testOpts)

		// create some dummy data
		for i := 0; i < 10; i++ {
			err := tab.InsertEventJSON(
				ctx, types.EventNID(i),
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
				values, err := tab.BulkSelectEventJSON(ctx, tc.args)
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
