package tables_test

import (
	"context"
	"testing"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/matrix-org/util"
	"github.com/stretchr/testify/assert"
)

func mustCreatePreviousEventsTable(t *testing.T, _ test.DependancyOption) (tab tables.PreviousEvents, closeDb func()) {
	t.Helper()

	ctx := context.TODO()
	connStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: connStr,
	}, sqlutil.NewExclusiveWriter())
	assert.NoError(t, err)
	err = postgres.CreatePrevEventsTable(db)
	assert.NoError(t, err)
	tab, err = postgres.PreparePrevEventsTable(db)

	assert.NoError(t, err)

	return tab, closeDb
}

func TestPreviousEventsTable(t *testing.T) {
	ctx := context.Background()
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		tab, closeFn := mustCreatePreviousEventsTable(t, testOpts)
		defer closeFn()

		for _, x := range room.Events() {
			for _, eventID := range x.PrevEventIDs() {
				err := tab.InsertPreviousEvent(ctx, nil, eventID, 1)
				assert.NoError(t, err)

				err = tab.SelectPreviousEventExists(ctx, nil, eventID)
				assert.NoError(t, err)
			}
		}

		// RandomString should fail and return sql.ErrNoRows
		err := tab.SelectPreviousEventExists(ctx, nil, util.RandomString(16))
		assert.Error(t, err)
	})
}
