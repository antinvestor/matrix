package tables_test

import (
	"context"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/pitabwire/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func mustCreatePreviousEventsTable(ctx context.Context, t *testing.T, dep test.DependancyOption) (tab tables.PreviousEvents, closeDb func()) {
	t.Helper()

	db, closeDb := migrateDatabase(ctx, t, dep)

	tab, err := postgres.NewPostgresPreviousEventsTable(ctx, db)

	assert.NoError(t, err)

	return tab, closeDb
}

func TestPreviousEventsTable(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx := testrig.NewContext(t)
		tab, closeFn := mustCreatePreviousEventsTable(ctx, t, testOpts)
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
