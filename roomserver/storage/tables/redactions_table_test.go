package tables_test

import (
	"context"
	"github.com/antinvestor/matrix/test/testrig"
	"testing"

	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/test"
	"github.com/pitabwire/util"
	"github.com/stretchr/testify/assert"
)

func mustCreateRedactionsTable(ctx context.Context, t *testing.T, dep test.DependancyOption) (tab tables.Redactions, closeDb func()) {
	t.Helper()

	db, closeDb := migrateDatabase(ctx, t, dep)
	tab, err := postgres.NewPostgresRedactionsTable(ctx, db)

	assert.NoError(t, err)

	return tab, closeDb
}

func TestRedactionsTable(t *testing.T) {

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx := testrig.NewContext(t)
		tab, closeFn := mustCreateRedactionsTable(ctx, t, testOpts)
		defer closeFn()

		// insert and verify some redactions
		for i := 0; i < 10; i++ {
			redactionEventID, redactsEventID := util.RandomString(16), util.RandomString(16)
			wantRedactionInfo := tables.RedactionInfo{
				Validated:        false,
				RedactsEventID:   redactsEventID,
				RedactionEventID: redactionEventID,
			}
			err := tab.InsertRedaction(ctx, nil, wantRedactionInfo)
			assert.NoError(t, err)

			// verify the redactions are inserted as expected
			redactionInfo, err := tab.SelectRedactionInfoByRedactionEventID(ctx, nil, redactionEventID)
			assert.NoError(t, err)
			assert.Equal(t, &wantRedactionInfo, redactionInfo)

			redactionInfo, err = tab.SelectRedactionInfoByEventBeingRedacted(ctx, nil, redactsEventID)
			assert.NoError(t, err)
			assert.Equal(t, &wantRedactionInfo, redactionInfo)

			// redact event
			err = tab.MarkRedactionValidated(ctx, nil, redactionEventID, true)
			assert.NoError(t, err)

			wantRedactionInfo.Validated = true
			redactionInfo, err = tab.SelectRedactionInfoByRedactionEventID(ctx, nil, redactionEventID)
			assert.NoError(t, err)
			assert.Equal(t, &wantRedactionInfo, redactionInfo)
		}

		// Should not fail, it just updates 0 rows
		err := tab.MarkRedactionValidated(ctx, nil, "iDontExist", true)
		assert.NoError(t, err)

		// Should also not fail, but return a nil redactionInfo
		redactionInfo, err := tab.SelectRedactionInfoByRedactionEventID(ctx, nil, "iDontExist")
		assert.NoError(t, err)
		assert.Nil(t, redactionInfo)

		redactionInfo, err = tab.SelectRedactionInfoByEventBeingRedacted(ctx, nil, "iDontExist")
		assert.NoError(t, err)
		assert.Nil(t, redactionInfo)
	})
}
