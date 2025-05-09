package tables_test

import (
	"context"
	"github.com/pitabwire/frame"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/test"
	"github.com/pitabwire/util"
	"github.com/stretchr/testify/assert"
)

func mustCreateRedactionsTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) tables.Redactions {
	t.Helper()

	cm := sqlutil.NewConnectionManager(svc)

	tab, err := postgres.NewPostgresRedactionsTable(ctx, cm)
	assert.NoError(t, err)

	return tab
}

func TestRedactionsTable(t *testing.T) {

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		tab := mustCreateRedactionsTable(ctx, svc, t, testOpts)

		// insert and verify some redactions
		for i := 0; i < 10; i++ {
			redactionEventID, redactsEventID := util.RandomString(16), util.RandomString(16)
			wantRedactionInfo := tables.RedactionInfo{
				Validated:        false,
				RedactsEventID:   redactsEventID,
				RedactionEventID: redactionEventID,
			}
			err := tab.InsertRedaction(ctx, wantRedactionInfo)
			assert.NoError(t, err)

			// verify the redactions are inserted as expected
			redactionInfo, err := tab.SelectRedactionInfoByRedactionEventID(ctx, redactionEventID)
			assert.NoError(t, err)
			assert.Equal(t, &wantRedactionInfo, redactionInfo)

			redactionInfo, err = tab.SelectRedactionInfoByEventBeingRedacted(ctx, redactsEventID)
			assert.NoError(t, err)
			assert.Equal(t, &wantRedactionInfo, redactionInfo)

			// redact event
			err = tab.MarkRedactionValidated(ctx, redactionEventID, true)
			assert.NoError(t, err)

			wantRedactionInfo.Validated = true
			redactionInfo, err = tab.SelectRedactionInfoByRedactionEventID(ctx, redactionEventID)
			assert.NoError(t, err)
			assert.Equal(t, &wantRedactionInfo, redactionInfo)
		}

		// Should not fail, it just updates 0 rows
		err := tab.MarkRedactionValidated(ctx, "iDontExist", true)
		assert.NoError(t, err)

		// Should also not fail, but return a nil redactionInfo
		redactionInfo, err := tab.SelectRedactionInfoByRedactionEventID(ctx, "iDontExist")
		assert.NoError(t, err)
		assert.Nil(t, redactionInfo)

		redactionInfo, err = tab.SelectRedactionInfoByEventBeingRedacted(ctx, "iDontExist")
		assert.NoError(t, err)
		assert.Nil(t, redactionInfo)
	})
}
