package tables_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

func mustCreateEventTypesTable(ctx context.Context, t *testing.T, _ test.DependancyOption) (tables.EventTypes, func()) {
	t.Helper()

	connStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString:   connStr,
		MaxOpenConnections: 10,
	}, sqlutil.NewExclusiveWriter())
	assert.NoError(t, err)
	var tab tables.EventTypes
	err = postgres.CreateEventTypesTable(ctx, db)
	assert.NoError(t, err)
	tab, err = postgres.PrepareEventTypesTable(ctx, db)

	assert.NoError(t, err)

	return tab, closeDb
}

func Test_EventTypesTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx := testrig.NewContext(t)
		tab, closeDb := mustCreateEventTypesTable(ctx, t, testOpts)
		defer closeDb()
		var eventTypeNID, gotEventTypeNID types.EventTypeNID
		var err error
		// create some dummy data
		eventTypeMap := make(map[string]types.EventTypeNID)
		for i := 0; i < 10; i++ {
			eventType := fmt.Sprintf("dummyEventType%d", i)
			eventTypeNID, err = tab.InsertEventTypeNID(ctx, nil, eventType)
			assert.NoError(t, err)
			eventTypeMap[eventType] = eventTypeNID
			gotEventTypeNID, err = tab.SelectEventTypeNID(ctx, nil, eventType)
			assert.NoError(t, err)
			assert.Equal(t, eventTypeNID, gotEventTypeNID)
		}
		// This should fail, since the dummyEventType0 already exists
		eventType := fmt.Sprintf("dummyEventType%d", 0)
		_, err = tab.InsertEventTypeNID(ctx, nil, eventType)
		assert.Error(t, err)

		// This should return an error, as this eventType does not exist
		_, err = tab.SelectEventTypeNID(ctx, nil, "dummyEventType13")
		assert.Error(t, err)

		eventTypeNIDs, err := tab.BulkSelectEventTypeNID(ctx, nil, []string{"dummyEventType0", "dummyEventType3"})
		assert.NoError(t, err)
		// verify that BulkSelectEventTypeNID and InsertEventTypeNID return the same values
		for eventType, nid := range eventTypeNIDs {
			if v, ok := eventTypeMap[eventType]; ok {
				assert.Equal(t, v, nid)
			} else {
				t.Fatalf("unable to find %d in result set", nid)
			}
		}
	})
}
