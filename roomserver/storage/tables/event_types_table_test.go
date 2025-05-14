package tables_test

import (
	"context"
	"fmt"
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

func mustCreateEventTypesTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) tables.EventTypes {
	t.Helper()

	cm := sqlutil.NewConnectionManager(svc)
	var tab tables.EventTypes
	tab, err := postgres.NewPostgresEventTypesTable(ctx, cm)
	assert.NoError(t, err)

	err = cm.Migrate(ctx)
	if err != nil {
		t.Fatalf("failed to migrate table: %s", err)
	}

	return tab
}

func Test_EventTypesTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		tab := mustCreateEventTypesTable(ctx, svc, t, testOpts)

		var eventTypeNID, gotEventTypeNID types.EventTypeNID
		var err error
		// create some dummy data
		eventTypeMap := make(map[string]types.EventTypeNID)
		for i := 0; i < 10; i++ {
			eventType := fmt.Sprintf("dummyEventType%d", i)
			eventTypeNID, err = tab.InsertEventTypeNID(ctx, eventType)
			assert.NoError(t, err)
			eventTypeMap[eventType] = eventTypeNID
			gotEventTypeNID, err = tab.SelectEventTypeNID(ctx, eventType)
			assert.NoError(t, err)
			assert.Equal(t, eventTypeNID, gotEventTypeNID)
		}
		// This should fail, since the dummyEventType0 already exists
		eventType := fmt.Sprintf("dummyEventType%d", 0)
		_, err = tab.InsertEventTypeNID(ctx, eventType)
		assert.Error(t, err)

		// This should return an error, as this eventType does not exist
		_, err = tab.SelectEventTypeNID(ctx, "dummyEventType13")
		assert.Error(t, err)

		eventTypeNIDs, err := tab.BulkSelectEventTypeNID(ctx, []string{"dummyEventType0", "dummyEventType3"})
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
