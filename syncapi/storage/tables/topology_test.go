package tables_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/syncapi/storage/postgres"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

func newTopologyTable(t *testing.T, _ test.DependancyOption) (tables.Topology, *sql.DB, func()) {
	t.Helper()

	ctx := testrig.NewContext(t)
	connStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString:   connStr,
		MaxOpenConnections: 10,
	}, sqlutil.NewExclusiveWriter())
	if err != nil {
		t.Fatalf("failed to open db: %s", err)
	}

	var tab tables.Topology
	tab, err = postgres.NewPostgresTopologyTable(ctx, db)

	if err != nil {
		t.Fatalf("failed to make new table: %s", err)
	}
	return tab, db, closeDb
}

func TestTopologyTable(t *testing.T) {
	ctx := testrig.NewContext(t)
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		tab, db, closeDb := newTopologyTable(t, testOpts)
		defer closeDb()
		events := room.Events()
		err := sqlutil.WithTransaction(db, func(txn *sql.Tx) error {
			var highestPos types.StreamPosition
			for i, ev := range events {
				topoPos, err := tab.InsertEventInTopology(ctx, txn, ev, types.StreamPosition(i))
				if err != nil {
					return fmt.Errorf("failed to InsertEventInTopology: %s", err)
				}
				// topo pos = depth, depth starts at 1, hence 1+i
				if topoPos != types.StreamPosition(1+i) {
					return fmt.Errorf("got topo pos %d want %d", topoPos, 1+i)
				}
				highestPos = topoPos + 1
			}
			// check ordering works without limit
			eventIDs, start, end, err := tab.SelectEventIDsInRange(ctx, txn, room.ID, 0, highestPos, highestPos, 100, true)
			assert.NoError(t, err, "failed to SelectEventIDsInRange")
			test.AssertEventIDsEqual(t, eventIDs, events[:])
			assert.Equal(t, types.TopologyToken{Depth: 1, PDUPosition: 0}, start)
			assert.Equal(t, types.TopologyToken{Depth: 5, PDUPosition: 4}, end)

			eventIDs, start, end, err = tab.SelectEventIDsInRange(ctx, txn, room.ID, 0, highestPos, highestPos, 100, false)
			assert.NoError(t, err, "failed to SelectEventIDsInRange")
			test.AssertEventIDsEqual(t, eventIDs, test.Reversed(events[:]))
			assert.Equal(t, types.TopologyToken{Depth: 5, PDUPosition: 4}, start)
			assert.Equal(t, types.TopologyToken{Depth: 1, PDUPosition: 0}, end)

			// check ordering works with limit
			eventIDs, start, end, err = tab.SelectEventIDsInRange(ctx, txn, room.ID, 0, highestPos, highestPos, 3, true)
			assert.NoError(t, err, "failed to SelectEventIDsInRange")
			test.AssertEventIDsEqual(t, eventIDs, events[:3])
			assert.Equal(t, types.TopologyToken{Depth: 1, PDUPosition: 0}, start)
			assert.Equal(t, types.TopologyToken{Depth: 3, PDUPosition: 2}, end)

			eventIDs, start, end, err = tab.SelectEventIDsInRange(ctx, txn, room.ID, 0, highestPos, highestPos, 3, false)
			assert.NoError(t, err, "failed to SelectEventIDsInRange")
			test.AssertEventIDsEqual(t, eventIDs, test.Reversed(events[len(events)-3:]))
			assert.Equal(t, types.TopologyToken{Depth: 5, PDUPosition: 4}, start)
			assert.Equal(t, types.TopologyToken{Depth: 3, PDUPosition: 2}, end)

			// Check that we return no values for invalid rooms
			eventIDs, start, end, err = tab.SelectEventIDsInRange(ctx, txn, "!doesnotexist:localhost", 0, highestPos, highestPos, 10, false)
			assert.NoError(t, err, "failed to SelectEventIDsInRange")
			assert.Equal(t, 0, len(eventIDs))
			assert.Equal(t, types.TopologyToken{}, start)
			assert.Equal(t, types.TopologyToken{}, end)
			return nil
		})
		if err != nil {
			t.Fatalf("err: %s", err)
		}
	})
}
