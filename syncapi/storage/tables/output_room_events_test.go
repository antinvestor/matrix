package tables_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/postgres"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/test"
)

func newOutputRoomEventsTable(ctx context.Context, t *testing.T, dep test.DependancyOption) (tables.Events, *sql.DB, func()) {
	t.Helper()

	db, closeDb := migrateDatabase(ctx, t, dep)

	tab, err := postgres.NewPostgresEventsTable(ctx, db)

	if err != nil {
		t.Fatalf("failed to make new table: %s", err)
	}
	return tab, db, closeDb
}

func TestOutputRoomEventsTable(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx := testrig.NewContext(t)

		tab, db, closeDb := newOutputRoomEventsTable(ctx, t, testOpts)
		defer closeDb()
		events := room.Events()
		err := sqlutil.WithTransaction(db, func(txn *sql.Tx) error {
			for _, ev := range events {
				_, err := tab.InsertEvent(ctx, txn, ev, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
				if err != nil {
					return fmt.Errorf("failed to InsertEvent: %s", err)
				}
			}
			// order = 2,0,3,1
			wantEventIDs := []string{
				events[2].EventID(), events[0].EventID(), events[3].EventID(), events[1].EventID(),
			}
			gotEvents, err := tab.SelectEvents(ctx, txn, wantEventIDs, nil, true)
			if err != nil {
				return fmt.Errorf("failed to SelectEvents: %s", err)
			}
			gotEventIDs := make([]string, len(gotEvents))
			for i := range gotEvents {
				gotEventIDs[i] = gotEvents[i].EventID()
			}
			if !reflect.DeepEqual(gotEventIDs, wantEventIDs) {
				return fmt.Errorf("SelectEvents\ngot  %v\n want %v", gotEventIDs, wantEventIDs)
			}

			// Test that contains_url is correctly populated
			urlEv := room.CreateEvent(t, alice, "m.text", map[string]interface{}{
				"body": "test.txt",
				"url":  "mxc://test.txt",
			})
			if _, err = tab.InsertEvent(ctx, txn, urlEv, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared); err != nil {
				return fmt.Errorf("failed to InsertEvent: %s", err)
			}
			wantEventID := []string{urlEv.EventID()}
			t := true
			gotEvents, err = tab.SelectEvents(ctx, txn, wantEventID, &synctypes.RoomEventFilter{Limit: 1, ContainsURL: &t}, true)
			if err != nil {
				return fmt.Errorf("failed to SelectEvents: %s", err)
			}
			gotEventIDs = make([]string, len(gotEvents))
			for i := range gotEvents {
				gotEventIDs[i] = gotEvents[i].EventID()
			}
			if !reflect.DeepEqual(gotEventIDs, wantEventID) {
				return fmt.Errorf("SelectEvents\ngot  %v\n want %v", gotEventIDs, wantEventID)
			}

			return nil
		})
		if err != nil {
			t.Fatalf("err: %s", err)
		}
	})
}

func TestReindex(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)

	room.CreateAndInsert(t, alice, spec.MRoomName, map[string]interface{}{
		"name": "my new room name",
	}, test.WithStateKey(""))

	room.CreateAndInsert(t, alice, spec.MRoomTopic, map[string]interface{}{
		"topic": "my new room topic",
	}, test.WithStateKey(""))

	room.CreateAndInsert(t, alice, "m.room.message", map[string]interface{}{
		"msgbody": "my room message",
		"type":    "m.text",
	})

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx := testrig.NewContext(t)

		tab, db, closeDb := newOutputRoomEventsTable(ctx, t, testOpts)
		defer closeDb()
		err := sqlutil.WithTransaction(db, func(txn *sql.Tx) error {
			for _, ev := range room.Events() {
				_, err := tab.InsertEvent(ctx, txn, ev, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
				if err != nil {
					return fmt.Errorf("failed to InsertEvent: %s", err)
				}
			}

			return nil
		})
		if err != nil {
			t.Fatalf("err: %s", err)
		}

		events, err := tab.ReIndex(ctx, nil, 10, 0, []string{
			spec.MRoomName,
			spec.MRoomTopic,
			"m.room.message"})
		if err != nil {
			t.Fatal(err)
		}

		wantEventCount := 3
		if len(events) != wantEventCount {
			t.Fatalf("expected %d events, got %d", wantEventCount, len(events))
		}
	})
}
