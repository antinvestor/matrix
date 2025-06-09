package tables_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/pitabwire/frame"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/postgres"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/test"
)

func newOutputRoomEventsTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) (sqlutil.ConnectionManager, tables.Events) {
	t.Helper()

	cm := sqlutil.NewConnectionManager(svc)

	var tab tables.Events
	tab, err := postgres.NewPostgresEventsTable(ctx, cm)
	if err != nil {
		t.Fatal("failed to make new table: %s", err)
	}

	err = cm.Migrate(ctx)
	if err != nil {
		t.Fatal("failed to migrate events table: %s", err)
	}
	return cm, tab
}

func TestOutputRoomEventsTable(t *testing.T) {

	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		cm, tab := newOutputRoomEventsTable(ctx, svc, t, testOpts)

		events := room.Events()
		err := cm.Do(ctx, func(ctx context.Context) error {
			for _, ev := range events {
				_, err := tab.InsertEvent(ctx, ev, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
				if err != nil {
					return fmt.Errorf("failed to InsertEvent: %s", err)
				}
			}
			// order = 2,0,3,1
			wantEventIDs := []string{
				events[2].EventID(), events[0].EventID(), events[3].EventID(), events[1].EventID(),
			}
			gotEvents, err := tab.SelectEvents(ctx, wantEventIDs, nil, true)
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
			if _, err = tab.InsertEvent(ctx, urlEv, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared); err != nil {
				return fmt.Errorf("failed to InsertEvent: %s", err)
			}
			wantEventID := []string{urlEv.EventID()}
			t := true
			gotEvents, err = tab.SelectEvents(ctx, wantEventID, &synctypes.RoomEventFilter{Limit: 1, ContainsURL: &t}, true)
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
			t.Fatal("err: %s", err)
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

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		cm, tab := newOutputRoomEventsTable(ctx, svc, t, testOpts)

		err := cm.Do(ctx, func(ctx context.Context) error {
			for _, ev := range room.Events() {
				_, err := tab.InsertEvent(ctx, ev, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
				if err != nil {
					return fmt.Errorf("failed to InsertEvent: %s", err)
				}
			}

			return nil
		})
		if err != nil {
			t.Fatal("err: %s", err)
		}

		events, err := tab.ReIndex(ctx, 10, 0, []string{
			spec.MRoomName,
			spec.MRoomTopic,
			"m.room.message"})
		if err != nil {
			t.Fatal(err)
		}

		wantEventCount := 3
		if len(events) != wantEventCount {
			t.Fatal("expected %d events, got %d", wantEventCount, len(events))
		}
	})
}
