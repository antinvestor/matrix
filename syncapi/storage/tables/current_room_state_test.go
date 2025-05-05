package tables_test

import (
	"context"
	"fmt"
	"github.com/pitabwire/frame"
	"testing"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/storage/postgres"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
)

func migrateDatabase(ctx context.Context, svc *frame.Service, t *testing.T, testOpts test.DependancyOption) *sqlutil.Connections {

	cm := sqlutil.NewConnectionManager(svc)
	_, err := storage.NewSyncServerDatasource(ctx, cm)
	if err != nil {
		t.Fatalf("failed to create sync DB: %s", err)
	}

	return cm
}

func newCurrentRoomStateTable(ctx context.Context, svc *frame.Service, t *testing.T, dep test.DependancyOption) tables.CurrentRoomState {
	t.Helper()

	cm := migrateDatabase(ctx, svc, t, dep)

	tab := postgres.NewPostgresCurrentRoomStateTable(cm)
	return tab
}

func TestCurrentRoomStateTable(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		tab := newCurrentRoomStateTable(ctx, svc, t, testOpts)
		events := room.CurrentState()
		err := func() error {
			for i, ev := range events {
				ev.StateKeyResolved = ev.StateKey()
				userID, err := spec.NewUserID(string(ev.SenderID()), true)
				if err != nil {
					return err
				}
				ev.UserID = *userID
				err = tab.UpsertRoomState(ctx, ev, nil, types.StreamPosition(i))
				if err != nil {
					return fmt.Errorf("failed to UpsertRoomState: %w", err)
				}
			}
			wantEventIDs := []string{
				events[0].EventID(), events[1].EventID(), events[2].EventID(), events[3].EventID(),
			}
			gotEvents, err := tab.SelectEventsWithEventIDs(ctx, wantEventIDs)
			if err != nil {
				return fmt.Errorf("failed to SelectEventsWithEventIDs: %w", err)
			}
			if len(gotEvents) != len(wantEventIDs) {
				return fmt.Errorf("SelectEventsWithEventIDs\ngot %d, want %d results", len(gotEvents), len(wantEventIDs))
			}
			gotEventIDs := make(map[string]struct{}, len(gotEvents))
			for _, event := range gotEvents {
				if event.ExcludeFromSync {
					return fmt.Errorf("SelectEventsWithEventIDs ExcludeFromSync should be false for current room state event %+v", event)
				}
				gotEventIDs[event.EventID()] = struct{}{}
			}
			for _, id := range wantEventIDs {
				if _, ok := gotEventIDs[id]; !ok {
					return fmt.Errorf("SelectEventsWithEventIDs\nexpected id %q not returned", id)
				}
			}

			testCurrentState(t, ctx, tab, room)

			return nil
		}()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
	})
}

func testCurrentState(t *testing.T, ctx context.Context, tab tables.CurrentRoomState, room *test.Room) {
	t.Run("test currentState", func(t *testing.T) {
		// returns the complete state of the room with a default filter
		filter := synctypes.DefaultStateFilter()
		evs, err := tab.SelectCurrentState(ctx, room.ID, &filter, nil)
		if err != nil {
			t.Fatal(err)
		}
		expectCount := 5
		if gotCount := len(evs); gotCount != expectCount {
			t.Fatalf("expected %d state events, got %d", expectCount, gotCount)
		}
		// When lazy loading, we expect no membership event, so only 4 events
		filter.LazyLoadMembers = true
		expectCount = 4
		evs, err = tab.SelectCurrentState(ctx, room.ID, &filter, nil)
		if err != nil {
			t.Fatal(err)
		}
		if gotCount := len(evs); gotCount != expectCount {
			t.Fatalf("expected %d state events, got %d", expectCount, gotCount)
		}
		// same as above, but with existing NotTypes defined
		notTypes := []string{spec.MRoomMember}
		filter.NotTypes = &notTypes
		evs, err = tab.SelectCurrentState(ctx, room.ID, &filter, nil)
		if err != nil {
			t.Fatal(err)
		}
		if gotCount := len(evs); gotCount != expectCount {
			t.Fatalf("expected %d state events, got %d", expectCount, gotCount)
		}
	})

}
