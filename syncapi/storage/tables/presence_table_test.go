package tables_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/pitabwire/frame"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/postgres"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/test"
)

func mustPresenceTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) tables.Presence {
	t.Helper()

	cm := sqlutil.NewConnectionManager(svc)
	var tab tables.Presence
	tab, err := postgres.NewPostgresPresenceTable(ctx, cm)

	if err != nil {
		t.Fatalf("failed to make new table: %s", err)
	}
	err = cm.Migrate(ctx)
	if err != nil {
		t.Fatalf("failed to migrate table: %s", err)
	}
	return tab
}

func TestPresence(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)

	statusMsg := "Hello World!"
	timestamp := spec.AsTimestamp(time.Now())

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		tab := mustPresenceTable(ctx, svc, t, testOpts)

		// Insert some presences
		pos, err := tab.UpsertPresence(ctx, alice.ID, &statusMsg, types.PresenceOnline, timestamp, false)
		if err != nil {
			t.Error(err)
		}
		wantPos := types.StreamPosition(1)
		if pos != wantPos {
			t.Errorf("expected pos to be %d, got %d", wantPos, pos)
		}
		pos, err = tab.UpsertPresence(ctx, bob.ID, &statusMsg, types.PresenceOnline, timestamp, false)
		if err != nil {
			t.Error(err)
		}
		wantPos = 2
		if pos != wantPos {
			t.Errorf("expected pos to be %d, got %d", wantPos, pos)
		}

		// verify the expected max presence ID
		maxPos, err := tab.GetMaxPresenceID(ctx)
		if err != nil {
			t.Error(err)
		}
		if maxPos != wantPos {
			t.Errorf("expected max pos to be %d, got %d", wantPos, maxPos)
		}

		// This should increment the position
		pos, err = tab.UpsertPresence(ctx, bob.ID, &statusMsg, types.PresenceOnline, timestamp, true)
		if err != nil {
			t.Error(err)
		}
		wantPos = pos
		if wantPos <= maxPos {
			t.Errorf("expected pos to be %d incremented, got %d", wantPos, pos)
		}

		// This should return only Bobs status
		presences, err := tab.GetPresenceAfter(ctx, maxPos, synctypes.EventFilter{Limit: 10})
		if err != nil {
			t.Error(err)
		}

		if c := len(presences); c > 1 {
			t.Errorf("expected only one presence, got %d", c)
		}

		// Validate the response
		wantPresence := &types.PresenceInternal{
			UserID:       bob.ID,
			Presence:     types.PresenceOnline,
			StreamPos:    wantPos,
			LastActiveTS: timestamp,
			ClientFields: types.PresenceClientResponse{
				LastActiveAgo: 0,
				Presence:      types.PresenceOnline.String(),
				StatusMsg:     &statusMsg,
			},
		}
		if !reflect.DeepEqual(wantPresence, presences[bob.ID]) {
			t.Errorf("unexpected presence result:\n%+v, want\n%+v", presences[bob.ID], wantPresence)
		}

		// Try getting presences for existing and non-existing users
		getUsers := []string{alice.ID, bob.ID, "@doesntexist:test"}
		presencesForUsers, err := tab.GetPresenceForUsers(ctx, getUsers)
		if err != nil {
			t.Error(err)
		}

		if len(presencesForUsers) >= len(getUsers) {
			t.Errorf("expected less presences, but they are the same/more as requested: %d >= %d", len(presencesForUsers), len(getUsers))
		}
	})

}
