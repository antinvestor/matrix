package tables_test

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/syncapi/storage/postgres"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/test"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func mustPresenceTable(t *testing.T, _ test.DependancyOption) (tables.Presence, func()) {
	t.Helper()
	ctx := context.TODO()
	connStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, sqlutil.NewExclusiveWriter())
	if err != nil {
		t.Fatalf("failed to open db: %s", err)
	}

	var tab tables.Presence
	tab, err = postgres.NewPostgresPresenceTable(db)

	if err != nil {
		t.Fatalf("failed to make new table: %s", err)
	}
	return tab, closeDb
}

func TestPresence(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	ctx := context.Background()

	statusMsg := "Hello World!"
	timestamp := spec.AsTimestamp(time.Now())

	var txn *sql.Tx
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		tab, closeDB := mustPresenceTable(t, testOpts)
		defer closeDB()

		// Insert some presences
		pos, err := tab.UpsertPresence(ctx, txn, alice.ID, &statusMsg, types.PresenceOnline, timestamp, false)
		if err != nil {
			t.Error(err)
		}
		wantPos := types.StreamPosition(1)
		if pos != wantPos {
			t.Errorf("expected pos to be %d, got %d", wantPos, pos)
		}
		pos, err = tab.UpsertPresence(ctx, txn, bob.ID, &statusMsg, types.PresenceOnline, timestamp, false)
		if err != nil {
			t.Error(err)
		}
		wantPos = 2
		if pos != wantPos {
			t.Errorf("expected pos to be %d, got %d", wantPos, pos)
		}

		// verify the expected max presence ID
		maxPos, err := tab.GetMaxPresenceID(ctx, txn)
		if err != nil {
			t.Error(err)
		}
		if maxPos != wantPos {
			t.Errorf("expected max pos to be %d, got %d", wantPos, maxPos)
		}

		// This should increment the position
		pos, err = tab.UpsertPresence(ctx, txn, bob.ID, &statusMsg, types.PresenceOnline, timestamp, true)
		if err != nil {
			t.Error(err)
		}
		wantPos = pos
		if wantPos <= maxPos {
			t.Errorf("expected pos to be %d incremented, got %d", wantPos, pos)
		}

		// This should return only Bobs status
		presences, err := tab.GetPresenceAfter(ctx, txn, maxPos, synctypes.EventFilter{Limit: 10})
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
		presencesForUsers, err := tab.GetPresenceForUsers(ctx, nil, getUsers)
		if err != nil {
			t.Error(err)
		}

		if len(presencesForUsers) >= len(getUsers) {
			t.Errorf("expected less presences, but they are the same/more as requested: %d >= %d", len(presencesForUsers), len(getUsers))
		}
	})

}
