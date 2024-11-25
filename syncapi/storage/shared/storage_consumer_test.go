package shared_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
)

func newSyncDB(t *testing.T, testOpts test.DependancyOption) (storage.Database, func()) {
	t.Helper()

	cfg, processCtx, closeDB := testrig.CreateConfig(t, testOpts)
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	syncDB, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
	if err != nil {
		t.Fatalf("failed to create sync DB: %s", err)
	}

	return syncDB, closeDB
}

func TestFilterTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		tab, closeDB := newSyncDB(t, testOpts)
		defer closeDB()

		// initially create a filter
		filter := &synctypes.Filter{}
		filterID, err := tab.PutFilter(context.Background(), "alice", filter)
		if err != nil {
			t.Fatal(err)
		}

		// create the same filter again, we should receive the existing filter
		secondFilterID, err := tab.PutFilter(context.Background(), "alice", filter)
		if err != nil {
			t.Fatal(err)
		}

		if secondFilterID != filterID {
			t.Fatalf("expected second filter to be the same as the first: %s vs %s", filterID, secondFilterID)
		}

		// query the filter again
		targetFilter := &synctypes.Filter{}
		if err = tab.GetFilter(context.Background(), targetFilter, "alice", filterID); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(filter, targetFilter) {
			t.Fatalf("%#v vs %#v", filter, targetFilter)
		}

		// query non-existent filter
		if err = tab.GetFilter(context.Background(), targetFilter, "bob", filterID); err == nil {
			t.Fatalf("expected filter to not exist, but it does exist: %v", targetFilter)
		}
	})
}

func TestIgnores(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		syncDB, closeDB := newSyncDB(t, testOpts)
		defer closeDB()

		tab, err := syncDB.NewDatabaseTransaction(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer tab.Rollback() // nolint: errcheck

		ignoredUsers := &types.IgnoredUsers{List: map[string]interface{}{
			bob.ID: "",
		}}
		if err = tab.UpdateIgnoresForUser(context.Background(), alice.ID, ignoredUsers); err != nil {
			t.Fatal(err)
		}

		gotIgnoredUsers, err := tab.IgnoresForUser(context.Background(), alice.ID)
		if err != nil {
			t.Fatal(err)
		}

		// verify the ignored users matches those we stored
		if !reflect.DeepEqual(gotIgnoredUsers, ignoredUsers) {
			t.Fatalf("%#v vs %#v", gotIgnoredUsers, ignoredUsers)
		}

		// Bob doesn't have any ignored users, so should receive sql.ErrNoRows
		if _, err = tab.IgnoresForUser(context.Background(), bob.ID); err == nil {
			t.Fatalf("expected an error but got none")
		}
	})
}
