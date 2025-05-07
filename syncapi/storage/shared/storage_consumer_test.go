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

func newSyncDB(ctx context.Context, t *testing.T, testOpts test.DependancyOption) (storage.Database, func()) {
	t.Helper()

	cfg, closeDB := testrig.CreateConfig(ctx, t, testOpts)
	cm := sqlutil.NewConnectionManager(ctx, cfg.Global.DatabaseOptions)
	syncDB, err := storage.NewSyncServerDatasource(ctx, cm, &cfg.SyncAPI.Database)
	if err != nil {
		t.Fatalf("failed to create sync Cm: %s", err)
	}

	return syncDB, closeDB
}

func TestFilterTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		tab, closeDB := newSyncDB(ctx, t, testOpts)
		defer closeDB()

		// initially create a filter
		filter := &synctypes.Filter{}
		filterID, err := tab.PutFilter(ctx, "alice", filter)
		if err != nil {
			t.Fatal(err)
		}

		// create the same filter again, we should receive the existing filter
		secondFilterID, err := tab.PutFilter(ctx, "alice", filter)
		if err != nil {
			t.Fatal(err)
		}

		if secondFilterID != filterID {
			t.Fatalf("expected second filter to be the same as the first: %s vs %s", filterID, secondFilterID)
		}

		// query the filter again
		targetFilter := &synctypes.Filter{}
		if err = tab.GetFilter(ctx, targetFilter, "alice", filterID); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(filter, targetFilter) {
			t.Fatalf("%#v vs %#v", filter, targetFilter)
		}

		// query non-existent filter
		if err = tab.GetFilter(ctx, targetFilter, "bob", filterID); err == nil {
			t.Fatalf("expected filter to not exist, but it does exist: %v", targetFilter)
		}
	})
}

func TestIgnores(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		syncDB, closeDB := newSyncDB(ctx, t, testOpts)
		defer closeDB()

		tab, err := syncDB.NewDatabaseTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tab.Rollback() // nolint: errcheck

		ignoredUsers := &types.IgnoredUsers{List: map[string]interface{}{
			bob.ID: "",
		}}
		if err = tab.UpdateIgnoresForUser(ctx, alice.ID, ignoredUsers); err != nil {
			t.Fatal(err)
		}

		gotIgnoredUsers, err := tab.IgnoresForUser(ctx, alice.ID)
		if err != nil {
			t.Fatal(err)
		}

		// verify the ignored users matches those we stored
		if !reflect.DeepEqual(gotIgnoredUsers, ignoredUsers) {
			t.Fatalf("%#v vs %#v", gotIgnoredUsers, ignoredUsers)
		}

		// Bob doesn't have any ignored users, so should receive sql.ErrNoRows
		if _, err = tab.IgnoresForUser(ctx, bob.ID); err == nil {
			t.Fatalf("expected an error but got none")
		}
	})
}
