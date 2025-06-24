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
	"github.com/pitabwire/frame"
)

func newSyncDB(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) storage.Database {
	t.Helper()

	cm := sqlutil.NewConnectionManager(svc)
	syncDB, err := storage.NewSyncServerDatabase(ctx, cm)
	if err != nil {
		t.Fatalf("failed to create sync Cm: %s", err)
	}

	return syncDB
}

func TestFilterTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		tab := newSyncDB(ctx, svc, t, testOpts)

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

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		syncDB := newSyncDB(ctx, svc, t, testOpts)

		tab, err := syncDB.NewDatabaseTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

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
