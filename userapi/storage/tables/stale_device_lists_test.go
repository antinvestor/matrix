package tables_test

import (
	"context"
	"testing"

	"github.com/pitabwire/frame"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/userapi/storage/postgres"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

func mustCreateTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) tables.StaleDeviceLists {

	cm := sqlutil.NewConnectionManager(svc)

	tab, err := postgres.NewPostgresStaleDeviceListsTable(ctx, cm)

	if err != nil {
		t.Fatal("failed to create new table: %s", err)
	}
	err = cm.Migrate(ctx)
	if err != nil {
		t.Fatal("failed to migrate stale device lists table: %s", err)
	}
	return tab
}

func TestStaleDeviceLists(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	charlie := "@charlie:localhost"

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		tab := mustCreateTable(ctx, svc, t, testOpts)

		if err := tab.InsertStaleDeviceList(ctx, alice.ID, true); err != nil {
			t.Fatal("failed to insert stale device: %s", err)
		}
		if err := tab.InsertStaleDeviceList(ctx, bob.ID, true); err != nil {
			t.Fatal("failed to insert stale device: %s", err)
		}
		if err := tab.InsertStaleDeviceList(ctx, charlie, true); err != nil {
			t.Fatal("failed to insert stale device: %s", err)
		}

		// Query one server
		wantStaleUsers := []string{alice.ID, bob.ID}
		gotStaleUsers, err := tab.SelectUserIDsWithStaleDeviceLists(ctx, []spec.ServerName{"test"})
		if err != nil {
			t.Fatal("failed to query stale device lists: %s", err)
		}
		if !test.UnsortedStringSliceEqual(wantStaleUsers, gotStaleUsers) {
			t.Fatal("expected stale users %v, got %v", wantStaleUsers, gotStaleUsers)
		}

		// Query all servers
		wantStaleUsers = []string{alice.ID, bob.ID, charlie}
		gotStaleUsers, err = tab.SelectUserIDsWithStaleDeviceLists(ctx, []spec.ServerName{})
		if err != nil {
			t.Fatal("failed to query stale device lists: %s", err)
		}
		if !test.UnsortedStringSliceEqual(wantStaleUsers, gotStaleUsers) {
			t.Fatal("expected stale users %v, got %v", wantStaleUsers, gotStaleUsers)
		}

		// Delete stale devices
		deleteUsers := []string{alice.ID, bob.ID}
		if err = tab.DeleteStaleDeviceLists(ctx, deleteUsers); err != nil {
			t.Fatal("failed to delete stale device lists: %s", err)
		}

		// Verify we don't get anything back after deleting
		gotStaleUsers, err = tab.SelectUserIDsWithStaleDeviceLists(ctx, []spec.ServerName{"test"})
		if err != nil {
			t.Fatal("failed to query stale device lists: %s", err)
		}

		if gotCount := len(gotStaleUsers); gotCount > 0 {
			t.Fatal("expected no stale users, got %d", gotCount)
		}
	})
}
