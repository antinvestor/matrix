package tables_test

import (
	"context"
	"testing"

	"github.com/antinvestor/matrix/userapi/storage/postgres"
	"github.com/matrix-org/gomatrixserverlib/spec"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"

	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

func mustCreateTable(t *testing.T, _ test.DependancyOption) (tab tables.StaleDeviceLists, closeDb func()) {
	ctx := context.TODO()
	connStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: connStr,
	}, nil)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	tab, err = postgres.NewPostgresStaleDeviceListsTable(db)

	if err != nil {
		t.Fatalf("failed to create new table: %s", err)
	}
	return tab, closeDb
}

func TestStaleDeviceLists(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	charlie := "@charlie:localhost"
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		tab, closeDB := mustCreateTable(t, testOpts)
		defer closeDB()

		if err := tab.InsertStaleDeviceList(ctx, alice.ID, true); err != nil {
			t.Fatalf("failed to insert stale device: %s", err)
		}
		if err := tab.InsertStaleDeviceList(ctx, bob.ID, true); err != nil {
			t.Fatalf("failed to insert stale device: %s", err)
		}
		if err := tab.InsertStaleDeviceList(ctx, charlie, true); err != nil {
			t.Fatalf("failed to insert stale device: %s", err)
		}

		// Query one server
		wantStaleUsers := []string{alice.ID, bob.ID}
		gotStaleUsers, err := tab.SelectUserIDsWithStaleDeviceLists(ctx, []spec.ServerName{"test"})
		if err != nil {
			t.Fatalf("failed to query stale device lists: %s", err)
		}
		if !test.UnsortedStringSliceEqual(wantStaleUsers, gotStaleUsers) {
			t.Fatalf("expected stale users %v, got %v", wantStaleUsers, gotStaleUsers)
		}

		// Query all servers
		wantStaleUsers = []string{alice.ID, bob.ID, charlie}
		gotStaleUsers, err = tab.SelectUserIDsWithStaleDeviceLists(ctx, []spec.ServerName{})
		if err != nil {
			t.Fatalf("failed to query stale device lists: %s", err)
		}
		if !test.UnsortedStringSliceEqual(wantStaleUsers, gotStaleUsers) {
			t.Fatalf("expected stale users %v, got %v", wantStaleUsers, gotStaleUsers)
		}

		// Delete stale devices
		deleteUsers := []string{alice.ID, bob.ID}
		if err = tab.DeleteStaleDeviceLists(ctx, nil, deleteUsers); err != nil {
			t.Fatalf("failed to delete stale device lists: %s", err)
		}

		// Verify we don't get anything back after deleting
		gotStaleUsers, err = tab.SelectUserIDsWithStaleDeviceLists(ctx, []spec.ServerName{"test"})
		if err != nil {
			t.Fatalf("failed to query stale device lists: %s", err)
		}

		if gotCount := len(gotStaleUsers); gotCount > 0 {
			t.Fatalf("expected no stale users, got %d", gotCount)
		}
	})
}
