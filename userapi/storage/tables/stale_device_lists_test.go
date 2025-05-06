package tables_test

import (
	"context"
	"github.com/pitabwire/frame"
	"testing"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/antinvestor/matrix/userapi/storage"
	"github.com/antinvestor/matrix/userapi/storage/postgres"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

func migrateDatabase(ctx context.Context, svc *frame.Service, t *testing.T) *sqlutil.Connections {

	cm := sqlutil.NewConnectionManager(svc)

	_, err := storage.NewUserDatabase(ctx, nil, cm, spec.ServerName("test"), 4, 0, 0, "")
	if err != nil {
		t.Fatalf("failed to create user api DB: %s", err)
	}

	_, err = storage.NewKeyDatabase(ctx, cm)
	if err != nil {
		t.Fatalf("failed to create key api DB: %s", err)
	}

	return cm
}

func mustCreateTable(ctx context.Context, svc *frame.Service, t *testing.T, dep test.DependancyOption) tables.StaleDeviceLists {
	cm := migrateDatabase(ctx, svc, t)
	tab := postgres.NewPostgresStaleDeviceListsTable(cm)
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
		if err = tab.DeleteStaleDeviceLists(ctx, deleteUsers); err != nil {
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
