package tables_test

import (
	"context"
	"database/sql"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/antinvestor/matrix/userapi/storage"
	"github.com/antinvestor/matrix/userapi/storage/postgres"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

func migrateDatabase(ctx context.Context, t *testing.T, testOpts test.DependancyOption) (*sql.DB, func()) {

	cfg, closeDB := testrig.CreateConfig(ctx, t, testOpts)
	cm := sqlutil.NewConnectionManager(ctx, cfg.Global.DatabaseOptions)
	_, err := storage.NewUserDatabase(ctx, nil, cm, &cfg.UserAPI.AccountDatabase, spec.ServerName("test"), 4, 0, 0, "")
	if err != nil {
		t.Fatalf("failed to create user api DB: %s", err)
	}

	_, err = storage.NewKeyDatabase(ctx, cm, &cfg.UserAPI.AccountDatabase)
	if err != nil {
		t.Fatalf("failed to create key api DB: %s", err)
	}

	db, err := sqlutil.Open(&cfg.UserAPI.AccountDatabase, sqlutil.NewExclusiveWriter())
	assert.NoError(t, err)

	return db, closeDB
}

func mustCreateTable(t *testing.T, dep test.DependancyOption) (tab tables.StaleDeviceLists, closeDb func()) {
	ctx := testrig.NewContext(t)

	db, closeDb := migrateDatabase(ctx, t, dep)

	tab, err := postgres.NewPostgresStaleDeviceListsTable(ctx, db)

	if err != nil {
		t.Fatalf("failed to create new table: %s", err)
	}
	return tab, closeDb
}

func TestStaleDeviceLists(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	charlie := "@charlie:localhost"
	ctx := testrig.NewContext(t)

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
