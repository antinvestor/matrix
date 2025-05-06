package tables_test

import (
	"context"
	"database/sql"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/postgres"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/test"
)

const (
	server1 = "server1"
	server2 = "server2"
	server3 = "server3"
	server4 = "server4"
)

type RelayServersDatabase struct {
	DB     *sql.DB
	Writer sqlutil.Writer
	Table  tables.FederationRelayServers
}

func mustCreateRelayServersTable(
	ctx context.Context, svc *frame.Service, cfg *config.Matrix, t *testing.T,
) RelayServersDatabase {
	t.Helper()

	cm := migrateDatabase(ctx, svc, cfg, t)
	tab := postgres.NewPostgresRelayServersTable(cm)

	return RelayServersDatabase{
		Table: tab,
	}
}

func Equal(a, b []spec.ServerName) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func TestShouldInsertRelayServers(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateRelayServersTable(ctx, svc, cfg, t)

		expectedRelayServers := []spec.ServerName{server2, server3}

		err := db.Table.InsertRelayServers(ctx, server1, expectedRelayServers)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}

		relayServers, err := db.Table.SelectRelayServers(ctx, server1)
		if err != nil {
			t.Fatalf("Failed retrieving relay servers for %s: %s", relayServers, err.Error())
		}

		if !Equal(relayServers, expectedRelayServers) {
			t.Fatalf("Expected: %v \nActual: %v", expectedRelayServers, relayServers)
		}
	})
}

func TestShouldInsertRelayServersWithDuplicates(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateRelayServersTable(ctx, svc, cfg, t)
		insertRelayServers := []spec.ServerName{server2, server2, server2, server3, server2}
		expectedRelayServers := []spec.ServerName{server2, server3}

		err := db.Table.InsertRelayServers(ctx, server1, insertRelayServers)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}

		// Insert the same list again, this shouldn't fail and should have no effect.
		err = db.Table.InsertRelayServers(ctx, server1, insertRelayServers)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}

		relayServers, err := db.Table.SelectRelayServers(ctx, server1)
		if err != nil {
			t.Fatalf("Failed retrieving relay servers for %s: %s", relayServers, err.Error())
		}

		if !Equal(relayServers, expectedRelayServers) {
			t.Fatalf("Expected: %v \nActual: %v", expectedRelayServers, relayServers)
		}
	})
}

func TestShouldGetRelayServersUnknownDestination(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateRelayServersTable(ctx, svc, cfg, t)

		// Query relay servers for a destination that doesn't exist in the table.
		relayServers, err := db.Table.SelectRelayServers(ctx, server1)
		if err != nil {
			t.Fatalf("Failed retrieving relay servers for %s: %s", relayServers, err.Error())
		}

		if !Equal(relayServers, []spec.ServerName{}) {
			t.Fatalf("Expected: %v \nActual: %v", []spec.ServerName{}, relayServers)
		}
	})
}

func TestShouldDeleteCorrectRelayServers(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateRelayServersTable(ctx, svc, cfg, t)
		relayServers1 := []spec.ServerName{server2, server3}
		relayServers2 := []spec.ServerName{server1, server3, server4}

		err := db.Table.InsertRelayServers(ctx, server1, relayServers1)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}
		err = db.Table.InsertRelayServers(ctx, server2, relayServers2)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}

		err = db.Table.DeleteRelayServers(ctx, server1, []spec.ServerName{server2})
		if err != nil {
			t.Fatalf("Failed deleting relay servers for %s: %s", server1, err.Error())
		}
		err = db.Table.DeleteRelayServers(ctx, server2, []spec.ServerName{server1, server4})
		if err != nil {
			t.Fatalf("Failed deleting relay servers for %s: %s", server2, err.Error())
		}

		expectedRelayServers := []spec.ServerName{server3}
		relayServers, err := db.Table.SelectRelayServers(ctx, server1)
		if err != nil {
			t.Fatalf("Failed retrieving relay servers for %s: %s", relayServers, err.Error())
		}
		if !Equal(relayServers, expectedRelayServers) {
			t.Fatalf("Expected: %v \nActual: %v", expectedRelayServers, relayServers)
		}
		relayServers, err = db.Table.SelectRelayServers(ctx, server2)
		if err != nil {
			t.Fatalf("Failed retrieving relay servers for %s: %s", relayServers, err.Error())
		}
		if !Equal(relayServers, expectedRelayServers) {
			t.Fatalf("Expected: %v \nActual: %v", expectedRelayServers, relayServers)
		}
	})
}

func TestShouldDeleteAllRelayServers(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateRelayServersTable(ctx, svc, cfg, t)
		expectedRelayServers := []spec.ServerName{server2, server3}

		err := db.Table.InsertRelayServers(ctx, server1, expectedRelayServers)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}
		err = db.Table.InsertRelayServers(ctx, server2, expectedRelayServers)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}

		err = db.Table.DeleteAllRelayServers(ctx, server1)
		if err != nil {
			t.Fatalf("Failed deleting relay servers for %s: %s", server1, err.Error())
		}

		expectedRelayServers1 := []spec.ServerName{}
		relayServers, err := db.Table.SelectRelayServers(ctx, server1)
		if err != nil {
			t.Fatalf("Failed retrieving relay servers for %s: %s", relayServers, err.Error())
		}
		if !Equal(relayServers, expectedRelayServers1) {
			t.Fatalf("Expected: %v \nActual: %v", expectedRelayServers1, relayServers)
		}
		relayServers, err = db.Table.SelectRelayServers(ctx, server2)
		if err != nil {
			t.Fatalf("Failed retrieving relay servers for %s: %s", relayServers, err.Error())
		}
		if !Equal(relayServers, expectedRelayServers) {
			t.Fatalf("Expected: %v \nActual: %v", expectedRelayServers, relayServers)
		}
	})
}
