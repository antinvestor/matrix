package sqlutil_test

import (
	"reflect"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/process"
	"github.com/antinvestor/matrix/test"
)

func TestConnectionManager(t *testing.T) {

	t.Run("component defined connection string", func(t *testing.T) {
		test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

			processCtx := process.NewProcessContext()
			ctx := processCtx.Context()

			conStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
			if err != nil {
				t.Fatalf("failed to open database: %s", err)
			}
			t.Cleanup(closeDb)
			cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{ConnectionString: conStr})

			dbProps := &config.DatabaseOptions{ConnectionString: conStr}
			db, writer, err := cm.Connection(dbProps)
			if err != nil {
				t.Fatal(err)
			}

			_, ok := writer.(*sqlutil.DummyWriter)
			if !ok {
				t.Fatalf("expected dummy writer")
			}

			// reuse existing connection
			db2, writer2, err := cm.Connection(dbProps)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(db, db2) {
				t.Fatalf("expected database connection to be reused")
			}
			if !reflect.DeepEqual(writer, writer2) {
				t.Fatalf("expected database writer to be reused")
			}
		})
	})

	t.Run("global connection pool", func(t *testing.T) {
		test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

			processCtx := process.NewProcessContext()
			ctx := processCtx.Context()

			conStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
			if err != nil {
				t.Fatalf("failed to open database: %s", err)
			}
			t.Cleanup(closeDb)
			cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{ConnectionString: conStr})

			dbProps := &config.DatabaseOptions{ConnectionString: conStr}
			db, writer, err := cm.Connection(dbProps)
			if err != nil {
				t.Fatal(err)
			}

			switch testOpts {
			case test.DependancyOption{}:
				_, ok := writer.(*sqlutil.DummyWriter)
				if !ok {
					t.Fatalf("expected dummy writer")
				}
			}

			// reuse existing connection
			db2, writer2, err := cm.Connection(dbProps)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(db, db2) {
				t.Fatalf("expected database connection to be reused")
			}
			if !reflect.DeepEqual(writer, writer2) {
				t.Fatalf("expected database writer to be reused")
			}
		})
	})

	t.Run("shutdown", func(t *testing.T) {
		test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

			cfg, ctx, closeRig := testrig.CreateConfig(t, test.DependancyOption{})
			defer closeRig()

			cm := sqlutil.NewConnectionManager(ctx,
				config.DatabaseOptions{ConnectionString: cfg.Global.DatabaseOptions.ConnectionString})

			_, _, err := cm.Connection(&cfg.Global.DatabaseOptions)
			if err != nil {
				t.Fatal(err)
			}

		})
	})

	// test invalid connection string configured
	cm2 := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
	_, _, err := cm2.Connection(&config.DatabaseOptions{ConnectionString: "http://"})
	if err == nil {
		t.Fatal("expected an error but got none")
	}

	// empty connection string is not allowed
	_, _, err = cm2.Connection(&config.DatabaseOptions{})
	if err == nil {
		t.Fatal("expected an error but got none")
	}
}
