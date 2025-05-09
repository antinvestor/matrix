package sqlutil_test

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
)

func TestConnectionManager(t *testing.T) {

	t.Run("component defined connection string", func(t *testing.T) {
		test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

			ctx, svc, _ := testrig.Init(t, testOpts)
			defer svc.Stop(ctx)

			cm := sqlutil.NewConnectionManager(svc)
			db := cm.Connection(ctx, false)

			// reuse existing connection
			db2 := cm.Connection(ctx, false)

			sqlDb, err0 := db.DB()
			assert.NoError(t, err0)

			sqlDb2, err0 := db2.DB()
			assert.NoError(t, err0)

			if !reflect.DeepEqual(sqlDb, sqlDb2) {
				t.Fatalf("expected database connection to be reused")
			}

		})
	})

	t.Run("global connection pool", func(t *testing.T) {
		test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
			ctx, svc, cfg := testrig.Init(t, testOpts)
			defer svc.Stop(ctx)

			cm, err := sqlutil.NewConnectionManagerWithOptions(ctx, svc, &cfg.Global.DatabaseOptions)
			if err != nil {
				t.Fatal(err)
			}

			db := cm.Connection(ctx, true)
			if err != nil {
				t.Fatal(err)
			}

			// reuse existing connection
			db2 := cm.Connection(ctx, true)

			sqlDb1, err := db.DB()
			assert.NoError(t, err)

			sqlDb2, err := db2.DB()
			assert.NoError(t, err)
			if err != nil {
				t.Fatal(err)
			}

			//We check the underlaying database connection as gorm mutates quickly
			if !reflect.DeepEqual(sqlDb1, sqlDb2) {
				t.Fatalf("expected database connection to be reused")
			}
		})
	})

	t.Run("shutdown", func(t *testing.T) {
		test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

			ctx, svc, cfg := testrig.Init(t, testOpts)
			defer svc.Stop(ctx)

			var err error
			cm, err := sqlutil.NewConnectionManagerWithOptions(ctx, svc, &config.DatabaseOptions{ConnectionString: cfg.Global.DatabaseOptions.ConnectionString})
			if err != nil {
				t.Fatal(err)
			}

			_ = cm.Connection(ctx, false)
			if err != nil {
				t.Fatal(err)
			}

		})
	})

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	// test invalid connection string configured
	_, err := sqlutil.NewConnectionManagerWithOptions(ctx, svc, &config.DatabaseOptions{ConnectionString: "http://"})
	if err == nil {
		t.Fatal("expected an error but got none")
	}

	// empty connection string is not allowed
	_, err = sqlutil.NewConnectionManagerWithOptions(ctx, svc, &config.DatabaseOptions{})
	if err == nil {
		t.Fatal("expected an error but got none")
	}
}
