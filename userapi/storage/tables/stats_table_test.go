package tables_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/pitabwire/util"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/postgres"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/antinvestor/matrix/userapi/types"
)

func mustMakeDBs(ctx context.Context, t *testing.T, _ test.DependancyOption) (
	*sql.DB, tables.AccountsTable, tables.DevicesTable, tables.StatsTable, func(),
) {
	t.Helper()

	var (
		accTable   tables.AccountsTable
		devTable   tables.DevicesTable
		statsTable tables.StatsTable
		err        error
	)

	connStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString:   connStr,
		MaxOpenConnections: 10,
	}, nil)
	if err != nil {
		t.Fatalf("failed to open db: %s", err)
	}

	accTable, err = postgres.NewPostgresAccountsTable(ctx, db, "localhost")
	if err != nil {
		t.Fatalf("unable to create acc db: %v", err)
	}
	devTable, err = postgres.NewPostgresDevicesTable(ctx, db, "localhost")
	if err != nil {
		t.Fatalf("unable to open device db: %v", err)
	}
	statsTable, err = postgres.NewPostgresStatsTable(ctx, db, "localhost")
	if err != nil {
		t.Fatalf("unable to open stats db: %v", err)
	}

	return db, accTable, devTable, statsTable, closeDb
}

func mustMakeAccountAndDevice(
	t *testing.T,
	ctx context.Context,
	accDB tables.AccountsTable,
	devDB tables.DevicesTable,
	localpart string,
	serverName spec.ServerName, // nolint:unparam
	accType api.AccountType,
	userAgent string,
) {
	t.Helper()

	appServiceID := ""
	if accType == api.AccountTypeAppService {
		appServiceID = util.RandomString(16)
	}

	_, err := accDB.InsertAccount(ctx, nil, localpart, serverName, "", appServiceID, accType)
	if err != nil {
		t.Fatalf("unable to create account: %v", err)
	}
	_, err = devDB.InsertDevice(ctx, nil, "deviceID", localpart, serverName, util.RandomString(16), nil, nil, "", userAgent)
	if err != nil {
		t.Fatalf("unable to create device: %v", err)
	}
}

func mustUpdateDeviceLastSeen(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	localpart string,
	timestamp time.Time,
) {
	t.Helper()
	_, err := db.ExecContext(ctx, "UPDATE userapi_devices SET last_seen_ts = $1 WHERE localpart = $2", spec.AsTimestamp(timestamp), localpart)
	if err != nil {
		t.Fatalf("unable to update device last seen")
	}
}

func mustUserUpdateRegistered(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	localpart string,
	timestamp time.Time,
) {
	_, err := db.ExecContext(ctx, "UPDATE userapi_accounts SET created_ts = $1 WHERE localpart = $2", spec.AsTimestamp(timestamp), localpart)
	if err != nil {
		t.Fatalf("unable to update device last seen")
	}
}

// These tests must run sequentially, as they build up on each other
func Test_UserStatistics(t *testing.T) {

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db, accDB, devDB, statsDB, closeDb := mustMakeDBs(ctx, t, testOpts)
		defer closeDb()
		wantType := "Postgres"

		t.Run(fmt.Sprintf("want %s database engine", wantType), func(t *testing.T) {
			_, gotDB, err := statsDB.UserStatistics(ctx, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if wantType != gotDB.Engine { // can't use DeepEqual, as the Version might differ
				t.Errorf("UserStatistics() got DB engine = %+v, want %s", gotDB.Engine, wantType)
			}
		})

		t.Run("Want Users", func(t *testing.T) {
			mustMakeAccountAndDevice(t, ctx, accDB, devDB, "user1", "localhost", api.AccountTypeUser, "Element Android")
			mustMakeAccountAndDevice(t, ctx, accDB, devDB, "user2", "localhost", api.AccountTypeUser, "Element iOS")
			mustMakeAccountAndDevice(t, ctx, accDB, devDB, "user3", "localhost", api.AccountTypeUser, "Element web")
			mustMakeAccountAndDevice(t, ctx, accDB, devDB, "user4", "localhost", api.AccountTypeGuest, "Element Electron")
			mustMakeAccountAndDevice(t, ctx, accDB, devDB, "user5", "localhost", api.AccountTypeAdmin, "gecko")
			mustMakeAccountAndDevice(t, ctx, accDB, devDB, "user6", "localhost", api.AccountTypeAppService, "gecko")
			gotStats, _, err := statsDB.UserStatistics(ctx, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			wantStats := &types.UserStatistics{
				RegisteredUsersByType: map[string]int64{
					"native":  4,
					"guest":   1,
					"bridged": 1,
				},
				R30Users: map[string]int64{},
				R30UsersV2: map[string]int64{
					"ios":      0,
					"android":  0,
					"web":      0,
					"electron": 0,
					"all":      0,
				},
				AllUsers:        6,
				NonBridgedUsers: 5,
				DailyUsers:      6,
				MonthlyUsers:    6,
			}
			if !reflect.DeepEqual(gotStats, wantStats) {
				t.Errorf("UserStatistics() gotStats = \n%+v\nwant\n%+v", gotStats, wantStats)
			}
		})

		t.Run("Users not active for one/two month", func(t *testing.T) {
			mustUpdateDeviceLastSeen(t, ctx, db, "user1", time.Now().AddDate(0, 0, -60))
			mustUpdateDeviceLastSeen(t, ctx, db, "user2", time.Now().AddDate(0, 0, -30))
			gotStats, _, err := statsDB.UserStatistics(ctx, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			wantStats := &types.UserStatistics{
				RegisteredUsersByType: map[string]int64{
					"native":  4,
					"guest":   1,
					"bridged": 1,
				},
				R30Users: map[string]int64{},
				R30UsersV2: map[string]int64{
					"ios":      0,
					"android":  0,
					"web":      0,
					"electron": 0,
					"all":      0,
				},
				AllUsers:        6,
				NonBridgedUsers: 5,
				DailyUsers:      4,
				MonthlyUsers:    4,
			}
			if !reflect.DeepEqual(gotStats, wantStats) {
				t.Errorf("UserStatistics() gotStats = \n%+v\nwant\n%+v", gotStats, wantStats)
			}
		})

		/* R30Users counts the number of 30 day retained users, defined as:
		- Users who have created their accounts more than 30 days ago
		- Where last seen at most 30 days ago
		- Where account creation and last_seen are > 30 days apart
		*/
		t.Run("R30Users tests", func(t *testing.T) {
			mustUserUpdateRegistered(t, ctx, db, "user1", time.Now().AddDate(0, 0, -60))
			mustUpdateDeviceLastSeen(t, ctx, db, "user1", time.Now())
			mustUserUpdateRegistered(t, ctx, db, "user4", time.Now().AddDate(0, 0, -60))
			mustUpdateDeviceLastSeen(t, ctx, db, "user4", time.Now())
			startTime := time.Now().AddDate(0, 0, -2)
			err := statsDB.UpdateUserDailyVisits(ctx, nil, startTime, startTime.Truncate(time.Hour*24))
			if err != nil {
				t.Fatalf("unable to update daily visits stats: %v", err)
			}

			gotStats, _, err := statsDB.UserStatistics(ctx, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			wantStats := &types.UserStatistics{
				RegisteredUsersByType: map[string]int64{
					"native":  3,
					"bridged": 1,
				},
				R30Users: map[string]int64{
					"all":      2,
					"android":  1,
					"electron": 1,
				},
				R30UsersV2: map[string]int64{
					"ios":      0,
					"android":  0,
					"web":      0,
					"electron": 0,
					"all":      0,
				},
				AllUsers:        6,
				NonBridgedUsers: 5,
				DailyUsers:      5,
				MonthlyUsers:    5,
			}
			if !reflect.DeepEqual(gotStats, wantStats) {
				t.Errorf("UserStatistics() gotStats = \n%+v\nwant\n%+v", gotStats, wantStats)
			}
		})

		/*
			R30UsersV2 counts the number of 30 day retained users, defined as users that:
			- Appear more than once in the past 60 days
			- Have more than 30 days between the most and least recent appearances that occurred in the past 60 days.
			most recent -> neueste
			least recent -> älteste

		*/
		t.Run("R30UsersV2 tests", func(t *testing.T) {
			// generate some data
			for i := 100; i > 0; i-- {
				mustUpdateDeviceLastSeen(t, ctx, db, "user1", time.Now().AddDate(0, 0, -i))
				mustUpdateDeviceLastSeen(t, ctx, db, "user5", time.Now().AddDate(0, 0, -i))
				startTime := time.Now().AddDate(0, 0, -i)
				err := statsDB.UpdateUserDailyVisits(ctx, nil, startTime, startTime.Truncate(time.Hour*24))
				if err != nil {
					t.Fatalf("unable to update daily visits stats: %v", err)
				}
			}
			gotStats, _, err := statsDB.UserStatistics(ctx, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			wantStats := &types.UserStatistics{
				RegisteredUsersByType: map[string]int64{
					"native":  3,
					"bridged": 1,
				},
				R30Users: map[string]int64{
					"all":      2,
					"android":  1,
					"electron": 1,
				},
				R30UsersV2: map[string]int64{
					"ios":      0,
					"android":  1,
					"web":      1,
					"electron": 0,
					"all":      2,
				},
				AllUsers:        6,
				NonBridgedUsers: 5,
				DailyUsers:      3,
				MonthlyUsers:    5,
			}
			if !reflect.DeepEqual(gotStats, wantStats) {
				t.Errorf("UserStatistics() gotStats = \n%+v\nwant\n%+v", gotStats, wantStats)
			}
		})
	})

}
