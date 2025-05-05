// Copyright 2022 The Global.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/antinvestor/matrix/userapi/types"
	"gorm.io/gorm"
)

// SQL: Create user daily visits table
const userDailyVisitsSchema = `
CREATE TABLE IF NOT EXISTS userapi_daily_visits (
    localpart TEXT NOT NULL,
	device_id TEXT NOT NULL,
	timestamp BIGINT NOT NULL,
	user_agent TEXT
);

-- Device IDs and timestamp must be unique for a given user per day
CREATE UNIQUE INDEX IF NOT EXISTS userapi_daily_visits_localpart_device_timestamp_idx ON userapi_daily_visits(localpart, device_id, timestamp);
CREATE INDEX IF NOT EXISTS userapi_daily_visits_timestamp_idx ON userapi_daily_visits(timestamp);
CREATE INDEX IF NOT EXISTS userapi_daily_visits_localpart_timestamp_idx ON userapi_daily_visits(localpart, timestamp);
`

// SQL: Revert user daily visits table
const userDailyVisitsSchemaRevert = "DROP TABLE IF EXISTS userapi_daily_visits CASCADE; DROP INDEX IF EXISTS userapi_daily_visits_localpart_device_timestamp_idx; DROP INDEX IF EXISTS userapi_daily_visits_timestamp_idx; DROP INDEX IF EXISTS userapi_daily_visits_localpart_timestamp_idx;"

// SQL: Create messages daily stats table
const messagesDailySchema = `
CREATE TABLE IF NOT EXISTS userapi_daily_stats (
	timestamp BIGINT NOT NULL,
	server_name TEXT NOT NULL,
	messages BIGINT NOT NULL,
	sent_messages BIGINT NOT NULL,
	e2ee_messages BIGINT NOT NULL,
	sent_e2ee_messages BIGINT NOT NULL,
	active_rooms BIGINT NOT NULL,
	active_e2ee_rooms BIGINT NOT NULL,
	CONSTRAINT daily_stats_unique UNIQUE (timestamp, server_name)
);
`

// SQL: Revert messages daily stats table
const messagesDailySchemaRevert = "DROP TABLE IF EXISTS userapi_daily_stats CASCADE;"

// SQL: Upsert daily messages
const upsertDailyMessagesSQL = `
	INSERT INTO userapi_daily_stats AS u (timestamp, server_name, messages, sent_messages, e2ee_messages, sent_e2ee_messages, active_rooms, active_e2ee_rooms)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT ON CONSTRAINT daily_stats_unique
	DO UPDATE SET
	    messages=u.messages+excluded.messages, sent_messages=u.sent_messages+excluded.sent_messages,
	    e2ee_messages=u.e2ee_messages+excluded.e2ee_messages, sent_e2ee_messages=u.sent_e2ee_messages+excluded.sent_e2ee_messages,
		active_rooms=GREATEST($7, u.active_rooms), active_e2ee_rooms=GREATEST($8, u.active_e2ee_rooms)
`

// SQL: Select daily messages
const selectDailyMessagesSQL = `
	SELECT messages, sent_messages, e2ee_messages, sent_e2ee_messages, active_rooms, active_e2ee_rooms
	FROM userapi_daily_stats
	WHERE server_name = $1 AND timestamp = $2;
`

// SQL: Count users last seen after
const countUsersLastSeenAfterSQL = "" +
	"SELECT COUNT(*) FROM (" +
	" SELECT localpart FROM userapi_devices WHERE last_seen_ts > $1 " +
	" GROUP BY localpart" +
	" ) u"

// Note on the following countR30UsersSQL and countR30UsersV2SQL: The different checks are intentional.
// This is to ensure the values reported by Dendrite are the same as by Synapse.
// Queries are taken from: https://github.com/matrix-org/synapse/blob/9ce51a47f6e37abd0a1275281806399d874eb026/synapse/storage/databases/main/stats.py

/*
R30Users counts the number of 30 day retained users, defined as:
- Users who have created their accounts more than 30 days ago
- Where last seen at most 30 days ago
- Where account creation and last_seen are > 30 days apart
*/
const countR30UsersSQL = `
SELECT platform, COUNT(*) FROM (
	SELECT users.localpart, platform, users.created_ts, MAX(uip.last_seen_ts)
	FROM userapi_accounts users
	INNER JOIN
	(SELECT 
		localpart, last_seen_ts,
		CASE
	    	WHEN user_agent LIKE '%%Android%%' THEN 'android'
    	    WHEN user_agent LIKE '%%iOS%%' THEN 'ios'
        	WHEN user_agent LIKE '%%Electron%%' THEN 'electron'
        	WHEN user_agent LIKE '%%Mozilla%%' THEN 'web'
        	WHEN user_agent LIKE '%%Gecko%%' THEN 'web'
        	ELSE 'unknown'
		END
    	AS platform
		FROM userapi_devices
	) uip
	ON users.localpart = uip.localpart
	AND users.account_type <> 4
	AND users.created_ts < $1
	AND uip.last_seen_ts > $1
	AND (uip.last_seen_ts) - users.created_ts > $2
	GROUP BY users.localpart, platform, users.created_ts
	) u GROUP BY PLATFORM
`

/*
R30UsersV2 counts the number of 30 day retained users, defined as users that:
- Appear more than once in the past 60 days
- Have more than 30 days between the most and least recent appearances that occurred in the past 60 days.
*/
const countR30UsersV2SQL = `
SELECT
	client_type,
    count(client_type)
FROM 
	(
    	SELECT
        	localpart,
            CASE
            	WHEN
                LOWER(user_agent) LIKE '%%riot%%' OR
				LOWER(user_agent) LIKE '%%element%%'
                THEN CASE
                	WHEN LOWER(user_agent) LIKE '%%electron%%' THEN 'electron'
					WHEN LOWER(user_agent) LIKE '%%android%%' THEN 'android'
					WHEN LOWER(user_agent) LIKE '%%ios%%' THEN 'ios'
					ELSE 'unknown'
				END
				WHEN LOWER(user_agent) LIKE '%%mozilla%%' OR LOWER(user_agent) LIKE '%%gecko%%' THEN 'web'
				ELSE 'unknown'
			END as client_type
		FROM userapi_daily_visits
		WHERE timestamp > $1 AND timestamp < $2
		GROUP BY localpart, client_type
		HAVING max(timestamp) - min(timestamp) > $3
	) AS temp
GROUP BY client_type
`

// SQL: Count user by account type
const countUserByAccountTypeSQL = `
SELECT COUNT(*) FROM userapi_accounts WHERE account_type = ANY($1)
`

// $1 = All non guest AccountType IDs
// $2 = Guest AccountType
const countRegisteredUserByTypeStmt = `
SELECT user_type, COUNT(*) AS count FROM (
	SELECT
    CASE
    	WHEN account_type = ANY($1) AND appservice_id IS NULL THEN 'native'
        WHEN account_type = $2 AND appservice_id IS NULL THEN 'guest'
        WHEN account_type = ANY($1) AND appservice_id IS NOT NULL THEN 'bridged'
	END AS user_type
    FROM userapi_accounts
    WHERE created_ts > $3
) AS t GROUP BY user_type
`

// SQL: Update user daily visits
const updateUserDailyVisitsSQL = `
INSERT INTO userapi_daily_visits(localpart, device_id, timestamp, user_agent)
	SELECT u.localpart, u.device_id, $1, MAX(u.user_agent)
	FROM userapi_devices AS u
	LEFT JOIN (
		SELECT localpart, device_id, timestamp FROM userapi_daily_visits
		WHERE timestamp = $1
	) udv
	ON u.localpart = udv.localpart AND u.device_id = udv.device_id
	INNER JOIN userapi_devices d ON d.localpart = u.localpart
	INNER JOIN userapi_accounts a ON a.localpart = u.localpart
	WHERE $2 <= d.last_seen_ts AND d.last_seen_ts < $3
	AND a.account_type in (1, 3)
	GROUP BY u.localpart, u.device_id
ON CONFLICT (localpart, device_id, timestamp) DO NOTHING
;
`

// SQL: Query DB engine version
const queryDBEngineVersion = "SHOW server_version;"

// statsTable implements tables.StatsTable using GORM and a connection manager.
type statsTable struct {
	cm *sqlutil.Connections

	// SQL queries assigned from constants at the top; see comments above each constant for details.
	selectDailyMessagesSQL       string // Select daily messages
	upsertDailyMessagesSQL       string // Upsert daily messages
	countUsersLastSeenAfterSQL   string // Count users last seen after
	countR30UsersSQL             string // Count R30 users
	countR30UsersV2SQL           string // Count R30 users V2
	countUserByAccountTypeSQL    string // Count user by account type
	countRegisteredUserByTypeSQL string // Count registered user by type
	updateUserDailyVisitsSQL     string // Update user daily visits
	queryDBEngineVersion         string // Query DB engine version
}

// NewPostgresStatsTable returns a new StatsTable using the provided connection manager.
func NewPostgresStatsTable(cm *sqlutil.Connections) tables.StatsTable {
	return &statsTable{
		cm:                           cm,
		selectDailyMessagesSQL:       selectDailyMessagesSQL,
		upsertDailyMessagesSQL:       upsertDailyMessagesSQL,
		countUsersLastSeenAfterSQL:   countUsersLastSeenAfterSQL,
		countR30UsersSQL:             countR30UsersSQL,
		countR30UsersV2SQL:           countR30UsersV2SQL,
		countUserByAccountTypeSQL:    countUserByAccountTypeSQL,
		countRegisteredUserByTypeSQL: countRegisteredUserByTypeStmt,
		updateUserDailyVisitsSQL:     updateUserDailyVisitsSQL,
		queryDBEngineVersion:         queryDBEngineVersion,
	}
}

func (s *statsTable) startTimers(ctx context.Context) {
	var updateStatsFunc func()
	updateStatsFunc = func() {
		logrus.Infof("Executing UpdateUserDailyVisits")
		if err := s.UpdateUserDailyVisits(ctx, time.Now(), time.Now()); err != nil {
			logrus.WithError(err).Error("failed to update daily user visits")
		}
		time.AfterFunc(time.Hour*3, updateStatsFunc)
	}
	time.AfterFunc(time.Minute*5, updateStatsFunc)
}

func (s *statsTable) allUsers(ctx context.Context) (result int64, err error) {
	db := s.cm.Connection(ctx, true)
	var count int64
	err = db.Raw(countUserByAccountTypeSQL,
		pq.Int64Array{
			int64(api.AccountTypeUser),
			int64(api.AccountTypeGuest),
			int64(api.AccountTypeAdmin),
			int64(api.AccountTypeAppService),
		},
	).Count(&count).Error
	result = count
	return
}

func (s *statsTable) nonBridgedUsers(ctx context.Context) (result int64, err error) {
	db := s.cm.Connection(ctx, true)
	var count int64
	err = db.Raw(countUserByAccountTypeSQL,
		pq.Int64Array{
			int64(api.AccountTypeUser),
			int64(api.AccountTypeGuest),
			int64(api.AccountTypeAdmin),
		},
	).Count(&count).Error
	result = count
	return
}

func (s *statsTable) registeredUserByType(ctx context.Context) (map[string]int64, error) {
	db := s.cm.Connection(ctx, true)
	registeredAfter := time.Now().AddDate(0, 0, -30)

	rows, err := db.Raw(s.countRegisteredUserByTypeSQL,
		pq.Int64Array{
			int64(api.AccountTypeUser),
			int64(api.AccountTypeAdmin),
			int64(api.AccountTypeAppService),
		},
		api.AccountTypeGuest,
		spec.AsTimestamp(registeredAfter),
	).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "RegisteredUserByType: failed to close rows")

	var userType sql.NullString
	var count int64
	var result = make(map[string]int64)
	for rows.Next() {
		if err = rows.Scan(&userType, &count); err != nil {
			return nil, err
		}

		if userType.Valid {
			result[userType.String] = count
		}

	}

	return result, rows.Err()
}

func (s *statsTable) dailyUsers(ctx context.Context) (result int64, err error) {
	db := s.cm.Connection(ctx, true)
	var count int64
	lastSeenAfter := time.Now().AddDate(0, 0, -1)
	err = db.Raw(countUsersLastSeenAfterSQL,
		spec.AsTimestamp(lastSeenAfter),
	).Count(&count).Error
	result = count
	return
}

func (s *statsTable) monthlyUsers(ctx context.Context) (result int64, err error) {
	db := s.cm.Connection(ctx, true)
	var count int64
	lastSeenAfter := time.Now().AddDate(0, 0, -30)
	err = db.Raw(countUsersLastSeenAfterSQL,
		spec.AsTimestamp(lastSeenAfter),
	).Count(&count).Error
	result = count
	return
}

/*
R30Users counts the number of 30 day retained users, defined as:
- Users who have created their accounts more than 30 days ago
- Where last seen at most 30 days ago
- Where account creation and last_seen are > 30 days apart
*/
func (s *statsTable) r30Users(ctx context.Context) (map[string]int64, error) {
	db := s.cm.Connection(ctx, true)
	lastSeenAfter := time.Now().AddDate(0, 0, -30)
	diff := time.Hour * 24 * 30

	rows, err := db.Raw(countR30UsersSQL,
		spec.AsTimestamp(lastSeenAfter),
		diff.Milliseconds(),
	).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "R30Users: failed to close rows")

	var platform string
	var count int64
	var result = make(map[string]int64)
	for rows.Next() {
		if err = rows.Scan(&platform, &count); err != nil {
			return nil, err
		}
		if platform == "unknown" {
			continue
		}
		result["all"] += count
		result[platform] = count
	}

	return result, rows.Err()
}

/*
R30UsersV2 counts the number of 30 day retained users, defined as users that:
- Appear more than once in the past 60 days
- Have more than 30 days between the most and least recent appearances that occurred in the past 60 days.
*/
func (s *statsTable) r30UsersV2(ctx context.Context) (map[string]int64, error) {
	db := s.cm.Connection(ctx, true)
	sixtyDaysAgo := time.Now().AddDate(0, 0, -60)
	diff := time.Hour * 24 * 30
	tomorrow := time.Now().Add(time.Hour * 24)

	rows, err := db.Raw(countR30UsersV2SQL,
		spec.AsTimestamp(sixtyDaysAgo),
		spec.AsTimestamp(tomorrow),
		diff.Milliseconds(),
	).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "R30UsersV2: failed to close rows")

	var platform string
	var count int64
	var result = map[string]int64{
		"ios":      0,
		"android":  0,
		"web":      0,
		"electron": 0,
		"all":      0,
	}
	for rows.Next() {
		if err = rows.Scan(&platform, &count); err != nil {
			return nil, err
		}
		if _, ok := result[platform]; !ok {
			continue
		}
		result["all"] += count
		result[platform] = count
	}

	return result, rows.Err()
}

// UserStatistics collects some information about users on this instance.
// Returns the stats itself as well as the database engine version and type.
// On error, returns the stats collected up to the error.
func (s *statsTable) UserStatistics(ctx context.Context) (*types.UserStatistics, *types.DatabaseEngine, error) {
	var (
		stats = &types.UserStatistics{
			R30UsersV2: map[string]int64{
				"ios":      0,
				"android":  0,
				"web":      0,
				"electron": 0,
				"all":      0,
			},
			R30Users:              map[string]int64{},
			RegisteredUsersByType: map[string]int64{},
		}
		dbEngine = &types.DatabaseEngine{Engine: "Postgres", Version: "unknown"}
		err      error
	)
	stats.AllUsers, err = s.allUsers(ctx)
	if err != nil {
		return stats, dbEngine, err
	}
	stats.DailyUsers, err = s.dailyUsers(ctx)
	if err != nil {
		return stats, dbEngine, err
	}
	stats.MonthlyUsers, err = s.monthlyUsers(ctx)
	if err != nil {
		return stats, dbEngine, err
	}
	stats.R30Users, err = s.r30Users(ctx)
	if err != nil {
		return stats, dbEngine, err
	}
	stats.R30UsersV2, err = s.r30UsersV2(ctx)
	if err != nil {
		return stats, dbEngine, err
	}
	stats.NonBridgedUsers, err = s.nonBridgedUsers(ctx)
	if err != nil {
		return stats, dbEngine, err
	}
	stats.RegisteredUsersByType, err = s.registeredUserByType(ctx)
	if err != nil {
		return stats, dbEngine, err
	}

	db := s.cm.Connection(ctx, true)
	err = db.Raw(queryDBEngineVersion).Row().Scan(&dbEngine.Version)
	return stats, dbEngine, err
}

func (s *statsTable) UpdateUserDailyVisits(ctx context.Context, startTime, lastUpdate time.Time) error {
	db := s.cm.Connection(ctx, false)
	startTime = startTime.Truncate(time.Hour * 24)

	// edge case
	if startTime.After(lastUpdate) {
		startTime = startTime.AddDate(0, 0, -1)
	}
	result := db.Exec(updateUserDailyVisitsSQL,
		spec.AsTimestamp(startTime),
		spec.AsTimestamp(lastUpdate),
		spec.AsTimestamp(time.Now()),
	)
	return result.Error
}

func (s *statsTable) UpsertDailyStats(ctx context.Context, serverName spec.ServerName, stats types.MessageStats, activeRooms, activeE2EERooms int64) error {
	db := s.cm.Connection(ctx, false)
	timestamp := time.Now().Truncate(time.Hour * 24)
	result := db.Exec(upsertDailyMessagesSQL,
		spec.AsTimestamp(timestamp),
		serverName,
		stats.Messages, stats.SentMessages, stats.MessagesE2EE, stats.SentMessagesE2EE,
		activeRooms, activeE2EERooms,
	)
	return result.Error
}

func (s *statsTable) DailyRoomsMessages(ctx context.Context, serverName spec.ServerName) (msgStats types.MessageStats, activeRooms, activeE2EERooms int64, err error) {
	db := s.cm.Connection(ctx, true)
	timestamp := time.Now().Truncate(time.Hour * 24)

	err = db.Raw(selectDailyMessagesSQL, serverName, spec.AsTimestamp(timestamp)).
		Row().Scan(&msgStats.Messages, &msgStats.SentMessages, &msgStats.MessagesE2EE, &msgStats.SentMessagesE2EE, &activeRooms, &activeE2EERooms)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return msgStats, 0, 0, err
	}
	return msgStats, activeRooms, activeE2EERooms, nil
}
