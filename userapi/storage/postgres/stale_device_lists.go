// Copyright 2020 The Matrix.org Foundation C.I.C.
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
	"time"

	"github.com/lib/pq"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"gorm.io/gorm"
)

var staleDeviceListsSchema = `
-- Stores whether a user's device lists are stale or not.
CREATE TABLE IF NOT EXISTS keyserver_stale_device_lists (
    user_id TEXT PRIMARY KEY NOT NULL,
	domain TEXT NOT NULL,
	is_stale BOOLEAN NOT NULL,
	ts_added_secs BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS keyserver_stale_device_lists_idx ON keyserver_stale_device_lists (domain, is_stale);
`

type staleDeviceListsTable struct {
	cm *sqlutil.Connections

	upsertStaleDeviceListStmt             string
	selectStaleDeviceListsWithDomainsStmt string
	selectStaleDeviceListsStmt            string
	deleteStaleDeviceListsStmt            string
}

func NewPostgresStaleDeviceListsTable(ctx context.Context, cm *sqlutil.Connections) (tables.StaleDeviceLists, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(staleDeviceListsSchema).Error; err != nil {
		return nil, err
	}

	// Initialize table with SQL statements
	t := &staleDeviceListsTable{
		cm: cm,
		upsertStaleDeviceListStmt: "INSERT INTO keyserver_stale_device_lists (user_id, domain, is_stale, ts_added_secs)" +
			" VALUES ($1, $2, $3, $4)" +
			" ON CONFLICT (user_id)" +
			" DO UPDATE SET is_stale = $3, ts_added_secs = $4",
		selectStaleDeviceListsWithDomainsStmt: "SELECT user_id FROM keyserver_stale_device_lists WHERE is_stale = $1 AND domain = $2 ORDER BY ts_added_secs DESC",
		selectStaleDeviceListsStmt:            "SELECT user_id FROM keyserver_stale_device_lists WHERE is_stale = $1 ORDER BY ts_added_secs DESC",
		deleteStaleDeviceListsStmt:            "DELETE FROM keyserver_stale_device_lists WHERE user_id = ANY($1)",
	}

	return t, nil
}

func (s *staleDeviceListsTable) InsertStaleDeviceList(ctx context.Context, userID string, isStale bool) error {
	_, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		return err
	}

	db := s.cm.Connection(ctx, false)
	err = db.Exec(s.upsertStaleDeviceListStmt, userID, string(domain), isStale, spec.AsTimestamp(time.Now())).Error
	return err
}

func (s *staleDeviceListsTable) SelectUserIDsWithStaleDeviceLists(ctx context.Context, domains []spec.ServerName) ([]string, error) {
	db := s.cm.Connection(ctx, true)

	// we only query for 1 domain or all domains so optimise for those use cases
	if len(domains) == 0 {
		rows, err := db.Raw(s.selectStaleDeviceListsStmt, true).Rows()
		if err != nil {
			return nil, err
		}
		return rowsToUserIDs(ctx, rows)
	}

	var result []string
	for _, domain := range domains {
		rows, err := db.Raw(s.selectStaleDeviceListsWithDomainsStmt, true, string(domain)).Rows()
		if err != nil {
			return nil, err
		}
		userIDs, err := rowsToUserIDs(ctx, rows)
		if err != nil {
			return nil, err
		}
		result = append(result, userIDs...)
	}
	return result, nil
}

// DeleteStaleDeviceLists removes users from stale device lists
func (s *staleDeviceListsTable) DeleteStaleDeviceLists(
	ctx context.Context, userIDs []string,
) error {
	db := s.cm.Connection(ctx, false)

	return db.Exec(s.deleteStaleDeviceListsStmt, pq.Array(userIDs)).Error
}

func rowsToUserIDs(ctx context.Context, rows *sql.Rows) (result []string, err error) {
	defer internal.CloseAndLogIfError(ctx, rows, "closing rowsToUserIDs failed")
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		result = append(result, userID)
	}
	return result, rows.Err()
}
