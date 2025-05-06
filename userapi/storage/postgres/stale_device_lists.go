// Copyright 2020 The Global.org Foundation C.I.C.
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
)

const staleDeviceListsSchema = `
-- Stores whether a user's device lists are stale or not.
CREATE TABLE IF NOT EXISTS keyserver_stale_device_lists (
    user_id TEXT PRIMARY KEY NOT NULL,
	domain TEXT NOT NULL,
	is_stale BOOLEAN NOT NULL,
	ts_added_secs BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS keyserver_stale_device_lists_idx ON keyserver_stale_device_lists (domain, is_stale);
`

const staleDeviceListsSchemaRevert = "DROP TABLE IF EXISTS keyserver_stale_device_lists CASCADE; DROP INDEX IF EXISTS keyserver_stale_device_lists_idx;"

// SQL: Upsert a stale device list
const upsertStaleDeviceListSQL = "" +
	"INSERT INTO keyserver_stale_device_lists (user_id, domain, is_stale, ts_added_secs)" +
	" VALUES ($1, $2, $3, $4)" +
	" ON CONFLICT (user_id)" +
	" DO UPDATE SET is_stale = $3, ts_added_secs = $4"

// SQL: Select stale device lists with domains
const selectStaleDeviceListsWithDomainsSQL = "" +
	"SELECT user_id FROM keyserver_stale_device_lists WHERE is_stale = $1 AND domain = $2 ORDER BY ts_added_secs DESC"

// SQL: Select all stale device lists
const selectStaleDeviceListsSQL = "" +
	"SELECT user_id FROM keyserver_stale_device_lists WHERE is_stale = $1 ORDER BY ts_added_secs DESC"

// SQL: Delete stale device lists by user IDs
const deleteStaleDeviceListsSQL = "" +
	"DELETE FROM keyserver_stale_device_lists WHERE user_id = ANY($1)"

// staleDeviceListsTable implements tables.StaleDeviceListsTable using GORM and a connection manager.
type staleDeviceListsTable struct {
	cm *sqlutil.Connections

	upsertStaleDeviceListSQL             string
	selectStaleDeviceListsWithDomainsSQL string
	selectStaleDeviceListsSQL            string
	deleteStaleDeviceListsSQL            string
}

// NewPostgresStaleDeviceListsTable returns a new StaleDeviceListsTable using the provided connection manager.
func NewPostgresStaleDeviceListsTable(cm *sqlutil.Connections) tables.StaleDeviceLists {
	return &staleDeviceListsTable{
		cm:                                   cm,
		upsertStaleDeviceListSQL:             upsertStaleDeviceListSQL,
		selectStaleDeviceListsWithDomainsSQL: selectStaleDeviceListsWithDomainsSQL,
		selectStaleDeviceListsSQL:            selectStaleDeviceListsSQL,
		deleteStaleDeviceListsSQL:            deleteStaleDeviceListsSQL,
	}
}

func (t *staleDeviceListsTable) InsertStaleDeviceList(ctx context.Context, userID string, isStale bool) error {
	_, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		return err
	}
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.upsertStaleDeviceListSQL, userID, string(domain), isStale, spec.AsTimestamp(time.Now()))
	return result.Error
}

func (t *staleDeviceListsTable) SelectUserIDsWithStaleDeviceLists(ctx context.Context, domains []spec.ServerName) ([]string, error) {
	// we only query for 1 domain or all domains so optimise for those use cases
	if len(domains) == 0 {
		db := t.cm.Connection(ctx, true)
		rows, err := db.Raw(t.selectStaleDeviceListsSQL, true).Rows()
		if err != nil {
			return nil, err
		}
		return rowsToUserIDs(ctx, rows)
	}
	var result []string
	for _, domain := range domains {
		db := t.cm.Connection(ctx, true)
		rows, err := db.Raw(t.selectStaleDeviceListsWithDomainsSQL, true, string(domain)).Rows()
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
func (t *staleDeviceListsTable) DeleteStaleDeviceLists(ctx context.Context, userIDs []string) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteStaleDeviceListsSQL, pq.Array(userIDs))
	return result.Error
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
