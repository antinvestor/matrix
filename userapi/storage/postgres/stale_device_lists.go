// Copyright 2025 Ant Investor Ltd.
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
	"github.com/pitabwire/frame"
)

// staleDeviceListsSchema defines the schema for the stale device lists table.
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

// staleDeviceListsSchemaRevert defines how to revert the stale device lists table schema.
const staleDeviceListsSchemaRevert = `
DROP TABLE IF EXISTS keyserver_stale_device_lists;
`

// upsertStaleDeviceListSQL is used to insert or update a stale device list entry.
const upsertStaleDeviceListSQL = "" +
	"INSERT INTO keyserver_stale_device_lists (user_id, domain, is_stale, ts_added_secs)" +
	" VALUES ($1, $2, $3, $4)" +
	" ON CONFLICT (user_id)" +
	" DO UPDATE SET is_stale = $3, ts_added_secs = $4"

// selectStaleDeviceListsWithDomainsSQL is used to retrieve stale device lists for a specific domain.
const selectStaleDeviceListsWithDomainsSQL = "" +
	"SELECT user_id FROM keyserver_stale_device_lists WHERE is_stale = $1 AND domain = $2 ORDER BY ts_added_secs DESC"

// selectStaleDeviceListsSQL is used to retrieve all stale device lists.
const selectStaleDeviceListsSQL = "" +
	"SELECT user_id FROM keyserver_stale_device_lists WHERE is_stale = $1 ORDER BY ts_added_secs DESC"

// deleteStaleDevicesSQL is used to delete stale device lists for the given user IDs.
const deleteStaleDevicesSQL = "" +
	"DELETE FROM keyserver_stale_device_lists WHERE user_id = ANY($1)"

type staleDeviceListsTable struct {
	cm                                   sqlutil.ConnectionManager
	upsertStaleDeviceListSQL             string
	selectStaleDeviceListsWithDomainsSQL string
	selectStaleDeviceListsSQL            string
	deleteStaleDevicesSQL                string
}

// NewPostgresStaleDeviceListsTable creates a new postgres stale device lists table.
func NewPostgresStaleDeviceListsTable(_ context.Context, cm sqlutil.ConnectionManager) (tables.StaleDeviceLists, error) {
	t := &staleDeviceListsTable{
		cm:                                   cm,
		upsertStaleDeviceListSQL:             upsertStaleDeviceListSQL,
		selectStaleDeviceListsWithDomainsSQL: selectStaleDeviceListsWithDomainsSQL,
		selectStaleDeviceListsSQL:            selectStaleDeviceListsSQL,
		deleteStaleDevicesSQL:                deleteStaleDevicesSQL,
	}

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "keyserver_stale_device_lists_table_schema_001",
		Patch:       staleDeviceListsSchema,
		RevertPatch: staleDeviceListsSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// InsertStaleDeviceList inserts a stale device list entry for the given user ID.
func (t *staleDeviceListsTable) InsertStaleDeviceList(ctx context.Context, userID string, isStale bool) error {
	_, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		return err
	}
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.upsertStaleDeviceListSQL, userID, string(domain), isStale, spec.AsTimestamp(time.Now())).Error
}

// SelectUserIDsWithStaleDeviceLists retrieves user IDs with stale device lists.
func (t *staleDeviceListsTable) SelectUserIDsWithStaleDeviceLists(ctx context.Context, domains []spec.ServerName) ([]string, error) {
	db := t.cm.Connection(ctx, true)

	// we only query for 1 domain or all domains so optimise for those use cases
	if len(domains) == 0 {
		rows, err := db.Raw(t.selectStaleDeviceListsSQL, true).Rows()
		if err != nil {
			return nil, err
		}
		return rowsToUserIDs(ctx, rows)
	}

	var result []string
	for _, domain := range domains {
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

// DeleteStaleDeviceLists removes users from stale device lists.
func (t *staleDeviceListsTable) DeleteStaleDeviceLists(
	ctx context.Context, userIDs []string,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteStaleDevicesSQL, pq.Array(userIDs)).Error
}

// rowsToUserIDs converts SQL rows to a slice of user IDs.
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
