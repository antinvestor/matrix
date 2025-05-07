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
	"errors"
	"time"

	"github.com/lib/pq"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

const deviceKeysSchema = `
-- Stores device keys for users
CREATE TABLE IF NOT EXISTS keyserver_device_keys (
    user_id TEXT NOT NULL,
	device_id TEXT NOT NULL,
	ts_added_secs BIGINT NOT NULL,
	key_json TEXT NOT NULL,
	-- the stream ID of this key, scoped per-user. This gets updated when the device key changes.
	-- This means we do not store an unbounded append-only log of device keys, which is not actually
	-- required in the spec because in the event of a missed update the server fetches the entire
	-- current set of keys rather than trying to 'fast-forward' or catchup missing stream IDs.
	stream_id BIGINT NOT NULL,
	display_name TEXT,
	-- Clobber based on tuple of user/device.
    CONSTRAINT keyserver_device_keys_unique UNIQUE (user_id, device_id)
);
`

// SQL query constants
const (
	upsertDeviceKeysSQL = "" +
		"INSERT INTO keyserver_device_keys (user_id, device_id, ts_added_secs, key_json, stream_id, display_name)" +
		" VALUES ($1, $2, $3, $4, $5, $6)" +
		" ON CONFLICT ON CONSTRAINT keyserver_device_keys_unique" +
		" DO UPDATE SET key_json = $4, stream_id = $5, display_name = $6"

	selectDeviceKeysSQL = "" +
		"SELECT key_json, stream_id, display_name FROM keyserver_device_keys WHERE user_id=$1 AND device_id=$2"

	selectBatchDeviceKeysSQL = "" +
		"SELECT device_id, key_json, stream_id, display_name FROM keyserver_device_keys WHERE user_id=$1 AND key_json <> ''"

	selectBatchDeviceKeysWithEmptiesSQL = "" +
		"SELECT device_id, key_json, stream_id, display_name FROM keyserver_device_keys WHERE user_id=$1"

	selectMaxStreamForUserSQL = "" +
		"SELECT MAX(stream_id) FROM keyserver_device_keys WHERE user_id=$1"

	countStreamIDsForUserSQL = "" +
		"SELECT COUNT(*) FROM keyserver_device_keys WHERE user_id=$1 AND stream_id = ANY($2)"

	deleteDeviceKeysSQL = "" +
		"DELETE FROM keyserver_device_keys WHERE user_id=$1 AND device_id=$2"

	deleteAllDeviceKeysSQL = "" +
		"DELETE FROM keyserver_device_keys WHERE user_id=$1"
)

type deviceKeysTable struct {
	cm *sqlutil.Connections

	// SQL queries
	upsertDeviceKeysStmt                 string
	selectDeviceKeysStmt                 string
	selectBatchDeviceKeysStmt            string
	selectBatchDeviceKeysWithEmptiesStmt string
	selectMaxStreamForUserStmt           string
	countStreamIDsForUserStmt            string
	deleteDeviceKeysStmt                 string
	deleteAllDeviceKeysStmt              string
}

func NewPostgresDeviceKeysTable(ctx context.Context, cm *sqlutil.Connections) (tables.DeviceKeys, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(deviceKeysSchema).Error; err != nil {
		return nil, err
	}

	// Initialize table with SQL statements
	t := &deviceKeysTable{
		cm: cm,

		// SQL statements
		upsertDeviceKeysStmt:                 upsertDeviceKeysSQL,
		selectDeviceKeysStmt:                 selectDeviceKeysSQL,
		selectBatchDeviceKeysStmt:            selectBatchDeviceKeysSQL,
		selectBatchDeviceKeysWithEmptiesStmt: selectBatchDeviceKeysWithEmptiesSQL,
		selectMaxStreamForUserStmt:           selectMaxStreamForUserSQL,
		countStreamIDsForUserStmt:            countStreamIDsForUserSQL,
		deleteDeviceKeysStmt:                 deleteDeviceKeysSQL,
		deleteAllDeviceKeysStmt:              deleteAllDeviceKeysSQL,
	}

	return t, nil
}

func (t *deviceKeysTable) SelectDeviceKeysJSON(ctx context.Context, keys []api.DeviceMessage) error {
	for i, key := range keys {
		// Get read-only database connection
		db := t.cm.Connection(ctx, true)

		var keyJSONStr string
		var streamID int64
		var displayName sql.NullString

		row := db.Raw(t.selectDeviceKeysStmt, key.UserID, key.DeviceID).Row()
		err := row.Scan(&keyJSONStr, &streamID, &displayName)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		// this will be '' when there is no device
		keys[i].Type = api.TypeDeviceKeyUpdate
		keys[i].KeyJSON = []byte(keyJSONStr)
		keys[i].StreamID = streamID
		if displayName.Valid {
			keys[i].DisplayName = displayName.String
		}
	}
	return nil
}

func (t *deviceKeysTable) SelectMaxStreamIDForUser(ctx context.Context, userID string) (streamID int64, err error) {
	// Get read-only database connection
	db := t.cm.Connection(ctx, true)

	// nullable if there are no results
	var nullStream sql.NullInt64
	err = db.Raw(t.selectMaxStreamForUserStmt, userID).Row().Scan(&nullStream)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	if nullStream.Valid {
		streamID = nullStream.Int64
	}
	return
}

func (t *deviceKeysTable) CountStreamIDsForUser(ctx context.Context, userID string, streamIDs []int64) (int, error) {
	// Get read-only database connection
	db := t.cm.Connection(ctx, true)

	// nullable if there are no results
	var count sql.NullInt32
	err := db.Raw(t.countStreamIDsForUserStmt, userID, pq.Int64Array(streamIDs)).Row().Scan(&count)
	if err != nil {
		return 0, err
	}
	if count.Valid {
		return int(count.Int32), nil
	}
	return 0, nil
}

func (t *deviceKeysTable) InsertDeviceKeys(ctx context.Context, keys []api.DeviceMessage) error {
	// Get database connection
	db := t.cm.Connection(ctx, false)

	for _, key := range keys {
		now := time.Now().Unix()
		if err := db.Exec(
			t.upsertDeviceKeysStmt,
			key.UserID, key.DeviceID, now, string(key.KeyJSON), key.StreamID, key.DisplayName,
		).Error; err != nil {
			return err
		}
	}
	return nil
}

func (t *deviceKeysTable) DeleteDeviceKeys(ctx context.Context, userID, deviceID string) error {
	// Get database connection
	db := t.cm.Connection(ctx, false)

	return db.Exec(t.deleteDeviceKeysStmt, userID, deviceID).Error
}

func (t *deviceKeysTable) DeleteAllDeviceKeys(ctx context.Context, userID string) error {
	// Get database connection
	db := t.cm.Connection(ctx, false)

	return db.Exec(t.deleteAllDeviceKeysStmt, userID).Error
}

func (t *deviceKeysTable) SelectBatchDeviceKeys(ctx context.Context, userID string, deviceIDs []string, includeEmpty bool) ([]api.DeviceMessage, error) {
	// Get read-only database connection
	db := t.cm.Connection(ctx, true)

	// Select appropriate statement based on includeEmpty flag
	var stmt string
	if includeEmpty {
		stmt = t.selectBatchDeviceKeysWithEmptiesStmt
	} else {
		stmt = t.selectBatchDeviceKeysStmt
	}

	// Execute query
	rows, err := db.Raw(stmt, userID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectBatchDeviceKeysStmt: rows.close() failed")

	// Create device ID map for lookup
	deviceIDMap := make(map[string]bool)
	for _, d := range deviceIDs {
		deviceIDMap[d] = true
	}

	// Process results
	var result []api.DeviceMessage
	var displayName sql.NullString
	for rows.Next() {
		dk := api.DeviceMessage{
			Type: api.TypeDeviceKeyUpdate,
			DeviceKeys: &api.DeviceKeys{
				UserID: userID,
			},
		}
		if err := rows.Scan(&dk.DeviceID, &dk.KeyJSON, &dk.StreamID, &displayName); err != nil {
			return nil, err
		}
		if displayName.Valid {
			dk.DisplayName = displayName.String
		}
		// include the key if we want all keys (no device) or it was asked
		if deviceIDMap[dk.DeviceID] || len(deviceIDs) == 0 {
			result = append(result, dk)
		}
	}
	return result, rows.Err()
}
