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
	"errors"
	"time"

	"github.com/lib/pq"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"gorm.io/gorm"
)

// SQL: Create device keys table
// Stores device keys for each user/device pair. This table is used to persist the public keys associated with a device.
const deviceKeysSchema = `
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

const deviceKeysSchemaRevert = "DROP TABLE IF EXISTS keyserver_device_keys CASCADE;"

const upsertDeviceKeysSQL = "" +
	"INSERT INTO keyserver_device_keys (user_id, device_id, ts_added_secs, key_json, stream_id, display_name)" +
	" VALUES ($1, $2, $3, $4, $5, $6)" +
	" ON CONFLICT ON CONSTRAINT keyserver_device_keys_unique" +
	" DO UPDATE SET key_json = $4, stream_id = $5, display_name = $6"

const selectDeviceKeysSQL = "" +
	"SELECT key_json, stream_id, display_name FROM keyserver_device_keys WHERE user_id=$1 AND device_id=$2"

const selectBatchDeviceKeysSQL = "" +
	"SELECT device_id, key_json, stream_id, display_name FROM keyserver_device_keys WHERE user_id=$1 AND key_json <> ''"

const selectBatchDeviceKeysWithEmptiesSQL = "" +
	"SELECT device_id, key_json, stream_id, display_name FROM keyserver_device_keys WHERE user_id=$1"

// SQL: Create device keys stream table
// Used for tracking device key changes for incremental sync.
const createDeviceKeysStreamTableSQL = `
CREATE TABLE IF NOT EXISTS keyserver_device_keys_stream (
    user_id TEXT NOT NULL,         -- The Global user ID for this device
    device_id TEXT NOT NULL,       -- The device ID for this user
    stream_id BIGINT NOT NULL,     -- The stream ordering for this key update
    PRIMARY KEY (user_id, device_id, stream_id)
);`

// SQL: Select max stream ID for user
// Gets the max stream ID for a user's device keys.
const selectMaxStreamForUserSQL = "SELECT MAX(stream_id) FROM keyserver_device_keys WHERE user_id=$1"

// SQL: Count stream IDs for user
// Counts how many device keys a user has with a given set of stream IDs.
const countStreamIDsForUserSQL = "SELECT COUNT(*) FROM keyserver_device_keys WHERE user_id=$1 AND stream_id = ANY($2)"

// SQL: Delete device keys
// Deletes the device key for a specific user/device pair.
const deleteDeviceKeysSQL = "DELETE FROM keyserver_device_keys WHERE user_id=$1 AND device_id=$2"

// SQL: Delete all device keys
// Deletes all device keys for a user.
const deleteAllDeviceKeysSQL = "DELETE FROM keyserver_device_keys WHERE user_id=$1"

// deviceKeysTable implements tables.DeviceKeysTable using GORM and a connection manager.
// Provides methods for inserting, selecting, and deleting device keys and their associated metadata.
type deviceKeysTable struct {
	cm *sqlutil.Connections

	// Schema and query SQLs assigned from constants at the top; see comments above each constant for details.
	createDeviceKeysStreamTableSQL      string // Create device keys stream table
	upsertDeviceKeysSQL                 string // Upsert device keys
	selectDeviceKeysSQL                 string // Select device keys
	selectBatchDeviceKeysSQL            string // Select batch device keys
	selectBatchDeviceKeysWithEmptiesSQL string // Select batch device keys with empties
	selectMaxStreamForUserSQL           string // Select max stream ID for user
	countStreamIDsForUserSQL            string // Count stream IDs for user
	deleteDeviceKeysSQL                 string // Delete device keys
	deleteAllDeviceKeysSQL              string // Delete all device keys
}

// NewPostgresDeviceKeysTable returns a new DeviceKeysTable using the provided connection manager.
// This struct provides all methods required to persist and query device keys for end-to-end encryption.
func NewPostgresDeviceKeysTable(cm *sqlutil.Connections) tables.DeviceKeys {
	return &deviceKeysTable{
		cm:                                  cm,
		createDeviceKeysStreamTableSQL:      createDeviceKeysStreamTableSQL,
		upsertDeviceKeysSQL:                 upsertDeviceKeysSQL,
		selectDeviceKeysSQL:                 selectDeviceKeysSQL,
		selectBatchDeviceKeysSQL:            selectBatchDeviceKeysSQL,
		selectBatchDeviceKeysWithEmptiesSQL: selectBatchDeviceKeysWithEmptiesSQL,
		selectMaxStreamForUserSQL:           selectMaxStreamForUserSQL,
		countStreamIDsForUserSQL:            countStreamIDsForUserSQL,
		deleteDeviceKeysSQL:                 deleteDeviceKeysSQL,
		deleteAllDeviceKeysSQL:              deleteAllDeviceKeysSQL,
	}
}

// InsertDeviceKeys inserts device keys for multiple devices.
func (t *deviceKeysTable) InsertDeviceKeys(ctx context.Context, keys []api.DeviceMessage) error {
	db := t.cm.Connection(ctx, false)
	for _, key := range keys {
		now := time.Now().Unix()
		result := db.Exec(t.upsertDeviceKeysSQL, key.UserID, key.DeviceID, now, string(key.KeyJSON), key.StreamID, key.DisplayName)
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// SelectDeviceKeysJSON retrieves device keys for a batch of device IDs.
func (t *deviceKeysTable) SelectDeviceKeysJSON(ctx context.Context, keys []api.DeviceMessage) error {
	db := t.cm.Connection(ctx, true)
	for i, key := range keys {
		var keyJSONStr string
		var streamID int64
		var displayName sql.NullString
		row := db.Raw(t.selectDeviceKeysSQL, key.UserID, key.DeviceID).Row()
		err := row.Scan(&keyJSONStr, &streamID, &displayName)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
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

// SelectBatchDeviceKeys retrieves device keys for a batch of device IDs.
func (t *deviceKeysTable) SelectBatchDeviceKeys(ctx context.Context, userID string, deviceIDs []string, includeEmpty bool) ([]api.DeviceMessage, error) {
	db := t.cm.Connection(ctx, true)
	var rows *sql.Rows
	var err error
	if includeEmpty {
		rows, err = db.Raw(t.selectBatchDeviceKeysWithEmptiesSQL, userID).Rows()
	} else {
		rows, err = db.Raw(t.selectBatchDeviceKeysSQL, userID).Rows()
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deviceIDMap := make(map[string]bool)
	for _, d := range deviceIDs {
		deviceIDMap[d] = true
	}
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

// SelectMaxStreamIDForUser returns the max stream ID for a user.
func (t *deviceKeysTable) SelectMaxStreamIDForUser(ctx context.Context, userID string) (int64, error) {
	db := t.cm.Connection(ctx, true)
	var streamID sql.NullInt64
	row := db.Raw(t.selectMaxStreamForUserSQL, userID).Row()
	if err := row.Scan(&streamID); err != nil {
		return 0, err
	}
	if streamID.Valid {
		return streamID.Int64, nil
	}
	return 0, nil
}

// CountStreamIDsForUser returns the count of stream IDs for a user.
func (t *deviceKeysTable) CountStreamIDsForUser(ctx context.Context, userID string, streamIDs []int64) (int, error) {
	db := t.cm.Connection(ctx, true)
	var count sql.NullInt32
	row := db.Raw(t.countStreamIDsForUserSQL, userID, pq.Int64Array(streamIDs)).Row()
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	if count.Valid {
		return int(count.Int32), nil
	}
	return 0, nil
}

// DeleteDeviceKeys deletes device keys for a specific device.
func (t *deviceKeysTable) DeleteDeviceKeys(ctx context.Context, userID, deviceID string) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteDeviceKeysSQL, userID, deviceID)
	return result.Error
}

// DeleteAllDeviceKeys deletes all device keys for a user.
func (t *deviceKeysTable) DeleteAllDeviceKeys(ctx context.Context, userID string) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteAllDeviceKeysSQL, userID)
	return result.Error
}
