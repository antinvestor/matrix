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
	"encoding/json"
	"time"

	"github.com/lib/pq"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/pitabwire/frame"
)

// deviceKeysSchema defines the schema for device keys storage
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

// deviceKeysSchemaRevert defines the revert operation for the device keys schema
const deviceKeysSchemaRevert = `
DROP TABLE IF EXISTS keyserver_device_keys;
`

// SQL query constants
const upsertDeviceKeysSQL = `
INSERT INTO keyserver_device_keys (user_id, device_id, ts_added_secs, key_json, stream_id, display_name)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (user_id, device_id)
DO UPDATE SET key_json = $4, stream_id = $5, display_name = $6
`

const selectDeviceKeysSQL = `
SELECT device_id, key_json FROM keyserver_device_keys
WHERE user_id = $1 AND device_id = ANY($2)
`

const selectBatchDeviceKeysSQL = `
SELECT device_id, key_json, stream_id, display_name FROM keyserver_device_keys
WHERE user_id = $1 AND key_json <> ''
`

const selectBatchDeviceKeysWithEmptiesSQL = `
SELECT
    device_id, key_json, stream_id, display_name
    FROM keyserver_device_keys WHERE user_id = $1
`

const selectMaxStreamForUserSQL = `
SELECT MAX(stream_id) FROM keyserver_device_keys WHERE user_id = $1
`

const countStreamIDsForUserSQL = `
SELECT COUNT(*) FROM keyserver_device_keys WHERE user_id = $1 AND stream_id = ANY($2)
`

const deleteDeviceKeysSQL = `
DELETE FROM keyserver_device_keys WHERE user_id = $1 AND device_id = $2
`

const deleteAllDeviceKeysSQL = `
DELETE FROM keyserver_device_keys WHERE user_id = $1
`

type deviceKeysTable struct {
	cm                                  sqlutil.ConnectionManager
	upsertDeviceKeysSQL                 string
	selectDeviceKeysSQL                 string
	selectBatchDeviceKeysSQL            string
	selectBatchDeviceKeysWithEmptiesSQL string
	selectMaxStreamForUserSQL           string
	countStreamIDsForUserSQL            string
	deleteDeviceKeysSQL                 string
	deleteAllDeviceKeysSQL              string
}

// NewPostgresDeviceKeysTable creates a new postgres device keys table
func NewPostgresDeviceKeysTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.DeviceKeys, error) {
	t := &deviceKeysTable{
		cm:                                  cm,
		upsertDeviceKeysSQL:                 upsertDeviceKeysSQL,
		selectDeviceKeysSQL:                 selectDeviceKeysSQL,
		selectBatchDeviceKeysSQL:            selectBatchDeviceKeysSQL,
		selectBatchDeviceKeysWithEmptiesSQL: selectBatchDeviceKeysWithEmptiesSQL,
		selectMaxStreamForUserSQL:           selectMaxStreamForUserSQL,
		countStreamIDsForUserSQL:            countStreamIDsForUserSQL,
		deleteDeviceKeysSQL:                 deleteDeviceKeysSQL,
		deleteAllDeviceKeysSQL:              deleteAllDeviceKeysSQL,
	}

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "keyserver_device_keys_table_schema_001",
		Patch:       deviceKeysSchema,
		RevertPatch: deviceKeysSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// SelectDeviceKeysJSON populates the KeyJSON for the given keys. If any keys aren't found, they are simply not modified.
func (s *deviceKeysTable) SelectDeviceKeysJSON(ctx context.Context, keys []api.DeviceMessage) error {
	// If there are no keys then there is nothing to do
	if len(keys) == 0 {
		return nil
	}

	deviceIDs := make([]string, len(keys))
	for i := range keys {
		deviceIDs[i] = keys[i].DeviceID
	}

	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectDeviceKeysSQL, keys[0].UserID, pq.StringArray(deviceIDs)).Rows()
	if err != nil {
		return err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectDeviceKeysJSON: rows.close() failed")

	for rows.Next() {
		var deviceID string
		var keyJSON string
		if err = rows.Scan(&deviceID, &keyJSON); err != nil {
			return err
		}

		// Find which element this corresponds to in the input
		for i := range keys {
			if keys[i].DeviceID == deviceID {
				if err = json.Unmarshal([]byte(keyJSON), &keys[i].KeyJSON); err != nil {
					return err
				}
				break
			}
		}
	}

	return rows.Err()
}

// SelectMaxStreamIDForUser returns the maximum stream ID for the given user.
func (s *deviceKeysTable) SelectMaxStreamIDForUser(ctx context.Context, userID string) (streamID int64, err error) {
	db := s.cm.Connection(ctx, true)

	var nullableStreamId sql.NullInt64

	row := db.Raw(s.selectMaxStreamForUserSQL, userID).Row()
	err = row.Scan(&nullableStreamId)
	if err != nil {
		if sqlutil.ErrorIsNoRows(err) {
			return 0, nil
		}
		return 0, err
	}

	if nullableStreamId.Valid {
		streamID = nullableStreamId.Int64
	}
	return
}

// CountStreamIDsForUser counts the number of stream IDs that exist for a user which are in the supplied list.
func (s *deviceKeysTable) CountStreamIDsForUser(ctx context.Context, userID string, streamIDs []int64) (int, error) {
	db := s.cm.Connection(ctx, true)
	var count int
	err := db.Raw(s.countStreamIDsForUserSQL, userID, pq.Int64Array(streamIDs)).Row().Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// InsertDeviceKeys inserts the given keys. Keys with the same user ID and device ID will be replaced.
func (s *deviceKeysTable) InsertDeviceKeys(ctx context.Context, keys []api.DeviceMessage) error {
	if len(keys) == 0 {
		return nil
	}

	db := s.cm.Connection(ctx, false)

	for _, key := range keys {
		now := time.Now().Unix()
		err := db.Exec(
			s.upsertDeviceKeysSQL,
			key.UserID, key.DeviceID, now, string(key.KeyJSON), key.StreamID, key.DisplayName,
		).Error
		if err != nil {
			return err
		}
	}

	return nil
}

// DeleteDeviceKeys removes the keys for a given user and device. If the device doesn't exist, no error is returned.
func (s *deviceKeysTable) DeleteDeviceKeys(ctx context.Context, userID, deviceID string) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteDeviceKeysSQL, userID, deviceID).Error
}

// DeleteAllDeviceKeys removes all keys for a given user. If the user doesn't exist, no error is returned.
func (s *deviceKeysTable) DeleteAllDeviceKeys(ctx context.Context, userID string) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteAllDeviceKeysSQL, userID).Error
}

// SelectBatchDeviceKeys returns a map of device keys for the given user and device IDs.
func (s *deviceKeysTable) SelectBatchDeviceKeys(ctx context.Context, userID string, deviceIDs []string, includeEmpty bool) ([]api.DeviceMessage, error) {
	db := s.cm.Connection(ctx, true)
	var rows *sql.Rows
	var err error

	if includeEmpty {
		rows, err = db.Raw(s.selectBatchDeviceKeysWithEmptiesSQL, userID).Rows()
	} else {
		rows, err = db.Raw(s.selectBatchDeviceKeysSQL, userID).Rows()
	}

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectBatchDeviceKeys: rows.close() failed")
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
		err0 := rows.Scan(&dk.DeviceID, &dk.KeyJSON, &dk.StreamID, &displayName)
		if err0 != nil {
			return nil, err0
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
