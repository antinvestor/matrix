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
	"encoding/json"
	"time"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
)

// oneTimeKeysSchema defines the schema for one-time keys table
const oneTimeKeysSchema = `
-- Stores one-time public keys for users
CREATE TABLE IF NOT EXISTS keyserver_one_time_keys (
    user_id TEXT NOT NULL,
	device_id TEXT NOT NULL,
	key_id TEXT NOT NULL,
	algorithm TEXT NOT NULL,
	ts_added_secs BIGINT NOT NULL,
	key_json TEXT NOT NULL,
	-- Clobber based on 4-uple of user/device/key/algorithm.
    CONSTRAINT keyserver_one_time_keys_unique UNIQUE (user_id, device_id, key_id, algorithm)
);

CREATE INDEX IF NOT EXISTS keyserver_one_time_keys_idx ON keyserver_one_time_keys (user_id, device_id);
`

// oneTimeKeysSchemaRevert defines the revert operation for the one-time keys schema
const oneTimeKeysSchemaRevert = `
DROP TABLE IF EXISTS keyserver_one_time_keys;
`

// SQL query constants
const upsertKeysSQL = `
INSERT INTO keyserver_one_time_keys (user_id, device_id, key_id, algorithm, ts_added_secs, key_json)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (user_id, device_id, key_id, algorithm)
DO UPDATE SET key_json = $6
`

const selectKeysSQL = `
SELECT key_id, algorithm, key_json FROM keyserver_one_time_keys
WHERE user_id = $1 AND device_id = $2 AND concat(algorithm, ':', key_id) = ANY($3)
`

const selectKeysCountSQL = `
SELECT algorithm, count(algorithm) FROM keyserver_one_time_keys
WHERE user_id = $1 AND device_id = $2
GROUP BY algorithm
`

const selectKeyByAlgorithmSQL = `
SELECT key_id, key_json FROM keyserver_one_time_keys
WHERE user_id = $1 AND device_id = $2 AND algorithm = $3
LIMIT 1
`

const deleteOneTimeKeySQL = `
DELETE FROM keyserver_one_time_keys
WHERE user_id = $1 AND device_id = $2 AND algorithm = $3 AND key_id = $4
RETURNING key_id, algorithm, key_json
`

const deleteOneTimeKeysSQL = `
DELETE FROM keyserver_one_time_keys
WHERE user_id = $1 AND device_id = $2
`

type oneTimeKeysTable struct {
	cm                      sqlutil.ConnectionManager
	upsertKeysSQL           string
	selectKeysSQL           string
	selectKeysCountSQL      string
	selectKeyByAlgorithmSQL string
	deleteOneTimeKeySQL     string
	deleteOneTimeKeysSQL    string
}

// NewPostgresOneTimeKeysTable creates a new one-time keys table
func NewPostgresOneTimeKeysTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.OneTimeKeys, error) {
	t := &oneTimeKeysTable{
		cm:                      cm,
		upsertKeysSQL:           upsertKeysSQL,
		selectKeysSQL:           selectKeysSQL,
		selectKeysCountSQL:      selectKeysCountSQL,
		selectKeyByAlgorithmSQL: selectKeyByAlgorithmSQL,
		deleteOneTimeKeySQL:     deleteOneTimeKeySQL,
		deleteOneTimeKeysSQL:    deleteOneTimeKeysSQL,
	}

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "keyserver_one_time_keys_table_schema_001",
		Patch:       oneTimeKeysSchema,
		RevertPatch: oneTimeKeysSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// SelectOneTimeKeys selects one-time keys for a given user and device.
func (s *oneTimeKeysTable) SelectOneTimeKeys(ctx context.Context, userID, deviceID string, keyIDsWithAlgorithms []string) (map[string]json.RawMessage, error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectKeysSQL, userID, deviceID, pq.Array(keyIDsWithAlgorithms)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectOneTimeKeys: rows.close() failed")

	result := make(map[string]json.RawMessage)
	for rows.Next() {
		var keyID, algorithm string
		var keyJSON string
		if err := rows.Scan(&keyID, &algorithm, &keyJSON); err != nil {
			return nil, err
		}

		// We need to combine the algorithm and key ID with a colon, so we use the form algorithm:key_id.
		result[algorithm+":"+keyID] = json.RawMessage(keyJSON)
	}

	return result, rows.Err()
}

// CountOneTimeKeys counts the number of one-time keys for a given user and device, grouped by algorithm.
func (s *oneTimeKeysTable) CountOneTimeKeys(ctx context.Context, userID, deviceID string) (*api.OneTimeKeysCount, error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectKeysCountSQL, userID, deviceID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "CountOneTimeKeys: rows.close() failed")

	result := &api.OneTimeKeysCount{
		DeviceID: deviceID,
		UserID:   userID,
		KeyCount: make(map[string]int),
	}

	for rows.Next() {
		var algorithm string
		var count int
		if err := rows.Scan(&algorithm, &count); err != nil {
			return nil, err
		}
		result.KeyCount[algorithm] = count
	}

	return result, rows.Err()
}

// InsertOneTimeKeys inserts one-time keys for a given user and device. Keys with the same key ID and algorithm will be replaced.
func (s *oneTimeKeysTable) InsertOneTimeKeys(ctx context.Context, keys api.OneTimeKeys) (*api.OneTimeKeysCount, error) {
	db := s.cm.Connection(ctx, false)

	// The txn passed in applies the INSERT and the SELECT COUNT, ensuring the count returned matches
	// the state of the database after the INSERT completes.
	now := time.Now().Unix()
	counts := &api.OneTimeKeysCount{
		DeviceID: keys.DeviceID,
		UserID:   keys.UserID,
		KeyCount: make(map[string]int),
	}
	for keyIDWithAlgo, keyJSON := range keys.KeyJSON {
		algo, keyID := keys.Split(keyIDWithAlgo)
		err := db.Exec(s.upsertKeysSQL, keys.UserID, keys.DeviceID, keyID, algo, now, string(keyJSON)).Error
		if err != nil {
			return nil, err
		}
	}
	rows, err := db.Raw(s.selectKeysCountSQL, keys.UserID, keys.DeviceID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectKeysCountStmt: rows.close() failed")
	for rows.Next() {
		var algorithm string
		var count int
		if err = rows.Scan(&algorithm, &count); err != nil {
			return nil, err
		}
		counts.KeyCount[algorithm] = count
	}

	return counts, rows.Err()
}

// SelectAndDeleteOneTimeKey selects a single one-time key for a given user, device and algorithm, and deletes it.
func (s *oneTimeKeysTable) SelectAndDeleteOneTimeKey(
	ctx context.Context, userID, deviceID, algorithm string,
) (map[string]json.RawMessage, error) {
	var keyID string
	var keyJSON string

	db := s.cm.Connection(ctx, false)
	row := db.Raw(s.selectKeyByAlgorithmSQL, userID, deviceID, algorithm).Row()
	err := row.Scan(&keyID, &keyJSON)
	if err != nil {
		if sqlutil.ErrorIsNoRows(err) {
			return nil, nil
		}
		return nil, err
	}
	err = db.Exec(s.deleteOneTimeKeySQL, userID, deviceID, algorithm, keyID).Error
	return map[string]json.RawMessage{
		algorithm + ":" + keyID: json.RawMessage(keyJSON),
	}, err
}

// DeleteOneTimeKeys deletes all one-time keys for a given user and device.
func (s *oneTimeKeysTable) DeleteOneTimeKeys(ctx context.Context, userID, deviceID string) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteOneTimeKeysSQL, userID, deviceID).Error
}
