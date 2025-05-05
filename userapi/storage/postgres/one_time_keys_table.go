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
	"encoding/json"
	"errors"
	"time"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

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

const oneTimeKeysSchemaRevert = "DROP TABLE IF EXISTS keyserver_one_time_keys CASCADE; DROP INDEX IF EXISTS keyserver_one_time_keys_idx;"

// SQL: Upsert one-time keys
const upsertKeysSQL = "INSERT INTO keyserver_one_time_keys (user_id, device_id, key_id, algorithm, ts_added_secs, key_json) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (user_id, device_id, key_id, algorithm) DO UPDATE SET ts_added_secs = $5, key_json = $6"

// SQL: Select one-time keys
const selectKeysSQL = "SELECT concat(algorithm, ':', key_id) as algorithmwithid, key_json FROM keyserver_one_time_keys WHERE user_id=$1 AND device_id=$2 AND concat(algorithm, ':', key_id) = ANY($3);"

// SQL: Select keys count
const selectKeysCountSQL = "SELECT algorithm, COUNT(key_id) FROM (SELECT algorithm, key_id FROM keyserver_one_time_keys WHERE user_id = $1 AND device_id = $2 LIMIT 100) x GROUP BY algorithm"

// SQL: Select key by algorithm
const selectKeyByAlgorithmSQL = "SELECT key_id, key_json FROM keyserver_one_time_keys WHERE user_id = $1 AND device_id = $2 AND algorithm = $3 LIMIT 1"

// SQL: Delete one-time key
const deleteOneTimeKeySQL = "DELETE FROM keyserver_one_time_keys WHERE user_id = $1 AND device_id = $2 AND algorithm = $3 AND key_id = $4"

// SQL: Delete all one-time keys
const deleteOneTimeKeysSQL = "DELETE FROM keyserver_one_time_keys WHERE user_id = $1 AND device_id = $2"

// oneTimeKeysTable implements tables.OneTimeKeysTable using GORM and a connection manager.
type oneTimeKeysTable struct {
	cm *sqlutil.Connections

	upsertKeysSQL           string
	selectKeysSQL           string
	selectKeysCountSQL      string
	selectKeyByAlgorithmSQL string
	deleteOneTimeKeySQL     string
	deleteOneTimeKeysSQL    string
}

// NewPostgresOneTimeKeysTable returns a new OneTimeKeysTable using the provided connection manager.
func NewPostgresOneTimeKeysTable(cm *sqlutil.Connections) tables.OneTimeKeys {
	return &oneTimeKeysTable{
		cm:                      cm,
		upsertKeysSQL:           upsertKeysSQL,
		selectKeysSQL:           selectKeysSQL,
		selectKeysCountSQL:      selectKeysCountSQL,
		selectKeyByAlgorithmSQL: selectKeyByAlgorithmSQL,
		deleteOneTimeKeySQL:     deleteOneTimeKeySQL,
		deleteOneTimeKeysSQL:    deleteOneTimeKeysSQL,
	}
}

// InsertOneTimeKeys inserts one-time keys for a device.
func (t *oneTimeKeysTable) InsertOneTimeKeys(ctx context.Context, keys api.OneTimeKeys) (*api.OneTimeKeysCount, error) {
	db := t.cm.Connection(ctx, false)
	now := time.Now().Unix()
	counts := &api.OneTimeKeysCount{
		DeviceID: keys.DeviceID,
		UserID:   keys.UserID,
		KeyCount: make(map[string]int),
	}
	for keyIDWithAlgo, keyJSON := range keys.KeyJSON {
		algo, keyID := keys.Split(keyIDWithAlgo)
		result := db.Exec(t.upsertKeysSQL, keys.UserID, keys.DeviceID, keyID, algo, now, string(keyJSON))
		if result.Error != nil {
			return nil, result.Error
		}
	}
	rows, err := db.Raw(t.selectKeysCountSQL, keys.UserID, keys.DeviceID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var algorithm string
		var count int
		if err := rows.Scan(&algorithm, &count); err != nil {
			return nil, err
		}
		counts.KeyCount[algorithm] = count
	}
	return counts, rows.Err()
}

// SelectOneTimeKeys selects one-time keys for a device.
func (t *oneTimeKeysTable) SelectOneTimeKeys(ctx context.Context, userID, deviceID string, keyIDsWithAlgorithms []string) (map[string]json.RawMessage, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectKeysSQL, userID, deviceID, pq.Array(keyIDsWithAlgorithms)).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]json.RawMessage)
	var (
		algorithmWithID string
		keyJSONStr      string
	)
	for rows.Next() {
		if err := rows.Scan(&algorithmWithID, &keyJSONStr); err != nil {
			return nil, err
		}
		result[algorithmWithID] = json.RawMessage(keyJSONStr)
	}
	return result, rows.Err()
}

// SelectAndDeleteOneTimeKey selects and deletes a one-time key for a device and algorithm.
func (t *oneTimeKeysTable) SelectAndDeleteOneTimeKey(ctx context.Context, userID, deviceID, algorithm string) (map[string]json.RawMessage, error) {
	db := t.cm.Connection(ctx, false)
	row := db.Raw(t.selectKeyByAlgorithmSQL, userID, deviceID, algorithm).Row()
	var keyID, keyJSON string
	if err := row.Scan(&keyID, &keyJSON); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	result := db.Exec(t.deleteOneTimeKeySQL, userID, deviceID, algorithm, keyID)
	if result.Error != nil {
		return nil, result.Error
	}
	return map[string]json.RawMessage{
		algorithm + ":" + keyID: json.RawMessage(keyJSON),
	}, nil
}

// DeleteOneTimeKeys deletes all one-time keys for a device.
func (t *oneTimeKeysTable) DeleteOneTimeKeys(ctx context.Context, userID, deviceID string) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteOneTimeKeysSQL, userID, deviceID)
	return result.Error
}

// CountOneTimeKeys counts one-time keys for a device.
func (t *oneTimeKeysTable) CountOneTimeKeys(ctx context.Context, userID, deviceID string) (*api.OneTimeKeysCount, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectKeysCountSQL, userID, deviceID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := &api.OneTimeKeysCount{
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
		counts.KeyCount[algorithm] = count
	}
	return counts, rows.Err()
}
