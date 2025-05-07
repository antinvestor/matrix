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
	"encoding/json"
	"errors"
	"time"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/lib/pq"
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

// SQL query constants for one-time keys operations
const (
	// upsertKeysSQL inserts or updates a one-time key
	upsertKeysSQL = "INSERT INTO keyserver_one_time_keys (user_id, device_id, key_id, algorithm, ts_added_secs, key_json)" +
		" VALUES ($1, $2, $3, $4, $5, $6)" +
		" ON CONFLICT ON CONSTRAINT keyserver_one_time_keys_unique" +
		" DO UPDATE SET key_json = $6"

	// selectOneTimeKeysSQL retrieves one-time keys by user, device, and algorithm+keyID combinations
	selectOneTimeKeysSQL = "SELECT concat(algorithm, ':', key_id) as algorithmwithid, key_json FROM keyserver_one_time_keys WHERE user_id=$1 AND device_id=$2 AND concat(algorithm, ':', key_id) = ANY($3);"

	// selectKeysCountSQL counts how many keys exist for a user/device by algorithm
	selectKeysCountSQL = "SELECT algorithm, COUNT(key_id) FROM " +
		" (SELECT algorithm, key_id FROM keyserver_one_time_keys WHERE user_id = $1 AND device_id = $2 LIMIT 100)" +
		" x GROUP BY algorithm"

	// deleteOneTimeKeySQL removes a single key by user, device, algorithm, and keyID
	deleteOneTimeKeySQL = "DELETE FROM keyserver_one_time_keys WHERE user_id = $1 AND device_id = $2 AND algorithm = $3 AND key_id = $4"

	// selectKeyByAlgorithmSQL retrieves a single key for a user/device by algorithm
	selectKeyByAlgorithmSQL = "SELECT key_id, key_json FROM keyserver_one_time_keys WHERE user_id = $1 AND device_id = $2 AND algorithm = $3 LIMIT 1"

	// deleteOneTimeKeysSQL removes all one-time keys for a user/device
	deleteOneTimeKeysSQL = "DELETE FROM keyserver_one_time_keys WHERE user_id = $1 AND device_id = $2"
)

type oneTimeKeysTable struct {
	cm *sqlutil.Connections

	upsertKeysStmt           string
	selectOneTimeKeysStmt    string
	selectKeysCountStmt      string
	selectKeyByAlgorithmStmt string
	deleteOneTimeKeyStmt     string
	deleteOneTimeKeysStmt    string
}

func NewPostgresOneTimeKeysTable(ctx context.Context, cm *sqlutil.Connections) (tables.OneTimeKeys, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(oneTimeKeysSchema).Error; err != nil {
		return nil, err
	}

	// Initialize table with SQL statements
	t := &oneTimeKeysTable{
		cm:                       cm,
		upsertKeysStmt:           upsertKeysSQL,
		selectOneTimeKeysStmt:    selectOneTimeKeysSQL,
		selectKeysCountStmt:      selectKeysCountSQL,
		selectKeyByAlgorithmStmt: selectKeyByAlgorithmSQL,
		deleteOneTimeKeyStmt:     deleteOneTimeKeySQL,
		deleteOneTimeKeysStmt:    deleteOneTimeKeysSQL,
	}

	return t, nil
}

func (t *oneTimeKeysTable) SelectOneTimeKeys(ctx context.Context, userID, deviceID string, keyIDsWithAlgorithms []string) (map[string]json.RawMessage, error) {
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(t.selectOneTimeKeysStmt, userID, deviceID, pq.Array(keyIDsWithAlgorithms)).Rows()
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

func (t *oneTimeKeysTable) CountOneTimeKeys(ctx context.Context, userID, deviceID string) (*api.OneTimeKeysCount, error) {
	db := t.cm.Connection(ctx, true)

	counts := &api.OneTimeKeysCount{
		DeviceID: deviceID,
		UserID:   userID,
		KeyCount: make(map[string]int),
	}

	rows, err := db.Raw(t.selectKeysCountStmt, userID, deviceID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
		if err := db.Exec(
			t.upsertKeysStmt, keys.UserID, keys.DeviceID, keyID, algo, now, string(keyJSON),
		).Error; err != nil {
			return nil, err
		}
	}

	rows, err := db.Raw(t.selectKeysCountStmt, keys.UserID, keys.DeviceID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

func (t *oneTimeKeysTable) SelectAndDeleteOneTimeKey(
	ctx context.Context, userID, deviceID, algorithm string,
) (map[string]json.RawMessage, error) {
	db := t.cm.Connection(ctx, false)

	var keyID string
	var keyJSON string

	row := db.Raw(t.selectKeyByAlgorithmStmt, userID, deviceID, algorithm).Row()
	err := row.Scan(&keyID, &keyJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	if err = db.Exec(t.deleteOneTimeKeyStmt, userID, deviceID, algorithm, keyID).Error; err != nil {
		return nil, err
	}

	return map[string]json.RawMessage{
		algorithm + ":" + keyID: json.RawMessage(keyJSON),
	}, nil
}

func (t *oneTimeKeysTable) DeleteOneTimeKeys(ctx context.Context, userID, deviceID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteOneTimeKeysStmt, userID, deviceID).Error
}
