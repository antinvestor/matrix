// Copyright 2021 The Matrix.org Foundation C.I.C.
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

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

const keyBackupTableSchema = `
CREATE TABLE IF NOT EXISTS userapi_key_backups (
    user_id TEXT NOT NULL,
    room_id TEXT NOT NULL,
    session_id TEXT NOT NULL,

    version TEXT NOT NULL,
    first_message_index INTEGER NOT NULL,
    forwarded_count INTEGER NOT NULL,
    is_verified BOOLEAN NOT NULL,
    session_data TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS userapi_key_backups_idx ON userapi_key_backups(user_id, room_id, session_id, version);
CREATE INDEX IF NOT EXISTS userapi_key_backups_versions_idx ON userapi_key_backups(user_id, version);
`

// SQL query constants for key backup operations
const (
	// insertBackupKeySQL inserts a new backup key
	insertBackupKeySQL = "INSERT INTO userapi_key_backups(user_id, room_id, session_id, version, first_message_index, forwarded_count, is_verified, session_data) " +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

	// updateBackupKeySQL updates an existing backup key
	updateBackupKeySQL = "UPDATE userapi_key_backups SET first_message_index=$1, forwarded_count=$2, is_verified=$3, session_data=$4 " +
		"WHERE user_id=$5 AND room_id=$6 AND session_id=$7 AND version=$8"

	// countKeysSQL counts the number of keys for a user and version
	countKeysSQL = "SELECT COUNT(*) FROM userapi_key_backups WHERE user_id = $1 AND version = $2"

	// selectBackupKeysSQL retrieves all keys for a user and version
	selectBackupKeysSQL = "SELECT room_id, session_id, first_message_index, forwarded_count, is_verified, session_data FROM userapi_key_backups " +
		"WHERE user_id = $1 AND version = $2"

	// selectKeysByRoomIDSQL retrieves keys for a user, version, and room ID
	selectKeysByRoomIDSQL = "SELECT room_id, session_id, first_message_index, forwarded_count, is_verified, session_data FROM userapi_key_backups " +
		"WHERE user_id = $1 AND version = $2 AND room_id = $3"

	// selectKeysByRoomIDAndSessionIDSQL retrieves keys for a user, version, room ID, and session ID
	selectKeysByRoomIDAndSessionIDSQL = "SELECT room_id, session_id, first_message_index, forwarded_count, is_verified, session_data FROM userapi_key_backups " +
		"WHERE user_id = $1 AND version = $2 AND room_id = $3 AND session_id = $4"
)

type keyBackupTable struct {
	cm *sqlutil.Connections

	// SQL queries stored as fields for better maintainability
	insertBackupKeyStmt                string
	updateBackupKeyStmt                string
	countKeysStmt                      string
	selectBackupKeysStmt               string
	selectKeysByRoomIDStmt             string
	selectKeysByRoomIDAndSessionIDStmt string
}

func NewPostgresKeyBackupTable(ctx context.Context, cm *sqlutil.Connections) (tables.KeyBackupTable, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(keyBackupTableSchema).Error; err != nil {
		return nil, err
	}

	// Initialize table with SQL statements
	t := &keyBackupTable{
		cm:                                 cm,
		insertBackupKeyStmt:                insertBackupKeySQL,
		updateBackupKeyStmt:                updateBackupKeySQL,
		countKeysStmt:                      countKeysSQL,
		selectBackupKeysStmt:               selectBackupKeysSQL,
		selectKeysByRoomIDStmt:             selectKeysByRoomIDSQL,
		selectKeysByRoomIDAndSessionIDStmt: selectKeysByRoomIDAndSessionIDSQL,
	}

	return t, nil
}

func (t *keyBackupTable) CountKeys(
	ctx context.Context, userID, version string,
) (count int64, err error) {
	db := t.cm.Connection(ctx, true)

	row := db.Raw(t.countKeysStmt, userID, version).Row()
	err = row.Scan(&count)
	return
}

func (t *keyBackupTable) InsertBackupKey(
	ctx context.Context, userID, version string, key api.InternalKeyBackupSession,
) (err error) {
	db := t.cm.Connection(ctx, false)

	return db.Exec(
		t.insertBackupKeyStmt,
		userID, key.RoomID, key.SessionID, version, key.FirstMessageIndex, key.ForwardedCount, key.IsVerified, string(key.SessionData),
	).Error
}

func (t *keyBackupTable) UpdateBackupKey(
	ctx context.Context, userID, version string, key api.InternalKeyBackupSession,
) (err error) {
	db := t.cm.Connection(ctx, false)

	return db.Exec(
		t.updateBackupKeyStmt,
		key.FirstMessageIndex, key.ForwardedCount, key.IsVerified, string(key.SessionData), userID, key.RoomID, key.SessionID, version,
	).Error
}

func (t *keyBackupTable) SelectKeys(
	ctx context.Context, userID, version string,
) (map[string]map[string]api.KeyBackupSession, error) {
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(t.selectBackupKeysStmt, userID, version).Rows()
	if err != nil {
		return nil, err
	}
	return unpackKeys(rows)
}

func (t *keyBackupTable) SelectKeysByRoomID(
	ctx context.Context, userID, version, roomID string,
) (map[string]map[string]api.KeyBackupSession, error) {
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(t.selectKeysByRoomIDStmt, userID, version, roomID).Rows()
	if err != nil {
		return nil, err
	}
	return unpackKeys(rows)
}

func (t *keyBackupTable) SelectKeysByRoomIDAndSessionID(
	ctx context.Context, userID, version, roomID, sessionID string,
) (map[string]map[string]api.KeyBackupSession, error) {
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(t.selectKeysByRoomIDAndSessionIDStmt, userID, version, roomID, sessionID).Rows()
	if err != nil {
		return nil, err
	}
	return unpackKeys(rows)
}

func unpackKeys(rows *sql.Rows) (map[string]map[string]api.KeyBackupSession, error) {
	result := make(map[string]map[string]api.KeyBackupSession)
	defer rows.Close()

	for rows.Next() {
		var (
			roomID            string
			sessionID         string
			firstMessageIndex int
			forwardedCount    int
			isVerified        bool
			sessionData       string
		)
		if err := rows.Scan(&roomID, &sessionID, &firstMessageIndex, &forwardedCount, &isVerified, &sessionData); err != nil {
			return nil, err
		}

		if _, ok := result[roomID]; !ok {
			result[roomID] = make(map[string]api.KeyBackupSession)
		}

		result[roomID][sessionID] = api.KeyBackupSession{
			FirstMessageIndex: firstMessageIndex,
			ForwardedCount:    forwardedCount,
			IsVerified:        isVerified,
			SessionData:       json.RawMessage(sessionData),
		}
	}

	return result, rows.Err()
}
