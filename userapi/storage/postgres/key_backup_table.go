// Copyright 2021 The Global.org Foundation C.I.C.
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

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

// SQL: Create key backup table
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

// SQL: Drop key backup table
const keyBackupTableSchemaRevert = "DROP TABLE IF EXISTS userapi_key_backups CASCADE;"

// SQL: Insert key backup
const insertBackupKeySQL = "" +
	"INSERT INTO userapi_key_backups(user_id, room_id, session_id, version, first_message_index, forwarded_count, is_verified, session_data) " +
	"VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

// SQL: Update key backup
const updateBackupKeySQL = "" +
	"UPDATE userapi_key_backups SET first_message_index=$1, forwarded_count=$2, is_verified=$3, session_data=$4 " +
	"WHERE user_id=$5 AND room_id=$6 AND session_id=$7 AND version=$8"

// SQL: Count keys
const countKeysSQL = "" +
	"SELECT COUNT(*) FROM userapi_key_backups WHERE user_id = $1 AND version = $2"

// SQL: Select keys
const selectBackupKeysSQL = "" +
	"SELECT room_id, session_id, first_message_index, forwarded_count, is_verified, session_data FROM userapi_key_backups " +
	"WHERE user_id = $1 AND version = $2"

// SQL: Select keys by room ID
const selectKeysByRoomIDSQL = "" +
	"SELECT room_id, session_id, first_message_index, forwarded_count, is_verified, session_data FROM userapi_key_backups " +
	"WHERE user_id = $1 AND version = $2 AND room_id = $3"

// SQL: Select keys by room ID and session ID
const selectKeysByRoomIDAndSessionIDSQL = "" +
	"SELECT room_id, session_id, first_message_index, forwarded_count, is_verified, session_data FROM userapi_key_backups " +
	"WHERE user_id = $1 AND version = $2 AND room_id = $3 AND session_id = $4"

// keyBackupTable implements tables.KeyBackupTable using GORM and a connection manager.
type keyBackupTable struct {
	cm *sqlutil.Connections

	// SQL queries assigned from constants at the top; see comments above each constant for details.
	insertBackupKeySQL                string // Insert key backup
	updateBackupKeySQL                string // Update key backup
	countKeysSQL                      string // Count keys
	selectBackupKeysSQL               string // Select keys
	selectKeysByRoomIDSQL             string // Select keys by room ID
	selectKeysByRoomIDAndSessionIDSQL string // Select keys by room ID and session ID
}

// NewPostgresKeyBackupTable returns a new KeyBackupTable using the provided connection manager.
func NewPostgresKeyBackupTable(cm *sqlutil.Connections) tables.KeyBackupTable {
	return &keyBackupTable{
		cm:                                cm,
		insertBackupKeySQL:                insertBackupKeySQL,
		updateBackupKeySQL:                updateBackupKeySQL,
		countKeysSQL:                      countKeysSQL,
		selectBackupKeysSQL:               selectBackupKeysSQL,
		selectKeysByRoomIDSQL:             selectKeysByRoomIDSQL,
		selectKeysByRoomIDAndSessionIDSQL: selectKeysByRoomIDAndSessionIDSQL,
	}
}

func (t *keyBackupTable) CountKeys(ctx context.Context, userID, version string) (count int64, err error) {
	db := t.cm.Connection(ctx, true)
	err = db.Raw(t.countKeysSQL, userID, version).Count(&count).Error
	return
}

func (t *keyBackupTable) InsertBackupKey(ctx context.Context, userID, version string, key api.InternalKeyBackupSession) (err error) {
	db := t.cm.Connection(ctx, false)
	err = db.Exec(t.insertBackupKeySQL, userID, key.RoomID, key.SessionID, version, key.FirstMessageIndex, key.ForwardedCount, key.IsVerified, string(key.SessionData)).Error
	return
}

func (t *keyBackupTable) UpdateBackupKey(ctx context.Context, userID, version string, key api.InternalKeyBackupSession) (err error) {
	db := t.cm.Connection(ctx, false)
	err = db.Exec(t.updateBackupKeySQL, key.FirstMessageIndex, key.ForwardedCount, key.IsVerified, string(key.SessionData), userID, key.RoomID, key.SessionID, version).Error
	return
}

func (t *keyBackupTable) SelectKeys(ctx context.Context, userID, version string) (map[string]map[string]api.KeyBackupSession, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectBackupKeysSQL, userID, version).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return unpackKeys(ctx, rows)
}

func (t *keyBackupTable) SelectKeysByRoomID(ctx context.Context, userID, version, roomID string) (map[string]map[string]api.KeyBackupSession, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectKeysByRoomIDSQL, userID, version, roomID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return unpackKeys(ctx, rows)
}

func (t *keyBackupTable) SelectKeysByRoomIDAndSessionID(ctx context.Context, userID, version, roomID, sessionID string) (map[string]map[string]api.KeyBackupSession, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectKeysByRoomIDAndSessionIDSQL, userID, version, roomID, sessionID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return unpackKeys(ctx, rows)
}

func unpackKeys(ctx context.Context, rows *sql.Rows) (map[string]map[string]api.KeyBackupSession, error) {
	result := make(map[string]map[string]api.KeyBackupSession)
	defer internal.CloseAndLogIfError(ctx, rows, "selectKeysStmt.Close failed")
	for rows.Next() {
		var key api.InternalKeyBackupSession
		// room_id, session_id, first_message_index, forwarded_count, is_verified, session_data
		var sessionDataStr string
		if err := rows.Scan(&key.RoomID, &key.SessionID, &key.FirstMessageIndex, &key.ForwardedCount, &key.IsVerified, &sessionDataStr); err != nil {
			return nil, err
		}
		key.SessionData = json.RawMessage(sessionDataStr)
		roomData := result[key.RoomID]
		if roomData == nil {
			roomData = make(map[string]api.KeyBackupSession)
		}
		roomData[key.SessionID] = key.KeyBackupSession
		result[key.RoomID] = roomData
	}
	return result, rows.Err()
}
