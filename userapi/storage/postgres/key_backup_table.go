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

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/pitabwire/frame"
)

// keyBackupTableSchema defines the schema for the key backups table.
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

// keyBackupTableSchemaRevert defines how to revert the key backups table schema.
const keyBackupTableSchemaRevert = `
DROP TABLE IF EXISTS userapi_key_backups;
`

// insertBackupKeySQL is used to insert a new backup key into the database.
const insertBackupKeySQL = "" +
	"INSERT INTO userapi_key_backups(user_id, room_id, session_id, version, first_message_index, forwarded_count, is_verified, session_data) " +
	"VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

// updateBackupKeySQL is used to update an existing backup key in the database.
const updateBackupKeySQL = "" +
	"UPDATE userapi_key_backups SET first_message_index=$1, forwarded_count=$2, is_verified=$3, session_data=$4 " +
	"WHERE user_id=$5 AND room_id=$6 AND session_id=$7 AND version=$8"

// countKeysSQL is used to count the number of keys for a specific user and version.
const countKeysSQL = "" +
	"SELECT COUNT(*) FROM userapi_key_backups WHERE user_id = $1 AND version = $2"

// selectBackupKeysSQL is used to retrieve backup keys for a specific user and version.
const selectBackupKeysSQL = "" +
	"SELECT room_id, session_id, first_message_index, forwarded_count, is_verified, session_data FROM userapi_key_backups " +
	"WHERE user_id = $1 AND version = $2"

// selectKeysByRoomIDSQL is used to retrieve backup keys for a specific user, version and room.
const selectKeysByRoomIDSQL = "" +
	"SELECT room_id, session_id, first_message_index, forwarded_count, is_verified, session_data FROM userapi_key_backups " +
	"WHERE user_id = $1 AND version = $2 AND room_id = $3"

// selectKeysByRoomIDAndSessionIDSQL is used to retrieve backup keys for a specific user, version, room and session.
const selectKeysByRoomIDAndSessionIDSQL = "" +
	"SELECT room_id, session_id, first_message_index, forwarded_count, is_verified, session_data FROM userapi_key_backups " +
	"WHERE user_id = $1 AND version = $2 AND room_id = $3 AND session_id = $4"

type keyBackupTable struct {
	cm                                sqlutil.ConnectionManager
	insertBackupKeySQL                string
	updateBackupKeySQL                string
	countKeysSQL                      string
	selectBackupKeysSQL               string
	selectKeysByRoomIDSQL             string
	selectKeysByRoomIDAndSessionIDSQL string
}

// NewPostgresKeyBackupTable creates a new postgres key backup table.
func NewPostgresKeyBackupTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.KeyBackupTable, error) {
	t := &keyBackupTable{
		cm:                                cm,
		insertBackupKeySQL:                insertBackupKeySQL,
		updateBackupKeySQL:                updateBackupKeySQL,
		countKeysSQL:                      countKeysSQL,
		selectBackupKeysSQL:               selectBackupKeysSQL,
		selectKeysByRoomIDSQL:             selectKeysByRoomIDSQL,
		selectKeysByRoomIDAndSessionIDSQL: selectKeysByRoomIDAndSessionIDSQL,
	}

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "userapi_key_backups_table_schema_001",
		Patch:       keyBackupTableSchema,
		RevertPatch: keyBackupTableSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// CountKeys counts the number of keys for a specific user and version.
func (t *keyBackupTable) CountKeys(
	ctx context.Context, userID, version string,
) (count int64, err error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.countKeysSQL, userID, version).Row()
	err = row.Scan(&count)
	return
}

// InsertBackupKey inserts a new backup key into the database.
func (t *keyBackupTable) InsertBackupKey(
	ctx context.Context, userID, version string, key api.InternalKeyBackupSession,
) (err error) {
	db := t.cm.Connection(ctx, false)
	err = db.Exec(
		t.insertBackupKeySQL, userID, key.RoomID, key.SessionID, version, key.FirstMessageIndex, key.ForwardedCount, key.IsVerified, string(key.SessionData),
	).Error
	return
}

// UpdateBackupKey updates an existing backup key in the database.
func (t *keyBackupTable) UpdateBackupKey(
	ctx context.Context, userID, version string, key api.InternalKeyBackupSession,
) (err error) {
	db := t.cm.Connection(ctx, false)
	err = db.Exec(
		t.updateBackupKeySQL, key.FirstMessageIndex, key.ForwardedCount, key.IsVerified, string(key.SessionData), userID, key.RoomID, key.SessionID, version,
	).Error
	return
}

// SelectKeys retrieves backup keys for a specific user and version.
func (t *keyBackupTable) SelectKeys(
	ctx context.Context, userID, version string,
) (map[string]map[string]api.KeyBackupSession, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectBackupKeysSQL, userID, version).Rows()
	if err != nil {
		return nil, err
	}
	return unpackKeys(ctx, rows)
}

// SelectKeysByRoomID retrieves backup keys for a specific user, version and room.
func (t *keyBackupTable) SelectKeysByRoomID(
	ctx context.Context, userID, version, roomID string,
) (map[string]map[string]api.KeyBackupSession, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectKeysByRoomIDSQL, userID, version, roomID).Rows()
	if err != nil {
		return nil, err
	}
	return unpackKeys(ctx, rows)
}

// SelectKeysByRoomIDAndSessionID retrieves backup keys for a specific user, version, room and session.
func (t *keyBackupTable) SelectKeysByRoomIDAndSessionID(
	ctx context.Context, userID, version, roomID, sessionID string,
) (map[string]map[string]api.KeyBackupSession, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectKeysByRoomIDAndSessionIDSQL, userID, version, roomID, sessionID).Rows()
	if err != nil {
		return nil, err
	}
	return unpackKeys(ctx, rows)
}

// unpackKeys unpacks the rows returned by the database into a map of room IDs to session IDs to sessions.
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
