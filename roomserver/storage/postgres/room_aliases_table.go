// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Global.org Foundation C.I.C.
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

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
)

const roomAliasesSchema = `
-- Stores room aliases and room IDs they refer to
CREATE TABLE IF NOT EXISTS roomserver_room_aliases (
    -- Alias of the room
    alias TEXT NOT NULL PRIMARY KEY,
    -- Room ID the alias refers to
    room_id TEXT NOT NULL,
    -- User ID of the creator of this alias
    creator_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS roomserver_room_id_idx ON roomserver_room_aliases(room_id);
`

const roomAliasesSchemaRevert = `DROP TABLE IF EXISTS roomserver_room_aliases;`

// Insert a new room alias.
const insertRoomAliasSQL = "" +
	"INSERT INTO roomserver_room_aliases (alias, room_id, creator_id) VALUES ($1, $2, $3)"

// Lookup room ID from alias.
const selectRoomIDFromAliasSQL = "" +
	"SELECT room_id FROM roomserver_room_aliases WHERE alias = $1"

// Lookup all aliases for a given room ID.
const selectAliasesFromRoomIDSQL = "" +
	"SELECT alias FROM roomserver_room_aliases WHERE room_id = $1"

// Lookup creator ID from alias.
const selectCreatorIDFromAliasSQL = "" +
	"SELECT creator_id FROM roomserver_room_aliases WHERE alias = $1"

// Delete a room alias.
const deleteRoomAliasSQL = "" +
	"DELETE FROM roomserver_room_aliases WHERE alias = $1"

type roomAliasesTable struct {
	cm                          *sqlutil.Connections
	insertRoomAliasSQL          string
	selectRoomIDFromAliasSQL    string
	selectAliasesFromRoomIDSQL  string
	selectCreatorIDFromAliasSQL string
	deleteRoomAliasSQL          string
}

func NewPostgresRoomAliasesTable(cm *sqlutil.Connections) tables.RoomAliases {
	return &roomAliasesTable{
		cm:                          cm,
		insertRoomAliasSQL:          insertRoomAliasSQL,
		selectRoomIDFromAliasSQL:    selectRoomIDFromAliasSQL,
		selectAliasesFromRoomIDSQL:  selectAliasesFromRoomIDSQL,
		selectCreatorIDFromAliasSQL: selectCreatorIDFromAliasSQL,
		deleteRoomAliasSQL:          deleteRoomAliasSQL,
	}
}

func (t *roomAliasesTable) InsertRoomAlias(ctx context.Context, alias string, roomID string, creatorUserID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.insertRoomAliasSQL, alias, roomID, creatorUserID).Error
}

func (t *roomAliasesTable) SelectRoomIDFromAlias(ctx context.Context, alias string) (string, error) {
	var roomID string
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectRoomIDFromAliasSQL, alias).Row()
	err := row.Scan(&roomID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return roomID, err
}

func (t *roomAliasesTable) SelectAliasesFromRoomID(ctx context.Context, roomID string) ([]string, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectAliasesFromRoomIDSQL, roomID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var aliases []string
	for rows.Next() {
		var alias string
		if err := rows.Scan(&alias); err != nil {
			return nil, err
		}
		aliases = append(aliases, alias)
	}
	return aliases, rows.Err()
}

func (t *roomAliasesTable) SelectCreatorIDFromAlias(ctx context.Context, alias string) (string, error) {
	var creatorID string
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectCreatorIDFromAliasSQL, alias).Row()
	err := row.Scan(&creatorID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return creatorID, err
}

func (t *roomAliasesTable) DeleteRoomAlias(ctx context.Context, alias string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteRoomAliasSQL, alias).Error
}
