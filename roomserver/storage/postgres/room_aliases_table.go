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

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/pitabwire/frame"
)

// Schema for the room aliases table
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

// Schema revert script for migration purposes
const roomAliasesSchemaRevert = `DROP TABLE IF EXISTS roomserver_room_aliases;`

// SQL to insert a room alias
const insertRoomAliasSQL = "" +
	"INSERT INTO roomserver_room_aliases (alias, room_id, creator_id) VALUES ($1, $2, $3)"

// SQL to select a room ID from an alias
const selectRoomIDFromAliasSQL = "" +
	"SELECT room_id FROM roomserver_room_aliases WHERE alias = $1"

// SQL to select all aliases for a room ID
const selectAliasesFromRoomIDSQL = "" +
	"SELECT alias FROM roomserver_room_aliases WHERE room_id = $1"

// SQL to select the creator ID of an alias
const selectCreatorIDFromAliasSQL = "" +
	"SELECT creator_id FROM roomserver_room_aliases WHERE alias = $1"

// SQL to delete a room alias
const deleteRoomAliasSQL = "" +
	"DELETE FROM roomserver_room_aliases WHERE alias = $1"

// roomAliasesTable implements the tables.RoomAliases interface using GORM
type roomAliasesTable struct {
	cm sqlutil.ConnectionManager

	// SQL query strings loaded from constants
	insertRoomAliasSQL          string
	selectRoomIDFromAliasSQL    string
	selectAliasesFromRoomIDSQL  string
	selectCreatorIDFromAliasSQL string
	deleteRoomAliasSQL          string
}

// NewPostgresRoomAliasesTable creates a new room aliases table
func NewPostgresRoomAliasesTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.RoomAliases, error) {
	// Create the table if it doesn't exist using migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "roomserver_room_aliases_table_schema_001",
		Patch:       roomAliasesSchema,
		RevertPatch: roomAliasesSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Initialise the table struct with the connection manager
	t := &roomAliasesTable{
		cm: cm,

		// Initialise SQL query strings from constants
		insertRoomAliasSQL:          insertRoomAliasSQL,
		selectRoomIDFromAliasSQL:    selectRoomIDFromAliasSQL,
		selectAliasesFromRoomIDSQL:  selectAliasesFromRoomIDSQL,
		selectCreatorIDFromAliasSQL: selectCreatorIDFromAliasSQL,
		deleteRoomAliasSQL:          deleteRoomAliasSQL,
	}

	return t, nil
}

func (t *roomAliasesTable) InsertRoomAlias(
	ctx context.Context, alias string, roomID string, creatorUserID string,
) (err error) {
	db := t.cm.Connection(ctx, false)

	result := db.Exec(t.insertRoomAliasSQL, alias, roomID, creatorUserID)
	return result.Error
}

func (t *roomAliasesTable) SelectRoomIDFromAlias(
	ctx context.Context, alias string,
) (roomID string, err error) {
	db := t.cm.Connection(ctx, true)

	row := db.Raw(t.selectRoomIDFromAliasSQL, alias).Row()
	err = row.Scan(&roomID)
	if sqlutil.ErrorIsNoRows(err) {
		return "", nil
	}
	return
}

func (t *roomAliasesTable) SelectAliasesFromRoomID(
	ctx context.Context, roomID string,
) ([]string, error) {
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(t.selectAliasesFromRoomIDSQL, roomID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectAliasesFromRoomID: rows.close() failed")

	var aliases []string
	var alias string
	for rows.Next() {
		if err = rows.Scan(&alias); err != nil {
			return nil, err
		}

		aliases = append(aliases, alias)
	}
	return aliases, rows.Err()
}

func (t *roomAliasesTable) SelectCreatorIDFromAlias(
	ctx context.Context, alias string,
) (creatorID string, err error) {
	db := t.cm.Connection(ctx, true)

	row := db.Raw(t.selectCreatorIDFromAliasSQL, alias).Row()
	err = row.Scan(&creatorID)
	if sqlutil.ErrorIsNoRows(err) {
		return "", nil
	}
	return
}

func (t *roomAliasesTable) DeleteRoomAlias(
	ctx context.Context, alias string,
) (err error) {
	db := t.cm.Connection(ctx, false)

	result := db.Exec(t.deleteRoomAliasSQL, alias)
	return result.Error
}
