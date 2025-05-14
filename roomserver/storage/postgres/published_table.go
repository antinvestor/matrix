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
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/pitabwire/frame"
)

// Schema for the published table
const publishedSchema = `
-- Stores which rooms are published in the room directory
CREATE TABLE IF NOT EXISTS roomserver_published (
    -- The room ID of the room
    room_id TEXT NOT NULL,
    -- The appservice ID of the room
    appservice_id TEXT NOT NULL,
    -- The network_id of the room
    network_id TEXT NOT NULL,
    -- Whether it is published or not
    published BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (room_id, appservice_id, network_id)
);
`

// Schema revert script for migration purposes
const publishedSchemaRevert = `DROP TABLE IF EXISTS roomserver_published;`

// SQL to insert or update a published room record
const upsertPublishedSQL = "" +
	"INSERT INTO roomserver_published (room_id, appservice_id, network_id, published) VALUES ($1, $2, $3, $4) " +
	"ON CONFLICT (room_id, appservice_id, network_id) DO UPDATE SET published=$4"

// SQL to select all published rooms
var selectAllPublishedSQL = "" +
	"SELECT room_id FROM roomserver_published WHERE published = $1 AND CASE WHEN $2 THEN 1=1 ELSE network_id = '' END ORDER BY room_id ASC"

// SQL to select all published rooms for a specific network
const selectNetworkPublishedSQL = "" +
	"SELECT room_id FROM roomserver_published WHERE published = $1 AND network_id = $2 ORDER BY room_id ASC"

// SQL to check if a room is published
const selectPublishedSQL = "" +
	"SELECT published FROM roomserver_published WHERE room_id = $1"

// publishedTable implements the tables.Published interface using GORM
type publishedTable struct {
	cm sqlutil.ConnectionManager

	// SQL query strings loaded from constants
	upsertPublishedSQL        string
	selectAllPublishedSQL     string
	selectPublishedSQL        string
	selectNetworkPublishedSQL string
}

// NewPostgresPublishedTable creates a new published table
func NewPostgresPublishedTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.Published, error) {
	// Create the table if it doesn't exist using migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "roomserver_published_table_schema_001",
		Patch:       publishedSchema,
		RevertPatch: publishedSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Initialize the table struct with just the connection manager
	t := &publishedTable{
		cm: cm,

		// Initialize SQL query strings from constants
		upsertPublishedSQL:        upsertPublishedSQL,
		selectAllPublishedSQL:     selectAllPublishedSQL,
		selectPublishedSQL:        selectPublishedSQL,
		selectNetworkPublishedSQL: selectNetworkPublishedSQL,
	}

	return t, nil
}

func (t *publishedTable) UpsertRoomPublished(
	ctx context.Context, roomID, appserviceID, networkID string, published bool,
) (err error) {
	db := t.cm.Connection(ctx, false)

	result := db.Exec(t.upsertPublishedSQL, roomID, appserviceID, networkID, published)
	return result.Error
}

func (t *publishedTable) SelectPublishedFromRoomID(
	ctx context.Context, roomID string,
) (published bool, err error) {
	db := t.cm.Connection(ctx, true)

	row := db.Raw(t.selectPublishedSQL, roomID).Row()
	err = row.Scan(&published)
	if sqlutil.ErrorIsNoRows(err) {
		return false, nil
	}
	return
}

func (t *publishedTable) SelectAllPublishedRooms(
	ctx context.Context, networkID string, published, includeAllNetworks bool,
) ([]string, error) {
	db := t.cm.Connection(ctx, true)

	var rows *sql.Rows
	var err error
	if networkID != "" {
		rows, err = db.Raw(t.selectNetworkPublishedSQL, published, networkID).Rows()
	} else {
		rows, err = db.Raw(t.selectAllPublishedSQL, published, includeAllNetworks).Rows()
	}
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectAllPublishedStmt: rows.close() failed")

	var roomIDs []string
	var roomID string
	for rows.Next() {
		if err = rows.Scan(&roomID); err != nil {
			return nil, err
		}

		roomIDs = append(roomIDs, roomID)
	}
	return roomIDs, rows.Err()
}
