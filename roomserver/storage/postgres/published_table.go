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
	"errors"
	"github.com/antinvestor/matrix/internal"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
)

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

const publishedSchemaRevert = `DROP TABLE IF EXISTS roomserver_published;`

const upsertPublishedSQL = "" +
	"INSERT INTO roomserver_published (room_id, appservice_id, network_id, published) VALUES ($1, $2, $3, $4) " +
	"ON CONFLICT (room_id, appservice_id, network_id) DO UPDATE SET published=$4"

const selectAllPublishedSQL = "" +
	"SELECT room_id FROM roomserver_published WHERE published = $1 AND CASE WHEN $2 THEN 1=1 ELSE network_id = '' END ORDER BY room_id ASC"

const selectNetworkPublishedSQL = "" +
	"SELECT room_id FROM roomserver_published WHERE published = $1 AND network_id = $2 ORDER BY room_id ASC"

const selectPublishedSQL = "" +
	"SELECT published FROM roomserver_published WHERE room_id = $1"

type publishedTable struct {
	cm                        *sqlutil.Connections
	upsertPublishedSQL        string
	selectAllPublishedSQL     string
	selectNetworkPublishedSQL string
	selectPublishedSQL        string
}

func NewPostgresPublishedTable(cm *sqlutil.Connections) tables.Published {
	return &publishedTable{
		cm:                        cm,
		upsertPublishedSQL:        upsertPublishedSQL,
		selectAllPublishedSQL:     selectAllPublishedSQL,
		selectNetworkPublishedSQL: selectNetworkPublishedSQL,
		selectPublishedSQL:        selectPublishedSQL,
	}
}

func (t *publishedTable) UpsertRoomPublished(ctx context.Context, roomID, appserviceID, networkID string, published bool) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.upsertPublishedSQL, roomID, appserviceID, networkID, published).Error
}

func (t *publishedTable) SelectPublishedFromRoomID(ctx context.Context, roomID string) (bool, error) {
	db := t.cm.Connection(ctx, true)
	var published bool
	row := db.Raw(t.selectPublishedSQL, roomID).Row()
	err := row.Scan(&published)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return published, err
}

func (t *publishedTable) SelectAllPublishedRooms(ctx context.Context, networkID string, published, includeAllNetworks bool) ([]string, error) {
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
	defer internal.CloseAndLogIfError(ctx, rows, "failed to close rows")
	var roomIDs []string
	for rows.Next() {
		var roomID string
		if err = rows.Scan(&roomID); err != nil {
			return nil, err
		}
		roomIDs = append(roomIDs, roomID)
	}
	return roomIDs, rows.Err()
}
