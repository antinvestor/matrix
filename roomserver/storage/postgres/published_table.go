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
	"errors"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres/deltas"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
)

// Schema for the published table
const publishedSchema = `-- Stores which rooms are published in the room directory
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
);`

// SQL query constants for published operations
const (
	// upsertPublishedSQL inserts or updates a record for a room's published status
	upsertPublishedSQL = `INSERT INTO roomserver_published (room_id, appservice_id, network_id, published) VALUES ($1, $2, $3, $4) ON CONFLICT (room_id, appservice_id, network_id) DO UPDATE SET published=$4`

	// selectAllPublishedSQL selects all published rooms, optionally filtering by network
	selectAllPublishedSQL = `SELECT room_id FROM roomserver_published WHERE published = $1 AND CASE WHEN $2 THEN 1=1 ELSE network_id = '' END ORDER BY room_id ASC`

	// selectPublishedSQL checks if a specific room is published
	selectPublishedSQL = `SELECT published FROM roomserver_published WHERE room_id = $1`

	// selectNetworkPublishedSQL selects all published rooms for a specific network
	selectNetworkPublishedSQL = `SELECT room_id FROM roomserver_published WHERE published = $1 AND network_id = $2 ORDER BY room_id ASC`
)

// publishedStatements contains all the SQL statements for published operations
type publishedStatements struct {
	cm *sqlutil.Connections
	
	// SQL statements stored as struct fields
	upsertPublishedStmt       string
	selectAllPublishedStmt    string
	selectPublishedStmt       string
	selectNetworkPublishedStmt string
}

// NewPostgresPublishedTable creates a new PostgreSQL published table
func NewPostgresPublishedTable(ctx context.Context, cm *sqlutil.Connections) (tables.Published, error) {
	// Create the table
	db := cm.Connection(ctx, false)
	if err := db.Exec(publishedSchema).Error; err != nil {
		return nil, err
	}

	s := &publishedStatements{
		cm: cm,
		
		// Initialize SQL statement fields with the constants
		upsertPublishedStmt:       upsertPublishedSQL,
		selectAllPublishedStmt:    selectAllPublishedSQL,
		selectPublishedStmt:       selectPublishedSQL,
		selectNetworkPublishedStmt: selectNetworkPublishedSQL,
	}

	// Run migrations
	m := sqlutil.NewMigrator(cm.Writer.DB)
	m.AddMigrations([]sqlutil.Migration{
		{
			Version: "roomserver: published appservice",
			Up:      deltas.UpPulishedAppservice,
		},
		{
			Version: "roomserver: published appservice pkey",
			Up:      deltas.UpPulishedAppservicePrimaryKey,
		},
	}...)
	if err := m.Up(ctx); err != nil {
		return nil, err
	}

	return s, nil
}

// UpsertRoomPublished updates or inserts the published status for a room
func (s *publishedStatements) UpsertRoomPublished(
	ctx context.Context, roomID, appserviceID, networkID string, published bool,
) (err error) {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.upsertPublishedStmt, roomID, appserviceID, networkID, published).Error
}

// SelectPublishedFromRoomID checks if a room is published
func (s *publishedStatements) SelectPublishedFromRoomID(
	ctx context.Context, roomID string,
) (published bool, err error) {
	db := s.cm.Connection(ctx, true)
	err = db.Raw(s.selectPublishedStmt, roomID).Scan(&published).Error
	if errors.Is(err, internal.ErrNoRows) {
		return false, nil
	}
	return
}

// SelectAllPublishedRooms retrieves all published rooms, optionally filtered by network
func (s *publishedStatements) SelectAllPublishedRooms(
	ctx context.Context, networkID string, published, includeAllNetworks bool,
) ([]string, error) {
	db := s.cm.Connection(ctx, true)

	var rows *sql.Rows
	var err error
	if networkID != "" {
		// Select rooms that are published for a specific network
		rows, err = db.Raw(s.selectNetworkPublishedStmt, published, networkID).Rows()
	} else {
		// Select rooms that are published across all networks
		rows, err = db.Raw(s.selectAllPublishedStmt, published, includeAllNetworks).Rows()
	}

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectAllPublishedRooms: rows.close() failed")

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
