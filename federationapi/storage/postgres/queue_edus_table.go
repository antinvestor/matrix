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

	"github.com/lib/pq"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/postgres/deltas"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
)

const queueEDUsSchema = `
CREATE TABLE IF NOT EXISTS federationsender_queue_edus (
	-- The type of the event (informational).
	edu_type TEXT NOT NULL,
    -- The domain part of the user ID the EDU event is for.
	server_name TEXT NOT NULL,
	-- The JSON NID from the federationsender_queue_edus_json table.
	json_nid BIGINT NOT NULL,
	-- The expiry time of this edu, if any.
	expires_at BIGINT NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS federationsender_queue_edus_json_nid_idx
    ON federationsender_queue_edus (json_nid, server_name);
CREATE INDEX IF NOT EXISTS federationsender_queue_edus_nid_idx
    ON federationsender_queue_edus (json_nid);
CREATE INDEX IF NOT EXISTS federationsender_queue_edus_server_name_idx
    ON federationsender_queue_edus (server_name);
`

// SQL query string constants
const (
	// insertQueueEDUSQL inserts a new EDU into the queue for a specific server
	insertQueueEDUSQL = "" +
		"INSERT INTO federationsender_queue_edus (edu_type, server_name, json_nid, expires_at)" +
		" VALUES ($1, $2, $3, $4)"

	// deleteQueueEDUSQL deletes EDUs from the queue for a specific server by JSON NIDs
	deleteQueueEDUSQL = "" +
		"DELETE FROM federationsender_queue_edus WHERE server_name = $1 AND json_nid = ANY($2)"

	// selectQueueEDUSQL retrieves JSON NIDs of queued EDUs for a specific server with a limit
	selectQueueEDUSQL = "" +
		"SELECT json_nid FROM federationsender_queue_edus" +
		" WHERE server_name = $1" +
		" LIMIT $2"

	// selectQueueEDUReferenceJSONCountSQL counts references to a specific JSON NID in the queue
	selectQueueEDUReferenceJSONCountSQL = "" +
		"SELECT COUNT(*) FROM federationsender_queue_edus" +
		" WHERE json_nid = $1"

	// selectQueueServerNamesSQL retrieves all distinct server names that have queued EDUs
	selectQueueServerNamesSQL = "" +
		"SELECT DISTINCT server_name FROM federationsender_queue_edus"

	// selectExpiredEDUsSQL retrieves JSON NIDs of EDUs that have expired
	selectExpiredEDUsSQL = "" +
		"SELECT DISTINCT json_nid FROM federationsender_queue_edus WHERE expires_at > 0 AND expires_at <= $1"

	// deleteExpiredEDUsSQL deletes all EDUs that have expired
	deleteExpiredEDUsSQL = "" +
		"DELETE FROM federationsender_queue_edus WHERE expires_at > 0 AND expires_at <= $1"
)

// queueEDUsTable contains the postgres-specific implementation
type queueEDUsTable struct {
	cm *sqlutil.Connections

	insertQueueEDUStmt                   string
	deleteQueueEDUStmt                   string
	selectQueueEDUStmt                   string
	selectQueueEDUReferenceJSONCountStmt string
	selectQueueEDUServerNamesStmt        string
	selectExpiredEDUsStmt                string
	deleteExpiredEDUsStmt                string
}

// NewPostgresQueueEDUsTable creates a new postgres queue EDUs table and prepares all statements
func NewPostgresQueueEDUsTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationQueueEDUs, error) {
	// Initialize schema using GORM
	gormDB := cm.Connection(ctx, false)
	if err := gormDB.Exec(queueEDUsSchema).Error; err != nil {
		return nil, err
	}

	// Apply migrations
	m := sqlutil.NewMigrations()
	m.AddMigrations(
		sqlutil.Migration{
			Version: "federationapi: add expiresat column",
			Up:      deltas.UpAddexpiresat,
		},
	)
	if err := m.RunMigrations(ctx, gormDB); err != nil {
		return nil, err
	}

	s := &queueEDUsTable{
		cm:                                   cm,
		insertQueueEDUStmt:                   insertQueueEDUSQL,
		deleteQueueEDUStmt:                   deleteQueueEDUSQL,
		selectQueueEDUStmt:                   selectQueueEDUSQL,
		selectQueueEDUReferenceJSONCountStmt: selectQueueEDUReferenceJSONCountSQL,
		selectQueueEDUServerNamesStmt:        selectQueueServerNamesSQL,
		selectExpiredEDUsStmt:                selectExpiredEDUsSQL,
		deleteExpiredEDUsStmt:                deleteExpiredEDUsSQL,
	}

	return s, nil
}

// InsertQueueEDU adds a new EDU to the queue
func (s *queueEDUsTable) InsertQueueEDU(
	ctx context.Context,
	eduType string,
	serverName spec.ServerName,
	nid int64,
	expiresAt spec.Timestamp,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	return db.Exec(
		s.insertQueueEDUStmt,
		eduType,    // the EDU type
		serverName, // destination server name
		nid,        // JSON blob NID
		expiresAt,  // timestamp of expiry
	).Error
}

// DeleteQueueEDUs removes EDUs from the queue for a specific server
func (s *queueEDUsTable) DeleteQueueEDUs(
	ctx context.Context,
	serverName spec.ServerName,
	jsonNIDs []int64,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	return db.Exec(
		s.deleteQueueEDUStmt,
		serverName, pq.Array(jsonNIDs),
	).Error
}

// SelectQueueEDUs retrieves EDUs from the queue for a specific server
func (s *queueEDUsTable) SelectQueueEDUs(
	ctx context.Context,
	serverName spec.ServerName,
	limit int,
) ([]int64, error) {
	// Get read-only database connection
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(
		s.selectQueueEDUStmt,
		serverName, limit,
	).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectQueueEDUs: rows.close() failed")

	var result []int64
	for rows.Next() {
		var nid int64
		if err = rows.Scan(&nid); err != nil {
			return nil, err
		}
		result = append(result, nid)
	}

	return result, rows.Err()
}

// SelectQueueEDUReferenceJSONCount counts references to a JSON NID in the queue
func (s *queueEDUsTable) SelectQueueEDUReferenceJSONCount(
	ctx context.Context, jsonNID int64,
) (int64, error) {
	// Get read-only database connection
	db := s.cm.Connection(ctx, true)

	var count int64
	row := db.Raw(
		s.selectQueueEDUReferenceJSONCountStmt,
		jsonNID,
	).Row()
	err := row.Scan(&count)
	if errors.Is(err, sql.ErrNoRows) {
		// It's acceptable for there to be no rows referencing a given
		// JSON NID but it's not an error condition. Just return as if
		// there's a zero count.
		return 0, nil
	}
	return count, err
}

// SelectQueueEDUServerNames retrieves all server names with queued EDUs
func (s *queueEDUsTable) SelectQueueEDUServerNames(
	ctx context.Context,
) ([]spec.ServerName, error) {
	// Get read-only database connection
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(
		s.selectQueueEDUServerNamesStmt,
	).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectQueueEDUServerNames: rows.close() failed")

	var result []spec.ServerName
	for rows.Next() {
		var serverName string
		if err = rows.Scan(&serverName); err != nil {
			return nil, err
		}
		result = append(result, spec.ServerName(serverName))
	}

	return result, rows.Err()
}

// SelectExpiredEDUs retrieves JSON NIDs of expired EDUs
func (s *queueEDUsTable) SelectExpiredEDUs(
	ctx context.Context,
	expiredBefore spec.Timestamp,
) ([]int64, error) {
	// Get read-only database connection
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(
		s.selectExpiredEDUsStmt,
		expiredBefore,
	).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectExpiredEDUs: rows.close() failed")

	var result []int64
	for rows.Next() {
		var nid int64
		if err = rows.Scan(&nid); err != nil {
			return nil, err
		}
		result = append(result, nid)
	}

	return result, rows.Err()
}

// DeleteExpiredEDUs removes all expired EDUs
func (s *queueEDUsTable) DeleteExpiredEDUs(
	ctx context.Context,
	expiredBefore spec.Timestamp,
) error {
	// Get writable database connection
	db := s.cm.Connection(ctx, false)

	return db.Exec(
		s.deleteExpiredEDUsStmt,
		expiredBefore,
	).Error
}

// Prepare is called to initialize the table before use
func (s *queueEDUsTable) Prepare() error {
	return nil
}
