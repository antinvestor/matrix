// Copyright 2017 Vector Creations Ltd
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

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

const threepidSchema = `
-- Stores data about third party identifiers
CREATE TABLE IF NOT EXISTS userapi_threepids (
	-- The third party identifier
	threepid TEXT NOT NULL,
	-- The 3PID medium
	medium TEXT NOT NULL DEFAULT 'email',
	-- The localpart of the Matrix user ID associated to this 3PID
	localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,

	PRIMARY KEY(threepid, medium)
);

CREATE INDEX IF NOT EXISTS userapi_threepid_idx ON userapi_threepids(localpart, server_name);
`

// SQL query constants for threepid operations
const (
	// selectLocalpartForThreePIDSQL retrieves a user's localpart based on their threepid
	selectLocalpartForThreePIDSQL = "SELECT localpart, server_name FROM userapi_threepids WHERE threepid = $1 AND medium = $2"

	// selectThreePIDsForLocalpartSQL retrieves all threepids associated with a user's localpart
	selectThreePIDsForLocalpartSQL = "SELECT threepid, medium FROM userapi_threepids WHERE localpart = $1 AND server_name = $2"

	// insertThreePIDSQL adds a new threepid association
	insertThreePIDSQL = "INSERT INTO userapi_threepids (threepid, medium, localpart, server_name) VALUES ($1, $2, $3, $4)"

	// deleteThreePIDSQL removes a threepid association
	deleteThreePIDSQL = "DELETE FROM userapi_threepids WHERE threepid = $1 AND medium = $2"
)

// threepidTable contains the postgres-specific implementation
type threepidTable struct {
	cm *sqlutil.Connections

	// SQL queries stored as fields for better maintainability
	selectLocalpartForThreePIDStmt  string
	selectThreePIDsForLocalpartStmt string
	insertThreePIDStmt              string
	deleteThreePIDStmt              string
}

func NewPostgresThreePIDTable(ctx context.Context, cm *sqlutil.Connections) (tables.ThreePIDTable, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(threepidSchema).Error; err != nil {
		return nil, err
	}

	// Initialize table with SQL statements
	t := &threepidTable{
		cm:                              cm,
		selectLocalpartForThreePIDStmt:  selectLocalpartForThreePIDSQL,
		selectThreePIDsForLocalpartStmt: selectThreePIDsForLocalpartSQL,
		insertThreePIDStmt:              insertThreePIDSQL,
		deleteThreePIDStmt:              deleteThreePIDSQL,
	}

	return t, nil
}

func (t *threepidTable) SelectLocalpartForThreePID(
	ctx context.Context, threepid string, medium string,
) (localpart string, serverName spec.ServerName, err error) {
	db := t.cm.Connection(ctx, true)

	row := db.Raw(t.selectLocalpartForThreePIDStmt, threepid, medium).Row()
	err = row.Scan(&localpart, &serverName)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	return
}

func (t *threepidTable) SelectThreePIDsForLocalpart(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (threepids []authtypes.ThreePID, err error) {
	// Get read-only database connection
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(t.selectThreePIDsForLocalpartStmt, localpart, serverName).Rows()
	if err != nil {
		return
	}
	defer rows.Close()

	threepids = []authtypes.ThreePID{}
	for rows.Next() {
		var threepid string
		var medium string
		if err = rows.Scan(&threepid, &medium); err != nil {
			return
		}
		threepids = append(threepids, authtypes.ThreePID{
			Address: threepid,
			Medium:  medium,
		})
	}
	err = rows.Err()
	return
}

func (t *threepidTable) InsertThreePID(
	ctx context.Context, threepid, medium,
	localpart string, serverName spec.ServerName,
) (err error) {
	db := t.cm.Connection(ctx, false)

	return db.Exec(t.insertThreePIDStmt, threepid, medium, localpart, serverName).Error
}

func (t *threepidTable) DeleteThreePID(
	ctx context.Context, threepid string, medium string) (err error) {
	db := t.cm.Connection(ctx, false)

	return db.Exec(t.deleteThreePIDStmt, threepid, medium).Error
}
