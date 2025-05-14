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

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/pitabwire/frame"
)

// threepidSchema defines the schema for third-party identifiers table.
const threepidSchema = `
-- Stores data about third party identifiers
CREATE TABLE IF NOT EXISTS userapi_threepids (
	-- The third party identifier
	threepid TEXT NOT NULL,
	-- The 3PID medium
	medium TEXT NOT NULL DEFAULT 'email',
	-- The localpart of the Global user ID associated to this 3PID
	localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,

	PRIMARY KEY(threepid, medium)
);

CREATE INDEX IF NOT EXISTS userapi_threepid_idx ON userapi_threepids(localpart, server_name);
`

// threepidSchemaRevert defines the revert operation for the threepid schema.
const threepidSchemaRevert = `
DROP TABLE IF EXISTS userapi_threepids;
`

// selectLocalpartForThreePIDSQL is used to retrieve the localpart for a given 3PID.
const selectLocalpartForThreePIDSQL = `
SELECT localpart, server_name FROM userapi_threepids WHERE threepid = $1 AND medium = $2
`

// selectThreePIDsForLocalpartSQL is used to retrieve the 3PIDs for a given localpart.
const selectThreePIDsForLocalpartSQL = `
SELECT threepid, medium FROM userapi_threepids WHERE localpart = $1 AND server_name = $2
`

// insertThreePIDSQL is used to insert a new 3PID.
const insertThreePIDSQL = `
INSERT INTO userapi_threepids (threepid, medium, localpart, server_name) VALUES ($1, $2, $3, $4)
`

// deleteThreePIDSQL is used to delete a 3PID.
const deleteThreePIDSQL = `
DELETE FROM userapi_threepids WHERE threepid = $1 AND medium = $2
`

type threepidTable struct {
	cm                             sqlutil.ConnectionManager
	selectLocalpartForThreePIDSQL  string
	selectThreePIDsForLocalpartSQL string
	insertThreePIDSQL              string
	deleteThreePIDSQL              string
}

// NewPostgresThreePIDTable creates a new ThreePID table.
func NewPostgresThreePIDTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.ThreePIDTable, error) {
	s := &threepidTable{
		cm:                             cm,
		selectLocalpartForThreePIDSQL:  selectLocalpartForThreePIDSQL,
		selectThreePIDsForLocalpartSQL: selectThreePIDsForLocalpartSQL,
		insertThreePIDSQL:              insertThreePIDSQL,
		deleteThreePIDSQL:              deleteThreePIDSQL,
	}

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "userapi_threepids_table_schema_001",
		Patch:       threepidSchema,
		RevertPatch: threepidSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// SelectLocalpartForThreePID retrieves the localpart for a given 3PID.
func (s *threepidTable) SelectLocalpartForThreePID(
	ctx context.Context, threepid string, medium string,
) (localpart string, serverName spec.ServerName, err error) {
	db := s.cm.Connection(ctx, true)
	row := db.Raw(s.selectLocalpartForThreePIDSQL, threepid, medium).Row()
	err = row.Scan(&localpart, &serverName)
	if err != nil {
		// If no rows were found, return empty strings and nil error
		if sqlutil.ErrorIsNoRows(err) {
			return "", "", nil
		}
		return "", "", err
	}
	return
}

// SelectThreePIDsForLocalpart retrieves the 3PIDs for a given localpart.
func (s *threepidTable) SelectThreePIDsForLocalpart(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (threepids []authtypes.ThreePID, err error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectThreePIDsForLocalpartSQL, localpart, serverName).Rows()
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectThreePIDsForLocalpart: failed to close rows")

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

// InsertThreePID inserts a new 3PID.
func (s *threepidTable) InsertThreePID(
	ctx context.Context, threepid, medium,
	localpart string, serverName spec.ServerName,
) (err error) {
	db := s.cm.Connection(ctx, false)
	err = db.Exec(s.insertThreePIDSQL, threepid, medium, localpart, serverName).Error
	return
}

// DeleteThreePID deletes a 3PID.
func (s *threepidTable) DeleteThreePID(
	ctx context.Context, threepid string, medium string) (err error) {
	db := s.cm.Connection(ctx, false)
	err = db.Exec(s.deleteThreePIDSQL, threepid, medium).Error
	return
}
