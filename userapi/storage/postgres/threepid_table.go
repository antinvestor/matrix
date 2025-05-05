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
	-- The localpart of the Global user ID associated to this 3PID
	localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,

	PRIMARY KEY(threepid, medium)
);

CREATE INDEX IF NOT EXISTS userapi_threepid_idx ON userapi_threepids(localpart, server_name);
`

const threepidSchemaRevert = "DROP TABLE IF EXISTS userapi_threepids CASCADE; DROP INDEX IF EXISTS userapi_threepid_idx;"

const selectLocalpartForThreePIDSQL = "" +
	"SELECT localpart, server_name FROM userapi_threepids WHERE threepid = $1 AND medium = $2"

const selectThreePIDsForLocalpartSQL = "" +
	"SELECT threepid, medium FROM userapi_threepids WHERE localpart = $1 AND server_name = $2"

const insertThreePIDSQL = "" +
	"INSERT INTO userapi_threepids (threepid, medium, localpart, server_name) VALUES ($1, $2, $3, $4)"

const deleteThreePIDSQL = "" +
	"DELETE FROM userapi_threepids WHERE threepid = $1 AND medium = $2"

// threepidTable implements tables.ThreePIDTable using GORM and a connection manager.
type threepidTable struct {
	cm *sqlutil.Connections

	selectLocalpartForThreePIDSQL  string
	selectThreePIDsForLocalpartSQL string
	insertThreePIDSQL              string
	deleteThreePIDSQL              string
}

// NewPostgresThreePIDTable returns a new ThreePIDTable using the provided connection manager.
func NewPostgresThreePIDTable(cm *sqlutil.Connections) tables.ThreePIDTable {
	return &threepidTable{
		cm:                             cm,
		selectLocalpartForThreePIDSQL:  selectLocalpartForThreePIDSQL,
		selectThreePIDsForLocalpartSQL: selectThreePIDsForLocalpartSQL,
		insertThreePIDSQL:              insertThreePIDSQL,
		deleteThreePIDSQL:              deleteThreePIDSQL,
	}
}

// SelectLocalpartForThreePID returns the localpart and server name for a given 3PID and medium.
func (t *threepidTable) SelectLocalpartForThreePID(ctx context.Context, threepid, medium string) (string, spec.ServerName, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectLocalpartForThreePIDSQL, threepid, medium).Row()
	var localpart string
	var serverName spec.ServerName
	if err := row.Scan(&localpart, &serverName); err != nil {
		return "", "", err
	}
	return localpart, serverName, nil
}

// SelectThreePIDsForLocalpart returns all 3PIDs for a given localpart and server name.
func (t *threepidTable) SelectThreePIDsForLocalpart(ctx context.Context, localpart string, serverName spec.ServerName) ([]authtypes.ThreePID, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectThreePIDsForLocalpartSQL, localpart, serverName).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var threepids []authtypes.ThreePID
	for rows.Next() {
		var pid authtypes.ThreePID
		if err := rows.Scan(&pid.Address, &pid.Medium); err != nil {
			return nil, err
		}
		threepids = append(threepids, pid)
	}
	return threepids, nil
}

// InsertThreePID inserts a new 3PID mapping.
func (t *threepidTable) InsertThreePID(ctx context.Context, threepid, medium, localpart string, serverName spec.ServerName) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.insertThreePIDSQL, threepid, medium, localpart, serverName)
	return result.Error
}

// DeleteThreePID deletes a 3PID mapping.
func (t *threepidTable) DeleteThreePID(ctx context.Context, threepid, medium string) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteThreePIDSQL, threepid, medium)
	return result.Error
}

// AutoMigrate migrates the schema.
func (t *threepidTable) AutoMigrate(ctx context.Context) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(threepidSchema).Error
}

// AutoMigrateRevert reverts the schema migration.
func (t *threepidTable) AutoMigrateRevert(ctx context.Context) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(threepidSchemaRevert).Error
}
