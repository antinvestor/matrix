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
	"encoding/json"
	"errors"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

const accountDataSchema = `
-- Stores data about accounts data.
CREATE TABLE IF NOT EXISTS userapi_account_datas (
    -- The Matrix user ID localpart for this account
    localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
    -- The room ID for this data (empty string if not specific to a room)
    room_id TEXT,
    -- The account data type
    type TEXT NOT NULL,
    -- The account data content
    content TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS userapi_account_datas_idx ON userapi_account_datas(localpart, server_name, room_id, type);
`

// SQL query constants for account data operations
const (
	// insertAccountDataSQL inserts or updates account data for a user with ON CONFLICT resolution
	insertAccountDataSQL = `
		INSERT INTO userapi_account_datas(localpart, server_name, room_id, type, content) 
		VALUES($1, $2, $3, $4, $5)
		ON CONFLICT (localpart, server_name, room_id, type) 
		DO UPDATE SET content = EXCLUDED.content
	`

	// selectAccountDataSQL selects all account data for a user
	selectAccountDataSQL = `
		SELECT room_id, type, content FROM userapi_account_datas 
		WHERE localpart = $1 AND server_name = $2
	`

	// selectAccountDataByTypeSQL selects specific account data by type for a user in a room
	selectAccountDataByTypeSQL = `
		SELECT content FROM userapi_account_datas 
		WHERE localpart = $1 AND server_name = $2 AND room_id = $3 AND type = $4
	`
)

type accountDataStatements struct {
	cm *sqlutil.Connections

	// SQL statements stored as struct fields
	insertAccountDataStmt      string
	selectAccountDataStmt      string
	selectAccountDataByTypeStmt string
}

func NewPostgresAccountDataTable(ctx context.Context, cm *sqlutil.Connections) (tables.AccountDataTable, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(accountDataSchema).Error; err != nil {
		return nil, err
	}

	// Initialize table with SQL statements
	t := &accountDataStatements{
		cm: cm,
		insertAccountDataStmt:      insertAccountDataSQL,
		selectAccountDataStmt:      selectAccountDataSQL,
		selectAccountDataByTypeStmt: selectAccountDataByTypeSQL,
	}

	return t, nil
}

func (t *accountDataStatements) InsertAccountData(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	roomID, dataType string, content json.RawMessage,
) (err error) {
	// Empty/nil json.RawMessage is not interpreted as "nil", so use *json.RawMessage
	// when passing the data to trigger "NOT NULL" constraint
	var data *json.RawMessage
	if len(content) > 0 {
		data = &content
	}

	// Get database connection
	db := t.cm.Connection(ctx, false)

	// Execute the query
	return db.Exec(t.insertAccountDataStmt, localpart, serverName, roomID, dataType, data).Error
}

func (t *accountDataStatements) SelectAccountData(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (
/* global */ map[string]json.RawMessage,
/* rooms */ map[string]map[string]json.RawMessage,
	error,
) {
	// Get read-only database connection
	db := t.cm.Connection(ctx, true)

	// Execute the query
	rows, err := db.Raw(t.selectAccountDataStmt, localpart, serverName).Rows()
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	// Initialize maps for results
	global := map[string]json.RawMessage{}
	rooms := map[string]map[string]json.RawMessage{}

	// Iterate over the results
	for rows.Next() {
		var roomID string
		var dataType string
		var content []byte

		if err = rows.Scan(&roomID, &dataType, &content); err != nil {
			return nil, nil, err
		}

		// Organize by room or global
		if roomID != "" {
			if _, ok := rooms[roomID]; !ok {
				rooms[roomID] = map[string]json.RawMessage{}
			}
			rooms[roomID][dataType] = content
		} else {
			global[dataType] = content
		}
	}

	return global, rooms, rows.Err()
}

func (t *accountDataStatements) SelectAccountDataByType(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	roomID, dataType string,
) (data json.RawMessage, err error) {
	// Get read-only database connection
	db := t.cm.Connection(ctx, true)

	// Execute the query
	var bytes []byte
	row := db.Raw(t.selectAccountDataByTypeStmt, localpart, serverName, roomID, dataType).Row()

	if err = row.Scan(&bytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return
	}

	data = bytes
	return
}
