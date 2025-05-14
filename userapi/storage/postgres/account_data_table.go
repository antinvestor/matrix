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
	"encoding/json"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/pitabwire/frame"
)

// accountDataSchema defines the schema for account data table.
const accountDataSchema = `
-- Stores data about accounts data.
CREATE TABLE IF NOT EXISTS userapi_account_datas (
    -- The Global user ID localpart for this account
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

// accountDataSchemaRevert defines the revert operation for account data schema.
const accountDataSchemaRevert = `
DROP TABLE IF EXISTS userapi_account_datas;
`

// insertAccountDataSQL is used to insert or update account data.
const insertAccountDataSQL = `
	INSERT INTO userapi_account_datas(localpart, server_name, room_id, type, content) VALUES($1, $2, $3, $4, $5)
	ON CONFLICT (localpart, server_name, room_id, type) DO UPDATE SET content = EXCLUDED.content
`

// selectAccountDataSQL is used to retrieve all account data for a user.
const selectAccountDataSQL = "" +
	"SELECT room_id, type, content FROM userapi_account_datas WHERE localpart = $1 AND server_name = $2"

// selectAccountDataByTypeSQL is used to retrieve account data of a specific type for a user.
const selectAccountDataByTypeSQL = "" +
	"SELECT content FROM userapi_account_datas WHERE localpart = $1 AND server_name = $2 AND room_id = $3 AND type = $4"

// accountDataTable represents the account data table in the database.
type accountDataTable struct {
	cm                         sqlutil.ConnectionManager
	insertAccountDataSQL       string
	selectAccountDataSQL       string
	selectAccountDataByTypeSQL string
}

// NewPostgresAccountDataTable creates a new account data table object.
func NewPostgresAccountDataTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.AccountDataTable, error) {
	t := &accountDataTable{
		cm:                         cm,
		insertAccountDataSQL:       insertAccountDataSQL,
		selectAccountDataSQL:       selectAccountDataSQL,
		selectAccountDataByTypeSQL: selectAccountDataByTypeSQL,
	}

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "userapi_account_datas_table_schema_001",
		Patch:       accountDataSchema,
		RevertPatch: accountDataSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// InsertAccountData inserts a new account data entry for a user.
func (t *accountDataTable) InsertAccountData(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	roomID, dataType string, content json.RawMessage,
) (err error) {
	db := t.cm.Connection(ctx, false)
	// Empty/nil json.RawMessage is not interpreted as "nil", so use *json.RawMessage
	// when passing the data to trigger "NOT NULL" constraint
	var data *json.RawMessage
	if len(content) > 0 {
		data = &content
	}
	return db.Exec(t.insertAccountDataSQL, localpart, serverName, roomID, dataType, data).Error
}

// SelectAccountData retrieves all account data for a user.
func (t *accountDataTable) SelectAccountData(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (
/* global */ map[string]json.RawMessage,
/* rooms */ map[string]map[string]json.RawMessage,
	error,
) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectAccountDataSQL, localpart, serverName).Rows()
	if err != nil {
		return nil, nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectAccountData: rows.close() failed")

	global := map[string]json.RawMessage{}
	rooms := map[string]map[string]json.RawMessage{}

	for rows.Next() {
		var roomID string
		var dataType string
		var content []byte

		if err = rows.Scan(&roomID, &dataType, &content); err != nil {
			return nil, nil, err
		}

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

// SelectAccountDataByType retrieves account data of a specific type for a user.
func (t *accountDataTable) SelectAccountDataByType(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	roomID, dataType string,
) (data json.RawMessage, err error) {
	var bytes []byte
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectAccountDataByTypeSQL, localpart, serverName, roomID, dataType).Row()
	if err = row.Scan(&bytes); err != nil {
		if sqlutil.ErrorIsNoRows(err) {
			return nil, nil
		}
		return
	}
	data = bytes
	return
}
