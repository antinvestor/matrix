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

const accountDataSchemaRevert = "DROP TABLE IF EXISTS userapi_account_datas CASCADE; DROP INDEX IF EXISTS userapi_account_datas_idx;"

const insertAccountDataSQL = `
	INSERT INTO userapi_account_datas(localpart, server_name, room_id, type, content) VALUES($1, $2, $3, $4, $5)
	ON CONFLICT (localpart, server_name, room_id, type) DO UPDATE SET content = EXCLUDED.content
`

const selectAccountDataSQL = "" +
	"SELECT room_id, type, content FROM userapi_account_datas WHERE localpart = $1 AND server_name = $2"

const selectAccountDataByTypeSQL = "" +
	"SELECT content FROM userapi_account_datas WHERE localpart = $1 AND server_name = $2 AND room_id = $3 AND type = $4"

// accountDataTable implements tables.AccountDataTable using GORM and a connection manager.
type accountDataTable struct {
	cm *sqlutil.Connections

	insertAccountDataSQL       string
	selectAccountDataSQL       string
	selectAccountDataByTypeSQL string
}

// NewPostgresAccountDataTable returns a new AccountDataTable using the provided connection manager.
func NewPostgresAccountDataTable(cm *sqlutil.Connections) tables.AccountDataTable {
	return &accountDataTable{
		cm:                         cm,
		insertAccountDataSQL:       insertAccountDataSQL,
		selectAccountDataSQL:       selectAccountDataSQL,
		selectAccountDataByTypeSQL: selectAccountDataByTypeSQL,
	}
}

// InsertAccountData inserts or updates account data for a user and room/type.
func (t *accountDataTable) InsertAccountData(ctx context.Context, localpart string, serverName spec.ServerName, roomID, dataType string, content json.RawMessage) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.insertAccountDataSQL, localpart, serverName, roomID, dataType, content)
	return result.Error
}

// SelectAccountData retrieves all account data for a user, returning global and per-room data maps.
func (t *accountDataTable) SelectAccountData(ctx context.Context, localpart string, serverName spec.ServerName) (map[string]json.RawMessage, map[string]map[string]json.RawMessage, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectAccountDataSQL, localpart, serverName).Rows()
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	global := map[string]json.RawMessage{}
	rooms := map[string]map[string]json.RawMessage{}
	for rows.Next() {
		var roomID, dataType string
		var content []byte
		if err := rows.Scan(&roomID, &dataType, &content); err != nil {
			return nil, nil, err
		}
		if roomID != "" {
			if _, ok := rooms[roomID]; !ok {
				rooms[roomID] = map[string]json.RawMessage{}
			}
			rooms[roomID][dataType] = json.RawMessage(content)
		} else {
			global[dataType] = json.RawMessage(content)
		}
	}
	return global, rooms, rows.Err()
}

// SelectAccountDataByType retrieves account data for a user, room, and type.
func (t *accountDataTable) SelectAccountDataByType(ctx context.Context, localpart string, serverName spec.ServerName, roomID, dataType string) (json.RawMessage, error) {
	db := t.cm.Connection(ctx, true)
	var content sql.NullString
	row := db.Raw(t.selectAccountDataByTypeSQL, localpart, serverName, roomID, dataType).Row()
	if err := row.Scan(&content); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if content.Valid {
		return json.RawMessage(content.String), nil
	}
	return nil, nil
}
