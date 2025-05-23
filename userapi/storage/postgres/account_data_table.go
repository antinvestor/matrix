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
	"github.com/antinvestor/matrix/internal"
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

const insertAccountDataSQL = `
	INSERT INTO userapi_account_datas(localpart, server_name, room_id, type, content) VALUES($1, $2, $3, $4, $5)
	ON CONFLICT (localpart, server_name, room_id, type) DO UPDATE SET content = EXCLUDED.content
`

const selectAccountDataSQL = "" +
	"SELECT room_id, type, content FROM userapi_account_datas WHERE localpart = $1 AND server_name = $2"

const selectAccountDataByTypeSQL = "" +
	"SELECT content FROM userapi_account_datas WHERE localpart = $1 AND server_name = $2 AND room_id = $3 AND type = $4"

type accountDataStatements struct {
	insertAccountDataStmt       *sql.Stmt
	selectAccountDataStmt       *sql.Stmt
	selectAccountDataByTypeStmt *sql.Stmt
}

func NewPostgresAccountDataTable(ctx context.Context, db *sql.DB) (tables.AccountDataTable, error) {
	s := &accountDataStatements{}
	_, err := db.Exec(accountDataSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertAccountDataStmt, insertAccountDataSQL},
		{&s.selectAccountDataStmt, selectAccountDataSQL},
		{&s.selectAccountDataByTypeStmt, selectAccountDataByTypeSQL},
	}.Prepare(db)
}

func (s *accountDataStatements) InsertAccountData(
	ctx context.Context, txn *sql.Tx,
	localpart string, serverName spec.ServerName,
	roomID, dataType string, content json.RawMessage,
) (err error) {
	stmt := sqlutil.TxStmt(txn, s.insertAccountDataStmt)
	// Empty/nil json.RawMessage is not interpreted as "nil", so use *json.RawMessage
	// when passing the data to trigger "NOT NULL" constraint
	var data *json.RawMessage
	if len(content) > 0 {
		data = &content
	}
	_, err = stmt.ExecContext(ctx, localpart, serverName, roomID, dataType, data)
	return
}

func (s *accountDataStatements) SelectAccountData(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (
	/* global */ map[string]json.RawMessage,
	/* rooms */ map[string]map[string]json.RawMessage,
	error,
) {
	rows, err := s.selectAccountDataStmt.QueryContext(ctx, localpart, serverName)
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

func (s *accountDataStatements) SelectAccountDataByType(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	roomID, dataType string,
) (data json.RawMessage, err error) {
	var bytes []byte
	stmt := s.selectAccountDataByTypeStmt
	if err = stmt.QueryRowContext(ctx, localpart, serverName, roomID, dataType).Scan(&bytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return
	}
	data = bytes
	return
}
