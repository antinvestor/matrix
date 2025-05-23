// Copyright 2022 The Matrix.org Foundation C.I.C.
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

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
)

const ignoresSchema = `
-- Stores data about ignoress
CREATE TABLE IF NOT EXISTS syncapi_ignores (
	-- The user ID whose ignore list this belongs to.
	user_id TEXT NOT NULL,
	ignores_json TEXT NOT NULL,
	PRIMARY KEY(user_id)
);
`

const selectIgnoresSQL = "" +
	"SELECT ignores_json FROM syncapi_ignores WHERE user_id = $1"

const upsertIgnoresSQL = "" +
	"INSERT INTO syncapi_ignores (user_id, ignores_json) VALUES ($1, $2)" +
	" ON CONFLICT (user_id) DO UPDATE set ignores_json = $2"

type ignoresStatements struct {
	selectIgnoresStmt *sql.Stmt
	upsertIgnoresStmt *sql.Stmt
}

func NewPostgresIgnoresTable(ctx context.Context, db *sql.DB) (tables.Ignores, error) {
	_, err := db.Exec(ignoresSchema)
	if err != nil {
		return nil, err
	}
	s := &ignoresStatements{}

	return s, sqlutil.StatementList{
		{&s.selectIgnoresStmt, selectIgnoresSQL},
		{&s.upsertIgnoresStmt, upsertIgnoresSQL},
	}.Prepare(db)
}

func (s *ignoresStatements) SelectIgnores(
	ctx context.Context, txn *sql.Tx, userID string,
) (*types.IgnoredUsers, error) {
	var ignoresData []byte
	err := sqlutil.TxStmt(txn, s.selectIgnoresStmt).QueryRowContext(ctx, userID).Scan(&ignoresData)
	if err != nil {
		return nil, err
	}
	var ignores types.IgnoredUsers
	if err = json.Unmarshal(ignoresData, &ignores); err != nil {
		return nil, err
	}
	return &ignores, nil
}

func (s *ignoresStatements) UpsertIgnores(
	ctx context.Context, txn *sql.Tx, userID string, ignores *types.IgnoredUsers,
) error {
	ignoresJSON, err := json.Marshal(ignores)
	if err != nil {
		return err
	}
	_, err = sqlutil.TxStmt(txn, s.upsertIgnoresStmt).ExecContext(ctx, userID, ignoresJSON)
	return err
}
