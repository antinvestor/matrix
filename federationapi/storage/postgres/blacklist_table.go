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

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
)

const blacklistSchema = `
CREATE TABLE IF NOT EXISTS federationsender_blacklist (
    -- The blacklisted server name
	server_name TEXT NOT NULL,
	UNIQUE (server_name)
);
`

const insertBlacklistSQL = "" +
	"INSERT INTO federationsender_blacklist (server_name) VALUES ($1)" +
	" ON CONFLICT DO NOTHING"

const selectBlacklistSQL = "" +
	"SELECT server_name FROM federationsender_blacklist WHERE server_name = $1"

const deleteBlacklistSQL = "" +
	"DELETE FROM federationsender_blacklist WHERE server_name = $1"

const deleteAllBlacklistSQL = "" +
	"TRUNCATE federationsender_blacklist"

type blacklistStatements struct {
	db                     *sql.DB
	insertBlacklistStmt    *sql.Stmt
	selectBlacklistStmt    *sql.Stmt
	deleteBlacklistStmt    *sql.Stmt
	deleteAllBlacklistStmt *sql.Stmt
}

func NewPostgresBlacklistTable(ctx context.Context, db *sql.DB) (s *blacklistStatements, err error) {
	s = &blacklistStatements{
		db: db,
	}
	_, err = db.Exec(blacklistSchema)
	if err != nil {
		return
	}

	return s, sqlutil.StatementList{
		{&s.insertBlacklistStmt, insertBlacklistSQL},
		{&s.selectBlacklistStmt, selectBlacklistSQL},
		{&s.deleteBlacklistStmt, deleteBlacklistSQL},
		{&s.deleteAllBlacklistStmt, deleteAllBlacklistSQL},
	}.Prepare(db)
}

func (s *blacklistStatements) InsertBlacklist(
	ctx context.Context, txn *sql.Tx, serverName spec.ServerName,
) error {
	stmt := sqlutil.TxStmt(txn, s.insertBlacklistStmt)
	_, err := stmt.ExecContext(ctx, serverName)
	return err
}

func (s *blacklistStatements) SelectBlacklist(
	ctx context.Context, txn *sql.Tx, serverName spec.ServerName,
) (bool, error) {
	stmt := sqlutil.TxStmt(txn, s.selectBlacklistStmt)
	res, err := stmt.QueryContext(ctx, serverName)
	if err != nil {
		return false, err
	}
	defer res.Close() // nolint:errcheck
	// The query will return the server name if the server is blacklisted, and
	// will return no rows if not. By calling Next, we find out if a row was
	// returned or not - we don't care about the value itself.
	return res.Next(), nil
}

func (s *blacklistStatements) DeleteBlacklist(
	ctx context.Context, txn *sql.Tx, serverName spec.ServerName,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteBlacklistStmt)
	_, err := stmt.ExecContext(ctx, serverName)
	return err
}

func (s *blacklistStatements) DeleteAllBlacklist(
	ctx context.Context, txn *sql.Tx,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteAllBlacklistStmt)
	_, err := stmt.ExecContext(ctx)
	return err
}
