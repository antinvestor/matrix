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

const insertQueueEDUSQL = "" +
	"INSERT INTO federationsender_queue_edus (edu_type, server_name, json_nid, expires_at)" +
	" VALUES ($1, $2, $3, $4)"

const deleteQueueEDUSQL = "" +
	"DELETE FROM federationsender_queue_edus WHERE server_name = $1 AND json_nid = ANY($2)"

const selectQueueEDUSQL = "" +
	"SELECT json_nid FROM federationsender_queue_edus" +
	" WHERE server_name = $1" +
	" LIMIT $2"

const selectQueueEDUReferenceJSONCountSQL = "" +
	"SELECT COUNT(*) FROM federationsender_queue_edus" +
	" WHERE json_nid = $1"

const selectQueueServerNamesSQL = "" +
	"SELECT DISTINCT server_name FROM federationsender_queue_edus"

const selectExpiredEDUsSQL = "" +
	"SELECT DISTINCT json_nid FROM federationsender_queue_edus WHERE expires_at > 0 AND expires_at <= $1"

const deleteExpiredEDUsSQL = "" +
	"DELETE FROM federationsender_queue_edus WHERE expires_at > 0 AND expires_at <= $1"

type queueEDUsStatements struct {
	db                                   *sql.DB
	insertQueueEDUStmt                   *sql.Stmt
	deleteQueueEDUStmt                   *sql.Stmt
	selectQueueEDUStmt                   *sql.Stmt
	selectQueueEDUReferenceJSONCountStmt *sql.Stmt
	selectQueueEDUServerNamesStmt        *sql.Stmt
	selectExpiredEDUsStmt                *sql.Stmt
	deleteExpiredEDUsStmt                *sql.Stmt
}

func NewPostgresQueueEDUsTable(ctx context.Context, db *sql.DB) (s *queueEDUsStatements, err error) {
	s = &queueEDUsStatements{
		db: db,
	}
	_, err = s.db.Exec(queueEDUsSchema)
	if err != nil {
		return s, err
	}

	m := sqlutil.NewMigrator(db)
	m.AddMigrations(
		sqlutil.Migration{
			Version: "federationapi: add expiresat column",
			Up:      deltas.UpAddexpiresat,
		},
	)
	if err := m.Up(ctx); err != nil {
		return s, err
	}

	return s, nil
}

func (s *queueEDUsStatements) Prepare() error {
	return sqlutil.StatementList{
		{&s.insertQueueEDUStmt, insertQueueEDUSQL},
		{&s.deleteQueueEDUStmt, deleteQueueEDUSQL},
		{&s.selectQueueEDUStmt, selectQueueEDUSQL},
		{&s.selectQueueEDUReferenceJSONCountStmt, selectQueueEDUReferenceJSONCountSQL},
		{&s.selectQueueEDUServerNamesStmt, selectQueueServerNamesSQL},
		{&s.selectExpiredEDUsStmt, selectExpiredEDUsSQL},
		{&s.deleteExpiredEDUsStmt, deleteExpiredEDUsSQL},
	}.Prepare(s.db)
}

func (s *queueEDUsStatements) InsertQueueEDU(
	ctx context.Context,
	txn *sql.Tx,
	eduType string,
	serverName spec.ServerName,
	nid int64,
	expiresAt spec.Timestamp,
) error {
	stmt := sqlutil.TxStmt(txn, s.insertQueueEDUStmt)
	_, err := stmt.ExecContext(
		ctx,
		eduType,    // the EDU type
		serverName, // destination server name
		nid,        // JSON blob NID
		expiresAt,  // timestamp of expiry
	)
	return err
}

func (s *queueEDUsStatements) DeleteQueueEDUs(
	ctx context.Context, txn *sql.Tx,
	serverName spec.ServerName,
	jsonNIDs []int64,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteQueueEDUStmt)
	_, err := stmt.ExecContext(ctx, serverName, pq.Int64Array(jsonNIDs))
	return err
}

func (s *queueEDUsStatements) SelectQueueEDUs(
	ctx context.Context, txn *sql.Tx,
	serverName spec.ServerName,
	limit int,
) ([]int64, error) {
	stmt := sqlutil.TxStmt(txn, s.selectQueueEDUStmt)
	rows, err := stmt.QueryContext(ctx, serverName, limit)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "queueFromStmt: rows.close() failed")
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

func (s *queueEDUsStatements) SelectQueueEDUReferenceJSONCount(
	ctx context.Context, txn *sql.Tx, jsonNID int64,
) (int64, error) {
	var count int64
	stmt := sqlutil.TxStmt(txn, s.selectQueueEDUReferenceJSONCountStmt)
	err := stmt.QueryRowContext(ctx, jsonNID).Scan(&count)
	if errors.Is(err, sql.ErrNoRows) {
		return -1, nil
	}
	return count, err
}

func (s *queueEDUsStatements) SelectQueueEDUServerNames(
	ctx context.Context, txn *sql.Tx,
) ([]spec.ServerName, error) {
	stmt := sqlutil.TxStmt(txn, s.selectQueueEDUServerNamesStmt)
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "queueFromStmt: rows.close() failed")
	var result []spec.ServerName
	for rows.Next() {
		var serverName spec.ServerName
		if err = rows.Scan(&serverName); err != nil {
			return nil, err
		}
		result = append(result, serverName)
	}

	return result, rows.Err()
}

func (s *queueEDUsStatements) SelectExpiredEDUs(
	ctx context.Context, txn *sql.Tx,
	expiredBefore spec.Timestamp,
) ([]int64, error) {
	stmt := sqlutil.TxStmt(txn, s.selectExpiredEDUsStmt)
	rows, err := stmt.QueryContext(ctx, expiredBefore)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectExpiredEDUs: rows.close() failed")
	var result []int64
	var nid int64
	for rows.Next() {
		if err = rows.Scan(&nid); err != nil {
			return nil, err
		}
		result = append(result, nid)
	}
	return result, rows.Err()
}

func (s *queueEDUsStatements) DeleteExpiredEDUs(
	ctx context.Context, txn *sql.Tx,
	expiredBefore spec.Timestamp,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteExpiredEDUsStmt)
	_, err := stmt.ExecContext(ctx, expiredBefore)
	return err
}
