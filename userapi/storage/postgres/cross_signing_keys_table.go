// Copyright 2021 The Matrix.org Foundation C.I.C.
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
	"fmt"

	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/antinvestor/matrix/userapi/types"
)

var crossSigningKeysSchema = `
CREATE TABLE IF NOT EXISTS keyserver_cross_signing_keys (
    user_id TEXT NOT NULL,
	key_type SMALLINT NOT NULL,
	key_data TEXT NOT NULL,
	PRIMARY KEY (user_id, key_type)
);
`

const selectCrossSigningKeysForUserSQL = "" +
	"SELECT key_type, key_data FROM keyserver_cross_signing_keys" +
	" WHERE user_id = $1"

const upsertCrossSigningKeysForUserSQL = "" +
	"INSERT INTO keyserver_cross_signing_keys (user_id, key_type, key_data)" +
	" VALUES($1, $2, $3)" +
	" ON CONFLICT (user_id, key_type) DO UPDATE SET key_data = $3"

type crossSigningKeysStatements struct {
	db                                *sql.DB
	selectCrossSigningKeysForUserStmt *sql.Stmt
	upsertCrossSigningKeysForUserStmt *sql.Stmt
}

func NewPostgresCrossSigningKeysTable(ctx context.Context, db *sql.DB) (tables.CrossSigningKeys, error) {
	s := &crossSigningKeysStatements{
		db: db,
	}
	_, err := db.Exec(crossSigningKeysSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.selectCrossSigningKeysForUserStmt, selectCrossSigningKeysForUserSQL},
		{&s.upsertCrossSigningKeysForUserStmt, upsertCrossSigningKeysForUserSQL},
	}.Prepare(db)
}

func (s *crossSigningKeysStatements) SelectCrossSigningKeysForUser(
	ctx context.Context, txn *sql.Tx, userID string,
) (r types.CrossSigningKeyMap, err error) {
	rows, err := sqlutil.TxStmt(txn, s.selectCrossSigningKeysForUserStmt).QueryContext(ctx, userID)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectCrossSigningKeysForUserStmt: rows.close() failed")
	r = types.CrossSigningKeyMap{}
	for rows.Next() {
		var keyTypeInt int16
		var keyData spec.Base64Bytes
		if err = rows.Scan(&keyTypeInt, &keyData); err != nil {
			return nil, err
		}
		keyType, ok := types.KeyTypeIntToPurpose[keyTypeInt]
		if !ok {
			return nil, fmt.Errorf("unknown key purpose int %d", keyTypeInt)
		}
		r[keyType] = keyData
	}
	err = rows.Err()
	return
}

func (s *crossSigningKeysStatements) UpsertCrossSigningKeysForUser(
	ctx context.Context, txn *sql.Tx, userID string, keyType fclient.CrossSigningKeyPurpose, keyData spec.Base64Bytes,
) error {
	keyTypeInt, ok := types.KeyTypePurposeToInt[keyType]
	if !ok {
		return fmt.Errorf("unknown key purpose %q", keyType)
	}
	if _, err := sqlutil.TxStmt(txn, s.upsertCrossSigningKeysForUserStmt).ExecContext(ctx, userID, keyTypeInt, keyData); err != nil {
		return fmt.Errorf("s.upsertCrossSigningKeysForUserStmt: %w", err)
	}
	return nil
}
