// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Matrix.org Foundation C.I.C.
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

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/lib/pq"
)

const serverSigningKeysSchema = `
-- A cache of signing keys downloaded from remote servers.
CREATE TABLE IF NOT EXISTS keydb_server_keys (
	-- The name of the matrix server the key is for.
	server_name TEXT NOT NULL,
	-- The ID of the server key.
	server_key_id TEXT NOT NULL,
	-- Combined server name and key ID separated by the ASCII unit separator
	-- to make it easier to run bulk queries.
	server_name_and_key_id TEXT NOT NULL,
	-- When the key is valid until as a millisecond timestamp.
	-- 0 if this is an expired key (in which case expired_ts will be non-zero)
	valid_until_ts BIGINT NOT NULL,
	-- When the key expired as a millisecond timestamp.
	-- 0 if this is an active key (in which case valid_until_ts will be non-zero)
	expired_ts BIGINT NOT NULL,
	-- The base64-encoded public key.
	server_key TEXT NOT NULL,
	CONSTRAINT keydb_server_keys_unique UNIQUE (server_name, server_key_id)
);

CREATE INDEX IF NOT EXISTS keydb_server_name_and_key_id ON keydb_server_keys (server_name_and_key_id);
`

const bulkSelectServerSigningKeysSQL = "" +
	"SELECT server_name, server_key_id, valid_until_ts, expired_ts, " +
	"   server_key FROM keydb_server_keys" +
	" WHERE server_name_and_key_id = ANY($1)"

const upsertServerSigningKeysSQL = "" +
	"INSERT INTO keydb_server_keys (server_name, server_key_id," +
	" server_name_and_key_id, valid_until_ts, expired_ts, server_key)" +
	" VALUES ($1, $2, $3, $4, $5, $6)" +
	" ON CONFLICT ON CONSTRAINT keydb_server_keys_unique" +
	" DO UPDATE SET valid_until_ts = $4, expired_ts = $5, server_key = $6"

type serverSigningKeyStatements struct {
	bulkSelectServerKeysStmt *sql.Stmt
	upsertServerKeysStmt     *sql.Stmt
}

func NewPostgresServerSigningKeysTable(ctx context.Context, db *sql.DB) (s *serverSigningKeyStatements, err error) {
	s = &serverSigningKeyStatements{}
	_, err = db.Exec(serverSigningKeysSchema)
	if err != nil {
		return
	}
	return s, sqlutil.StatementList{
		{&s.bulkSelectServerKeysStmt, bulkSelectServerSigningKeysSQL},
		{&s.upsertServerKeysStmt, upsertServerSigningKeysSQL},
	}.Prepare(db)
}

func (s *serverSigningKeyStatements) BulkSelectServerKeys(
	ctx context.Context, txn *sql.Tx,
	requests map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp,
) (map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult, error) {
	var nameAndKeyIDs []string
	for request := range requests {
		nameAndKeyIDs = append(nameAndKeyIDs, nameAndKeyID(request))
	}
	stmt := s.bulkSelectServerKeysStmt
	rows, err := stmt.QueryContext(ctx, pq.StringArray(nameAndKeyIDs))
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectServerKeys: rows.close() failed")
	results := map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult{}

	var serverName string
	var keyID string
	var key string
	var validUntilTS int64
	var expiredTS int64
	var vk gomatrixserverlib.VerifyKey
	for rows.Next() {
		if err = rows.Scan(&serverName, &keyID, &validUntilTS, &expiredTS, &key); err != nil {
			return nil, err
		}
		r := gomatrixserverlib.PublicKeyLookupRequest{
			ServerName: spec.ServerName(serverName),
			KeyID:      gomatrixserverlib.KeyID(keyID),
		}
		err = vk.Key.Decode(key)
		if err != nil {
			return nil, err
		}
		results[r] = gomatrixserverlib.PublicKeyLookupResult{
			VerifyKey:    vk,
			ValidUntilTS: spec.Timestamp(validUntilTS),
			ExpiredTS:    spec.Timestamp(expiredTS),
		}
	}
	return results, rows.Err()
}

func (s *serverSigningKeyStatements) UpsertServerKeys(
	ctx context.Context, txn *sql.Tx,
	request gomatrixserverlib.PublicKeyLookupRequest,
	key gomatrixserverlib.PublicKeyLookupResult,
) error {
	stmt := sqlutil.TxStmt(txn, s.upsertServerKeysStmt)
	_, err := stmt.ExecContext(
		ctx,
		string(request.ServerName),
		string(request.KeyID),
		nameAndKeyID(request),
		key.ValidUntilTS,
		key.ExpiredTS,
		key.Key.Encode(),
	)
	return err
}

func nameAndKeyID(request gomatrixserverlib.PublicKeyLookupRequest) string {
	return string(request.ServerName) + "\x1F" + string(request.KeyID)
}
