// Copyright 2021 The Global.org Foundation C.I.C.
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
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/lib/pq"
)

const notaryServerKeysMetadataSchema = `
CREATE TABLE IF NOT EXISTS federationsender_notary_server_keys_metadata (
    notary_id BIGINT NOT NULL,
	server_name TEXT NOT NULL,
	key_id TEXT NOT NULL,
	UNIQUE (server_name, key_id)
);
`

const notaryServerKeysMetadataSchemaRevert = `DROP TABLE IF EXISTS federationsender_notary_server_keys_metadata;`

// upsertServerKeysSQL is the SQL query to insert or update a (server_name, key_id) tuple.
const upsertServerKeysSQL = "" +
	"INSERT INTO federationsender_notary_server_keys_metadata (notary_id, server_name, key_id) VALUES ($1, $2, $3)" +
	" ON CONFLICT (server_name, key_id) DO UPDATE SET notary_id = $1"

// selectNotaryKeyMetadataSQL is the SQL query to select the existing notary ID and valid until for a given (server_name, key_id).
const selectNotaryKeyMetadataSQL = `
	SELECT federationsender_notary_server_keys_metadata.notary_id, valid_until FROM federationsender_notary_server_keys_json
	JOIN federationsender_notary_server_keys_metadata ON
	federationsender_notary_server_keys_metadata.notary_id = federationsender_notary_server_keys_json.notary_id
	WHERE federationsender_notary_server_keys_metadata.server_name = $1 AND federationsender_notary_server_keys_metadata.key_id = $2
`

// selectNotaryKeyResponsesSQL is the SQL query to select the response which has the highest valid_until value.
const selectNotaryKeyResponsesSQL = `
	SELECT response_json FROM federationsender_notary_server_keys_json
	WHERE server_name = $1 AND valid_until = (
		SELECT MAX(valid_until) FROM federationsender_notary_server_keys_json WHERE server_name = $1
	)
`

// selectNotaryKeyResponsesWithKeyIDsSQL is the SQL query to select the responses which have the given key IDs.
const selectNotaryKeyResponsesWithKeyIDsSQL = `
	SELECT response_json FROM federationsender_notary_server_keys_json
	JOIN federationsender_notary_server_keys_metadata ON
	federationsender_notary_server_keys_metadata.notary_id = federationsender_notary_server_keys_json.notary_id
	WHERE federationsender_notary_server_keys_json.server_name = $1 AND federationsender_notary_server_keys_metadata.key_id = ANY ($2)
	GROUP BY federationsender_notary_server_keys_json.notary_id
`

// deleteUnusedServerKeysJSONSQL is the SQL query to delete all responses which are not referenced in FederationNotaryServerKeysMetadata.
const deleteUnusedServerKeysJSONSQL = `
	DELETE FROM federationsender_notary_server_keys_json WHERE federationsender_notary_server_keys_json.notary_id NOT IN (
		SELECT DISTINCT notary_id FROM federationsender_notary_server_keys_metadata
	)
`

// notaryServerKeysMetadataTable provides methods for notary server keys metadata operations using GORM.
type notaryServerKeysMetadataTable struct {
	cm                              *sqlutil.Connections // Connection manager for database access
	UpsertSQL                       string
	SelectKeyMetadataSQL            string
	SelectKeyResponsesSQL           string
	SelectKeyResponsesWithKeyIDsSQL string
	DeleteUnusedServerKeysJSONSQL   string
}

// NewPostgresNotaryServerKeysMetadataTable initializes a notaryServerKeysMetadataTable with SQL constants and a connection manager
func NewPostgresNotaryServerKeysMetadataTable(cm *sqlutil.Connections) tables.FederationNotaryServerKeysMetadata {
	return &notaryServerKeysMetadataTable{
		cm:                              cm,
		UpsertSQL:                       upsertServerKeysSQL,
		SelectKeyMetadataSQL:            selectNotaryKeyMetadataSQL,
		SelectKeyResponsesSQL:           selectNotaryKeyResponsesSQL,
		SelectKeyResponsesWithKeyIDsSQL: selectNotaryKeyResponsesWithKeyIDsSQL,
		DeleteUnusedServerKeysJSONSQL:   deleteUnusedServerKeysJSONSQL,
	}
}

// UpsertKey updates or inserts a (server_name, key_id) tuple, pointing it via NotaryID at the response which has the longest valid_until_ts
func (t *notaryServerKeysMetadataTable) UpsertKey(ctx context.Context, serverName spec.ServerName, keyID gomatrixserverlib.KeyID, newNotaryID tables.NotaryID, newValidUntil spec.Timestamp) (tables.NotaryID, error) {
	db := t.cm.Connection(ctx, false)
	// Check if the current valid_until is newer than the new one; if so, don't update
	var existingNotaryID tables.NotaryID
	var existingValidUntil spec.Timestamp
	row := db.Raw(t.SelectKeyMetadataSQL, serverName, keyID).Row()
	err := row.Scan(&existingNotaryID, &existingValidUntil)
	if err == nil && existingValidUntil > newValidUntil {
		return existingNotaryID, nil
	}
	if err := db.Exec(t.UpsertSQL, newNotaryID, serverName, keyID).Error; err != nil {
		return 0, err
	}
	return newNotaryID, nil
}

// SelectKeys returns the signed JSON objects which contain the given key IDs.
func (t *notaryServerKeysMetadataTable) SelectKeys(ctx context.Context, serverName spec.ServerName, keyIDs []gomatrixserverlib.KeyID) ([]gomatrixserverlib.ServerKeys, error) {
	db := t.cm.Connection(ctx, true)
	var rows *sql.Rows
	var err error
	if len(keyIDs) == 0 {
		rows, err = db.Raw(t.SelectKeyResponsesSQL, serverName).Rows()
	} else {
		rows, err = db.Raw(t.SelectKeyResponsesWithKeyIDsSQL, serverName, pq.Array(keyIDs)).Rows()
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []gomatrixserverlib.ServerKeys
	for rows.Next() {
		var rawJSON string
		if err := rows.Scan(&rawJSON); err != nil {
			return nil, err
		}
		var keys gomatrixserverlib.ServerKeys
		if err := json.Unmarshal([]byte(rawJSON), &keys); err != nil {
			return nil, err
		}
		results = append(results, keys)
	}
	return results, rows.Err()
}

// DeleteOldJSONResponses removes all responses which are not referenced in FederationNotaryServerKeysMetadata
func (t *notaryServerKeysMetadataTable) DeleteOldJSONResponses(ctx context.Context) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.DeleteUnusedServerKeysJSONSQL).Error
}
