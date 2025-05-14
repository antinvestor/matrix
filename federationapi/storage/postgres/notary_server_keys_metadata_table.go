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
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
	"gorm.io/gorm"
)

// Schema for the notary server keys metadata table
const notaryServerKeysMetadataSchema = `
CREATE TABLE IF NOT EXISTS federationsender_notary_server_keys_metadata (
    notary_id BIGINT NOT NULL,
	server_name TEXT NOT NULL,
	key_id TEXT NOT NULL,
	UNIQUE (server_name, key_id)
);
`

// Schema revert for the notary server keys metadata table
const notaryServerKeysMetadataSchemaRevert = `
DROP TABLE IF EXISTS federationsender_notary_server_keys_metadata;
`

// SQL for upserting server keys metadata
const upsertServerKeysSQL = "" +
	"INSERT INTO federationsender_notary_server_keys_metadata (notary_id, server_name, key_id) VALUES ($1, $2, $3)" +
	" ON CONFLICT (server_name, key_id) DO UPDATE SET notary_id = $1"

// SQL for selecting notary key metadata
// for a given (server_name, key_id), find the existing notary ID and valid until. Used to check if we will replace it
// JOINs with the json table
const selectNotaryKeyMetadataSQL = `
	SELECT federationsender_notary_server_keys_metadata.notary_id, valid_until FROM federationsender_notary_server_keys_json
	JOIN federationsender_notary_server_keys_metadata ON
	federationsender_notary_server_keys_metadata.notary_id = federationsender_notary_server_keys_json.notary_id
	WHERE federationsender_notary_server_keys_metadata.server_name = $1 AND federationsender_notary_server_keys_metadata.key_id = $2
`

// SQL for selecting notary key responses
// select the response which has the highest valid_until value
// JOINs with the json table
const selectNotaryKeyResponsesSQL = `
	SELECT response_json FROM federationsender_notary_server_keys_json
	WHERE server_name = $1 AND valid_until = (
		SELECT MAX(valid_until) FROM federationsender_notary_server_keys_json WHERE server_name = $1
	)
`

// SQL for selecting notary key responses with key IDs
// select the responses which have the given key IDs
// JOINs with the json table
const selectNotaryKeyResponsesWithKeyIDsSQL = `
	SELECT response_json FROM federationsender_notary_server_keys_json
	JOIN federationsender_notary_server_keys_metadata ON
	federationsender_notary_server_keys_metadata.notary_id = federationsender_notary_server_keys_json.notary_id
	WHERE federationsender_notary_server_keys_json.server_name = $1 AND federationsender_notary_server_keys_metadata.key_id = ANY ($2)
	GROUP BY federationsender_notary_server_keys_json.notary_id
`

// SQL for deleting unused server keys JSON
// JOINs with the metadata table
const deleteUnusedServerKeysJSONSQL = `
	DELETE FROM federationsender_notary_server_keys_json WHERE federationsender_notary_server_keys_json.notary_id NOT IN (
		SELECT DISTINCT notary_id FROM federationsender_notary_server_keys_metadata
	)
`

// notaryServerKeysMetadataTable stores the metadata for notary server keys
type notaryServerKeysMetadataTable struct {
	cm sqlutil.ConnectionManager
	// SQL query string fields, initialized at construction
	upsertServerKeysSQL                   string
	selectNotaryKeyResponsesSQL           string
	selectNotaryKeyResponsesWithKeyIDsSQL string
	selectNotaryKeyMetadataSQL            string
	deleteUnusedServerKeysJSONSQL         string
}

// NewPostgresNotaryServerKeysMetadataTable creates a new postgres notary server keys metadata table
func NewPostgresNotaryServerKeysMetadataTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.FederationNotaryServerKeysMetadata, error) {
	s := &notaryServerKeysMetadataTable{
		cm:                                    cm,
		upsertServerKeysSQL:                   upsertServerKeysSQL,
		selectNotaryKeyResponsesSQL:           selectNotaryKeyResponsesSQL,
		selectNotaryKeyResponsesWithKeyIDsSQL: selectNotaryKeyResponsesWithKeyIDsSQL,
		selectNotaryKeyMetadataSQL:            selectNotaryKeyMetadataSQL,
		deleteUnusedServerKeysJSONSQL:         deleteUnusedServerKeysJSONSQL,
	}

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "federationapi_notary_server_keys_metadata_table_schema_001",
		Patch:       notaryServerKeysMetadataSchema,
		RevertPatch: notaryServerKeysMetadataSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// UpsertKey upserts a key into the metadata table
func (s *notaryServerKeysMetadataTable) UpsertKey(
	ctx context.Context, serverName spec.ServerName, keyID gomatrixserverlib.KeyID, newNotaryID tables.NotaryID, newValidUntil spec.Timestamp,
) (tables.NotaryID, error) {
	notaryID := newNotaryID
	// see if the existing notary ID a) exists, b) has a longer valid_until
	var existingNotaryID tables.NotaryID
	var existingValidUntil spec.Timestamp

	db := s.cm.Connection(ctx, true)
	row := db.Raw(s.selectNotaryKeyMetadataSQL, serverName, keyID).Row()
	err := row.Scan(&existingNotaryID, &existingValidUntil)
	if err != nil {
		if !sqlutil.ErrorIsNoRows(err) {
			return 0, err
		}
	}

	if existingValidUntil.Time().After(newValidUntil.Time()) {
		// the existing valid_until is valid longer, so use that.
		return existingNotaryID, nil
	}

	// overwrite the notary_id for this (server_name, key_id) tuple
	db = s.cm.Connection(ctx, false)
	return notaryID, db.Exec(s.upsertServerKeysSQL, notaryID, serverName, keyID).Error
}

// SelectKeys selects keys from the metadata table
func (s *notaryServerKeysMetadataTable) SelectKeys(ctx context.Context, serverName spec.ServerName, keyIDs []gomatrixserverlib.KeyID) ([]gomatrixserverlib.ServerKeys, error) {
	var rows *sql.Rows
	var err error

	db := s.cm.Connection(ctx, true)
	if len(keyIDs) == 0 {
		rows, err = db.Raw(s.selectNotaryKeyResponsesSQL, string(serverName)).Rows()
	} else {
		keyIDstr := make([]string, len(keyIDs))
		for i := range keyIDs {
			keyIDstr[i] = string(keyIDs[i])
		}
		rows, err = db.Raw(s.selectNotaryKeyResponsesWithKeyIDsSQL, string(serverName), pq.StringArray(keyIDstr)).Rows()
	}
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectNotaryKeyResponsesStmt close failed")

	var results []gomatrixserverlib.ServerKeys
	for rows.Next() {
		var sk gomatrixserverlib.ServerKeys
		var raw string
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(raw), &sk); err != nil {
			return nil, err
		}
		results = append(results, sk)
	}
	return results, rows.Err()
}

// DeleteOldJSONResponses deletes old JSON responses
func (s *notaryServerKeysMetadataTable) DeleteOldJSONResponses(ctx context.Context) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteUnusedServerKeysJSONSQL).Error
}

// GetDB returns the underlying GORM database connection
func (s *notaryServerKeysMetadataTable) GetDB() *gorm.DB {
	return s.cm.Connection(context.Background(), true)
}
