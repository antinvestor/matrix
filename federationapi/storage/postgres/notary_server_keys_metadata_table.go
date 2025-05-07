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
	"encoding/json"
	"errors"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

const notaryServerKeysMetadataSchema = `
CREATE TABLE IF NOT EXISTS federationsender_notary_server_keys_metadata (
    notary_id BIGINT NOT NULL,
	server_name TEXT NOT NULL,
	key_id TEXT NOT NULL,
	UNIQUE (server_name, key_id)
);
`

// notaryServerKeysMetadataTable contains the postgres-specific implementation
type notaryServerKeysMetadataTable struct {
	cm *sqlutil.Connections

	// upsertServerKeysSQL inserts or updates a server key entry
	upsertServerKeysSQL string

	// selectNotaryKeyMetadataSQL retrieves notary ID and valid until for a given server name and key ID
	// JOINs with the json table
	selectNotaryKeyMetadataSQL string

	// selectNotaryKeyResponsesSQL retrieves the response with the highest valid_until value
	// JOINs with the json table
	selectNotaryKeyResponsesSQL string

	// selectNotaryKeyResponsesWithKeyIDsSQL retrieves responses with specific key IDs
	// JOINs with the json table
	selectNotaryKeyResponsesWithKeyIDsSQL string

	// deleteUnusedServerKeysJSONSQL removes unreferenced JSON responses
	// JOINs with the metadata table
	deleteUnusedServerKeysJSONSQL string
}

// NewPostgresNotaryServerKeysMetadataTable creates a new postgres notary server keys metadata table
func NewPostgresNotaryServerKeysMetadataTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationNotaryServerKeysMetadata, error) {
	// Initialize schema using GORM
	gormDB := cm.Connection(ctx, false)
	if err := gormDB.Exec(notaryServerKeysMetadataSchema).Error; err != nil {
		return nil, err
	}

	s := &notaryServerKeysMetadataTable{
		cm: cm,
		upsertServerKeysSQL: "INSERT INTO federationsender_notary_server_keys_metadata (notary_id, server_name, key_id) VALUES ($1, $2, $3) ON CONFLICT (server_name, key_id) DO UPDATE SET notary_id = $1",
		
		selectNotaryKeyMetadataSQL: `
			SELECT federationsender_notary_server_keys_metadata.notary_id, valid_until FROM federationsender_notary_server_keys_json
			JOIN federationsender_notary_server_keys_metadata ON
			federationsender_notary_server_keys_metadata.notary_id = federationsender_notary_server_keys_json.notary_id
			WHERE federationsender_notary_server_keys_metadata.server_name = $1 AND federationsender_notary_server_keys_metadata.key_id = $2
		`,
		
		selectNotaryKeyResponsesSQL: `
			SELECT response_json FROM federationsender_notary_server_keys_json
			WHERE server_name = $1 AND valid_until = (
				SELECT MAX(valid_until) FROM federationsender_notary_server_keys_json WHERE server_name = $1
			)
		`,
		
		selectNotaryKeyResponsesWithKeyIDsSQL: `
			SELECT response_json FROM federationsender_notary_server_keys_json
			JOIN federationsender_notary_server_keys_metadata ON
			federationsender_notary_server_keys_metadata.notary_id = federationsender_notary_server_keys_json.notary_id
			WHERE federationsender_notary_server_keys_json.server_name = $1 AND federationsender_notary_server_keys_metadata.key_id = ANY ($2)
			GROUP BY federationsender_notary_server_keys_json.notary_id
		`,
		
		deleteUnusedServerKeysJSONSQL: `
			DELETE FROM federationsender_notary_server_keys_json WHERE federationsender_notary_server_keys_json.notary_id NOT IN (
				SELECT DISTINCT notary_id FROM federationsender_notary_server_keys_metadata
			)
		`,
	}

	return s, nil
}

// UpsertKey inserts or updates a server key entry
func (s *notaryServerKeysMetadataTable) UpsertKey(
	ctx context.Context, serverName spec.ServerName, keyID gomatrixserverlib.KeyID, newNotaryID tables.NotaryID, newValidUntil spec.Timestamp,
) (tables.NotaryID, error) {
	db := s.cm.Connection(ctx, false)

	notaryID := newNotaryID
	// see if the existing notary ID a) exists, b) has a longer valid_until
	var existingNotaryID tables.NotaryID
	var existingValidUntil spec.Timestamp

	row := db.Raw(s.selectNotaryKeyMetadataSQL, serverName, keyID).Row()
	err := row.Scan(&existingNotaryID, &existingValidUntil)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, err
		}
	}

	if existingValidUntil.Time().After(newValidUntil.Time()) {
		// the existing valid_until is valid longer, so use that.
		return existingNotaryID, nil
	}

	// overwrite the notary_id for this (server_name, key_id) tuple
	err = db.Exec(s.upsertServerKeysSQL, notaryID, serverName, keyID).Error
	return notaryID, err
}

// SelectKeys retrieves server keys
func (s *notaryServerKeysMetadataTable) SelectKeys(
	ctx context.Context, serverName spec.ServerName, keyIDs []gomatrixserverlib.KeyID,
) ([]gomatrixserverlib.ServerKeys, error) {
	db := s.cm.Connection(ctx, true)

	var rows *gorm.Rows
	var err error

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

// DeleteOldJSONResponses removes unreferenced JSON responses
func (s *notaryServerKeysMetadataTable) DeleteOldJSONResponses(ctx context.Context) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteUnusedServerKeysJSONSQL).Error
}
