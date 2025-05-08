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

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/pitabwire/frame"
)

// Schema for the notary server keys JSON table
const notaryServerKeysJSONSchema = `
CREATE SEQUENCE IF NOT EXISTS federationsender_notary_server_keys_json_pkey;
CREATE TABLE IF NOT EXISTS federationsender_notary_server_keys_json (
    notary_id BIGINT PRIMARY KEY NOT NULL DEFAULT nextval('federationsender_notary_server_keys_json_pkey'),
	response_json TEXT NOT NULL,
	server_name TEXT NOT NULL,
	valid_until BIGINT NOT NULL
);
`

// Schema revert for the notary server keys JSON table
const notaryServerKeysJSONSchemaRevert = `
DROP TABLE IF EXISTS federationsender_notary_server_keys_json;
DROP SEQUENCE IF EXISTS federationsender_notary_server_keys_json_pkey;
`

// SQL for inserting server keys JSON
const insertServerKeysJSONSQL = "" +
	"INSERT INTO federationsender_notary_server_keys_json (response_json, server_name, valid_until) VALUES ($1, $2, $3)" +
	" RETURNING notary_id"

// notaryServerKeysTable stores the JSON responses from server key requests
type notaryServerKeysTable struct {
	cm *sqlutil.Connections
	// SQL query string fields, initialized at construction
	insertServerKeysJSONSQL string
}

// NewPostgresNotaryServerKeysTable creates a new postgres notary server keys table
func NewPostgresNotaryServerKeysTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationNotaryServerKeysJSON, error) {
	s := &notaryServerKeysTable{
		cm:                      cm,
		insertServerKeysJSONSQL: insertServerKeysJSONSQL,
	}

	// Perform schema migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "federationapi_notary_server_keys_json_table_schema_001",
		Patch:       notaryServerKeysJSONSchema,
		RevertPatch: notaryServerKeysJSONSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertJSONResponse inserts a JSON server key response
func (s *notaryServerKeysTable) InsertJSONResponse(
	ctx context.Context, keyQueryResponseJSON gomatrixserverlib.ServerKeys, serverName spec.ServerName, validUntil spec.Timestamp,
) (tables.NotaryID, error) {
	var notaryID tables.NotaryID

	db := s.cm.Connection(ctx, false)
	result := db.Raw(s.insertServerKeysJSONSQL, string(keyQueryResponseJSON.Raw), serverName, validUntil).Row()
	err := result.Scan(&notaryID)
	return notaryID, err
}
