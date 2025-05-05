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
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal/sqlutil"
)

const notaryServerKeysJSONSchema = `
CREATE SEQUENCE IF NOT EXISTS federationsender_notary_server_keys_json_pkey;
CREATE TABLE IF NOT EXISTS federationsender_notary_server_keys_json (
    notary_id BIGINT PRIMARY KEY NOT NULL DEFAULT nextval('federationsender_notary_server_keys_json_pkey'),
    response_json TEXT NOT NULL,    -- The raw JSON response from the server
    server_name TEXT NOT NULL,      -- The server this key response is for
    valid_until BIGINT NOT NULL     -- The timestamp until which this key is valid
);
`

const notaryServerKeysJSONSchemaRevert = `DROP TABLE IF EXISTS federationsender_notary_server_keys_json; DROP SEQUENCE IF EXISTS federationsender_notary_server_keys_json_pkey;`

// SQL queries for notary server keys JSON operations
const (
	// Insert a new server keys JSON response
	insertServerKeysJSONSQL = "INSERT INTO federationsender_notary_server_keys_json (response_json, server_name, valid_until) VALUES ($1, $2, $3) RETURNING notary_id"
)

// notaryServerKeysJSONTable provides methods for notary server keys JSON operations using GORM.
type notaryServerKeysJSONTable struct {
	cm        *sqlutil.Connections // Connection manager for database access
	insertSQL string
}

// NewPostgresNotaryServerKeysJSONTable initializes a notaryServerKeysJSONTable with SQL constants and a connection manager
func NewPostgresNotaryServerKeysJSONTable(cm *sqlutil.Connections) tables.FederationNotaryServerKeysJSON {
	return &notaryServerKeysJSONTable{
		cm:        cm,
		insertSQL: insertServerKeysJSONSQL,
	}
}

// InsertJSONResponse implements tables.FederationNotaryServerKeysJSON
func (t *notaryServerKeysJSONTable) InsertJSONResponse(
	ctx context.Context,
	keyQueryResponseJSON gomatrixserverlib.ServerKeys,
	serverName spec.ServerName,
	validUntil spec.Timestamp,
) (tables.NotaryID, error) {
	db := t.cm.Connection(ctx, false)
	var notaryID tables.NotaryID
	err := db.Raw(t.insertSQL, string(keyQueryResponseJSON.Raw), serverName, validUntil).Row().Scan(&notaryID)
	return notaryID, err
}
