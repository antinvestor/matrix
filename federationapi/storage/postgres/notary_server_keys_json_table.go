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
	"gorm.io/gorm"
)

const notaryServerKeysJSONSchema = `
CREATE SEQUENCE IF NOT EXISTS federationsender_notary_server_keys_json_pkey;
CREATE TABLE IF NOT EXISTS federationsender_notary_server_keys_json (
    notary_id BIGINT PRIMARY KEY NOT NULL DEFAULT nextval('federationsender_notary_server_keys_json_pkey'),
	response_json TEXT NOT NULL,
	server_name TEXT NOT NULL,
	valid_until BIGINT NOT NULL
);
`

// notaryServerKeysJSONTable contains the postgres-specific implementation
type notaryServerKeysJSONTable struct {
	cm *sqlutil.Connections
	
	// insertServerKeysJSONSQL inserts a server key response JSON and returns the created notary ID
	insertServerKeysJSONSQL string
}

// NewPostgresNotaryServerKeysTable creates a new postgres notary server keys table
func NewPostgresNotaryServerKeysTable(ctx context.Context, cm *sqlutil.Connections) (tables.FederationNotaryServerKeysJSON, error) {
	// Initialize schema using GORM
	gormDB := cm.Connection(ctx, false)
	if err := gormDB.Exec(notaryServerKeysJSONSchema).Error; err != nil {
		return nil, err
	}

	s := &notaryServerKeysJSONTable{
		cm: cm,
		insertServerKeysJSONSQL: "INSERT INTO federationsender_notary_server_keys_json (response_json, server_name, valid_until) VALUES ($1, $2, $3) RETURNING notary_id",
	}

	return s, nil
}

// InsertJSONResponse stores a server key response and returns the notary ID
func (s *notaryServerKeysJSONTable) InsertJSONResponse(
	ctx context.Context, keyQueryResponseJSON gomatrixserverlib.ServerKeys, serverName spec.ServerName, validUntil spec.Timestamp,
) (tables.NotaryID, error) {
	db := s.cm.Connection(ctx, false)
	
	var notaryID tables.NotaryID
	row := db.Raw(
		s.insertServerKeysJSONSQL, 
		string(keyQueryResponseJSON.Raw), 
		serverName, 
		validUntil,
	).Row()
	
	err := row.Scan(&notaryID)
	return notaryID, err
}
