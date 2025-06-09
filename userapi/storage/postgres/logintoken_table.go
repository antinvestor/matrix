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
	"encoding/json"
	"time"

	"golang.org/x/oauth2"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/util"
)

// loginTokenSchema defines the schema for the login tokens table.
const loginTokenSchema = `
CREATE TABLE IF NOT EXISTS userapi_login_tokens (
	-- The random value of the token issued to a user
	token TEXT NOT NULL PRIMARY KEY,
	-- When the token expires
	token_expires_at TIMESTAMP NOT NULL,
	
    -- The mxid for this account
	user_id TEXT NOT NULL,
                                                
    -- Stores any extra data that may be required for login
	extra_data JSONB
);

-- This index allows efficient garbage collection of expired tokens.
CREATE INDEX IF NOT EXISTS userapi_login_tokens_expiration_idx ON userapi_login_tokens(token_expires_at);
`

// loginTokenSchemaRevert defines how to revert the login tokens table schema.
const loginTokenSchemaRevert = `
DROP TABLE IF EXISTS userapi_login_tokens;
`

// insertLoginTokenSQL is used to insert a new login token into the database.
const insertLoginTokenSQL = "" +
	"INSERT INTO userapi_login_tokens(token, token_expires_at, user_id, extra_data) VALUES ($1, $2, $3, $4)"

// deleteLoginTokenSQL is used to delete a login token from the database.
const deleteLoginTokenSQL = "" +
	"DELETE FROM userapi_login_tokens WHERE token = $1 OR token_expires_at <= $2"

// selectLoginTokenSQL is used to retrieve a login token from the database.
const selectLoginTokenSQL = "" +
	"SELECT user_id, extra_data FROM userapi_login_tokens WHERE token = $1 AND token_expires_at > $2"

type loginTokenTable struct {
	cm                  sqlutil.ConnectionManager
	insertLoginTokenSQL string
	deleteLoginTokenSQL string
	selectLoginTokenSQL string
}

// NewPostgresLoginTokenTable creates a new postgres login token table.
func NewPostgresLoginTokenTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.LoginTokenTable, error) {
	t := &loginTokenTable{
		cm:                  cm,
		insertLoginTokenSQL: insertLoginTokenSQL,
		deleteLoginTokenSQL: deleteLoginTokenSQL,
		selectLoginTokenSQL: selectLoginTokenSQL,
	}

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "userapi_login_tokens_table_schema_001",
		Patch:       loginTokenSchema,
		RevertPatch: loginTokenSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// InsertLoginToken insert adds an already generated token to the database.
func (t *loginTokenTable) InsertLoginToken(ctx context.Context, metadata *api.LoginTokenMetadata, data *api.LoginTokenData) error {
	extraData, err := json.Marshal(data.SSOToken)
	if err != nil {
		return err
	}

	db := t.cm.Connection(ctx, false)
	err = db.Exec(t.insertLoginTokenSQL, metadata.Token, metadata.Expiration.UTC(), data.UserID, extraData).Error
	return err
}

// DeleteLoginToken removes the named token.
//
// As a simple way to garbage-collect stale tokens, we also remove all expired tokens.
// The userapi_login_tokens_expiration_idx index should make that efficient.
func (t *loginTokenTable) DeleteLoginToken(ctx context.Context, token string) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteLoginTokenSQL, token, time.Now().UTC())
	if result.Error != nil {
		return result.Error
	}

	if n := result.RowsAffected; n > 1 {
		util.GetLogger(ctx).WithField("num_deleted", n).Info("Deleted %d login tokens (%d likely additional expired token)", n, n-1)
	}
	return nil
}

// SelectLoginToken returns the data associated with the given token. May return sql.ErrNoRows.
func (t *loginTokenTable) SelectLoginToken(ctx context.Context, token string) (*api.LoginTokenData, error) {
	var (
		data      api.LoginTokenData
		extraData []byte
	)

	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectLoginTokenSQL, token, time.Now().UTC()).Row()
	err := row.Scan(&data.UserID, &extraData)
	if err != nil {
		return nil, err
	}

	if len(extraData) > 0 && string(extraData) != "null" {
		ssoToken := &oauth2.Token{}
		err = json.Unmarshal(extraData, ssoToken)
		if err != nil {
			return nil, err
		}
		data.SSOToken = ssoToken
	}

	return &data, nil
}
