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
	"time"

	"golang.org/x/oauth2"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/pitabwire/util"
)

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

// SQL query constants for login token operations
const (
	// insertLoginTokenSQL inserts a new login token
	insertLoginTokenSQL = "INSERT INTO userapi_login_tokens(token, token_expires_at, user_id, extra_data) VALUES ($1, $2, $3, $4)"

	// deleteLoginTokenSQL deletes a login token by token value or expiration
	deleteLoginTokenSQL = "DELETE FROM userapi_login_tokens WHERE token = $1 OR token_expires_at <= $2"

	// selectLoginTokenSQL selects a login token by token value that hasn't expired
	selectLoginTokenSQL = "SELECT user_id, extra_data FROM userapi_login_tokens WHERE token = $1 AND token_expires_at > $2"
)

type loginTokenTable struct {
	cm *sqlutil.Connections
	
	insertTokenStmt string
	deleteTokenStmt string
	selectTokenStmt string
}

func NewPostgresLoginTokenTable(ctx context.Context, cm *sqlutil.Connections) (tables.LoginTokenTable, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(loginTokenSchema).Error; err != nil {
		return nil, err
	}

	// Initialize table with SQL statements
	t := &loginTokenTable{
		cm:              cm,
		insertTokenStmt: insertLoginTokenSQL,
		deleteTokenStmt: deleteLoginTokenSQL,
		selectTokenStmt: selectLoginTokenSQL,
	}

	return t, nil
}

// InsertLoginToken insert adds an already generated token to the database.
func (t *loginTokenTable) InsertLoginToken(ctx context.Context, metadata *api.LoginTokenMetadata, data *api.LoginTokenData) error {
	db := t.cm.Connection(ctx, false)

	extraData, err := json.Marshal(data.SSOToken)
	if err != nil {
		return err
	}

	return db.Exec(t.insertTokenStmt, metadata.Token, metadata.Expiration.UTC(), data.UserID, extraData).Error
}

// DeleteLoginToken removes the named token.
//
// As a simple way to garbage-collect stale tokens, we also remove all expired tokens.
// The userapi_login_tokens_expiration_idx index should make that efficient.
func (t *loginTokenTable) DeleteLoginToken(ctx context.Context, token string) error {
	db := t.cm.Connection(ctx, false)

	// Delete this specific token and all expired tokens
	return db.Exec(t.deleteTokenStmt, token, time.Now().UTC()).Error
}

// SelectLoginToken returns the data associated with the given token. May return sql.ErrNoRows.
func (t *loginTokenTable) SelectLoginToken(ctx context.Context, token string) (*api.LoginTokenData, error) {
	db := t.cm.Connection(ctx, true)

	var userID string
	var extraDataBytes []byte

	row := db.Raw(t.selectTokenStmt, token, time.Now().UTC()).Row()
	err := row.Scan(&userID, &extraDataBytes)
	if err != nil {
		return nil, err
	}

	var oauthToken oauth2.Token
	if len(extraDataBytes) > 0 {
		if err = json.Unmarshal(extraDataBytes, &oauthToken); err != nil {
			return nil, err
		}
	}

	return &api.LoginTokenData{
		UserID:   userID,
		SSOToken: &oauthToken,
	}, nil
}
