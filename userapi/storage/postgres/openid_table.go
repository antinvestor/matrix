package postgres

import (
	"context"
	"fmt"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

const openIDTokenSchema = `
-- Stores data about openid tokens issued for accounts.
CREATE TABLE IF NOT EXISTS userapi_openid_tokens (
	-- The value of the token issued to a user
	token TEXT NOT NULL PRIMARY KEY,
    -- The Global user ID for this account
	localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
	-- When the token expires, as a unix timestamp (ms resolution).
	token_expires_at_ms BIGINT NOT NULL
);
`

const openIDTokenSchemaRevert = "DROP TABLE IF EXISTS userapi_openid_tokens CASCADE;"

const insertOpenIDTokenSQL = "" +
	"INSERT INTO userapi_openid_tokens(token, localpart, server_name, token_expires_at_ms) VALUES ($1, $2, $3, $4)"

const selectOpenIDTokenSQL = "" +
	"SELECT localpart, server_name, token_expires_at_ms FROM userapi_openid_tokens WHERE token = $1"

const deleteOpenIDTokenSQL = "" +
	"DELETE FROM userapi_openid_tokens WHERE token = $1"

// openIDTable implements tables.OpenIDTable using GORM and a connection manager.
type openIDTable struct {
	cm *sqlutil.Connections

	insertOpenIDTokenSQL string
	selectOpenIDTokenSQL string
	deleteOpenIDTokenSQL string
}

// NewPostgresOpenIDTable returns a new OpenIDTable using the provided connection manager.
func NewPostgresOpenIDTable(cm *sqlutil.Connections) tables.OpenIDTable {
	return &openIDTable{
		cm:                   cm,
		insertOpenIDTokenSQL: insertOpenIDTokenSQL,
		selectOpenIDTokenSQL: selectOpenIDTokenSQL,
		deleteOpenIDTokenSQL: deleteOpenIDTokenSQL,
	}
}

// InsertOpenIDToken inserts a new OpenID token into the database.
// The token is associated with a user and has an expiration timestamp (ms resolution).
func (t *openIDTable) InsertOpenIDToken(ctx context.Context, token string, userID string, expiresAt int64) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.insertOpenIDTokenSQL, token, userID, expiresAt)
	return result.Error
}

// SelectOpenIDToken retrieves an OpenID token's attributes by the token string.
// Returns the user ID and expiration timestamp if found, or an error if not found.
func (t *openIDTable) SelectOpenIDToken(ctx context.Context, token string) (*api.OpenIDTokenAttributes, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectOpenIDTokenSQL, token).Row()
	var localpart string
	var serverName spec.ServerName
	var expiresAt int64
	if err := row.Scan(&localpart, &serverName, &expiresAt); err != nil {
		return nil, err
	}
	attrs := api.OpenIDTokenAttributes{
		UserID:      fmt.Sprintf("@%s:%s", localpart, serverName),
		ExpiresAtMS: expiresAt,
	}
	return &attrs, nil
}

// DeleteOpenIDToken deletes an OpenID token from the database by token string.
// Returns an error if the operation fails.
func (t *openIDTable) DeleteOpenIDToken(ctx context.Context, token string) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteOpenIDTokenSQL, token)
	return result.Error
}
