package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/pitabwire/frame"
	log "github.com/sirupsen/logrus"
)

// openIDTokenSchema defines the schema for the openid tokens table.
const openIDTokenSchema = `
-- Stores data about openid tokens issued for accounts.
CREATE TABLE IF NOT EXISTS userapi_openid_tokens (
	-- The value of the token issued to a user
	token TEXT NOT NULL PRIMARY KEY,
    -- The Matrix user ID for this account
	localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
	-- When the token expires, as a unix timestamp (ms resolution).
	token_expires_at_ms BIGINT NOT NULL
);
`

// openIDTokenSchemaRevert defines how to revert the openid tokens table schema.
const openIDTokenSchemaRevert = `
DROP TABLE IF EXISTS userapi_openid_tokens;
`

// insertOpenIDTokenSQL is used to insert a new token into the database.
const insertOpenIDTokenSQL = "" +
	"INSERT INTO userapi_openid_tokens(token, localpart, server_name, token_expires_at_ms) VALUES ($1, $2, $3, $4)"

// selectOpenIDTokenSQL is used to retrieve a token from the database.
const selectOpenIDTokenSQL = "" +
	"SELECT localpart, server_name, token_expires_at_ms FROM userapi_openid_tokens WHERE token = $1"

type openIDTable struct {
	cm               *sqlutil.Connections
	serverName       spec.ServerName
	insertTokenSQL   string
	selectTokenSQL   string
}

// NewPostgresOpenIDTable creates a new postgres openid table.
func NewPostgresOpenIDTable(ctx context.Context, cm *sqlutil.Connections, serverName spec.ServerName) (tables.OpenIDTable, error) {
	t := &openIDTable{
		cm:               cm,
		serverName:       serverName,
		insertTokenSQL:   insertOpenIDTokenSQL,
		selectTokenSQL:   selectOpenIDTokenSQL,
	}

	// Perform schema migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "userapi_openid_tokens_table_schema_001",
		Patch:       openIDTokenSchema,
		RevertPatch: openIDTokenSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// InsertOpenIDToken inserts a new OpenID Connect token to the DB.
// Returns new token, otherwise returns error if the token already exists.
func (t *openIDTable) InsertOpenIDToken(
	ctx context.Context,
	token, localpart string, serverName spec.ServerName,
	expiresAtMS int64,
) (err error) {
	db := t.cm.Connection(ctx, false)
	err = db.Exec(t.insertTokenSQL, token, localpart, serverName, expiresAtMS).Error
	return
}

// SelectOpenIDTokenAtrributes gets the attributes associated with an OpenID token from the DB
// Returns the existing token's attributes, or err if no token is found
func (t *openIDTable) SelectOpenIDTokenAtrributes(
	ctx context.Context,
	token string,
) (*api.OpenIDTokenAttributes, error) {
	var openIDTokenAttrs api.OpenIDTokenAttributes
	var localpart string
	var serverName spec.ServerName
	
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectTokenSQL, token).Row()
	err := row.Scan(&localpart, &serverName, &openIDTokenAttrs.ExpiresAtMS)
	
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			log.WithError(err).Error("Unable to retrieve token from the db")
		}
		return nil, err
	}

	openIDTokenAttrs.UserID = fmt.Sprintf("@%s:%s", localpart, serverName)
	return &openIDTokenAttrs, nil
}
