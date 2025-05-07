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
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

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

// SQL query constants for OpenID token operations
const (
	// insertOpenIDTokenSQL inserts a new OpenID token
	insertOpenIDTokenSQL = "INSERT INTO userapi_openid_tokens(token, localpart, server_name, token_expires_at_ms) VALUES ($1, $2, $3, $4)"

	// selectOpenIDTokenSQL selects an OpenID token by token value
	selectOpenIDTokenSQL = "SELECT localpart, server_name, token_expires_at_ms FROM userapi_openid_tokens WHERE token = $1"
)

type openIDTable struct {
	cm         *sqlutil.Connections
	serverName spec.ServerName
	
	insertTokenStmt string
	selectTokenStmt string
}

func NewPostgresOpenIDTable(cm *sqlutil.Connections, serverName spec.ServerName) (tables.OpenIDTable, error) {
	// Initialize schema
	db := cm.Connection(context.Background(), false)
	if err := db.Exec(openIDTokenSchema).Error; err != nil {
		return nil, err
	}
	
	// Initialize table with SQL statements
	t := &openIDTable{
		cm: cm,
		insertTokenStmt: insertOpenIDTokenSQL,
		selectTokenStmt: selectOpenIDTokenSQL,
		serverName: serverName,
	}
	
	return t, nil
}

// insertToken inserts a new OpenID Connect token to the DB.
// Returns new token, otherwise returns error if the token already exists.
func (t *openIDTable) InsertOpenIDToken(
	ctx context.Context,
	token, localpart string, serverName spec.ServerName,
	expiresAtMS int64,
) (err error) {
	db := t.cm.Connection(ctx, false)
	
	return db.Exec(t.insertTokenStmt, token, localpart, serverName, expiresAtMS).Error
}

// selectOpenIDTokenAtrributes gets the attributes associated with an OpenID token from the DB
// Returns the existing token's attributes, or err if no token is found
func (t *openIDTable) SelectOpenIDTokenAtrributes(
	ctx context.Context,
	token string,
) (*api.OpenIDTokenAttributes, error) {
	// Get read-only database connection
	db := t.cm.Connection(ctx, true)
	
	var openIDTokenAttrs api.OpenIDTokenAttributes
	var localpart string
	var serverName spec.ServerName
	
	row := db.Raw(t.selectTokenStmt, token).Row()
	err := row.Scan(&localpart, &serverName, &openIDTokenAttrs.ExpiresAtMS)
	
	openIDTokenAttrs.UserID = fmt.Sprintf("@%s:%s", localpart, serverName)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			log.WithError(err).Error("Unable to retrieve token from the db")
		}
		return nil, err
	}

	return &openIDTokenAttrs, nil
}
