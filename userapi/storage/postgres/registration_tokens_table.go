package postgres

import (
	"context"
	"database/sql"
	"errors"
	"github.com/antinvestor/matrix/internal"
	"time"

	"github.com/antinvestor/matrix/clientapi/api"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"golang.org/x/exp/constraints"
)

const registrationTokensSchema = `
CREATE TABLE IF NOT EXISTS userapi_registration_tokens (
	token TEXT PRIMARY KEY,
	pending BIGINT,
	completed BIGINT,
	uses_allowed BIGINT,
	expiry_time BIGINT
);
`

const registrationTokensSchemaRevert = "DROP TABLE IF EXISTS userapi_registration_tokens CASCADE;"

const selectTokenSQL = "" +
	"SELECT token FROM userapi_registration_tokens WHERE token = $1"

const insertTokenSQL = "" +
	"INSERT INTO userapi_registration_tokens (token, uses_allowed, expiry_time, pending, completed) VALUES ($1, $2, $3, $4, $5)"

const listAllTokensSQL = "" +
	"SELECT * FROM userapi_registration_tokens"

const listValidTokensSQL = "" +
	"SELECT * FROM userapi_registration_tokens WHERE" +
	"(uses_allowed > pending + completed OR uses_allowed IS NULL) AND" +
	"(expiry_time > $1 OR expiry_time IS NULL)"

const listInvalidTokensSQL = "" +
	"SELECT * FROM userapi_registration_tokens WHERE" +
	"(uses_allowed <= pending + completed OR expiry_time <= $1)"

const getTokenSQL = "" +
	"SELECT pending, completed, uses_allowed, expiry_time FROM userapi_registration_tokens WHERE token = $1"

const deleteTokenSQL = "" +
	"DELETE FROM userapi_registration_tokens WHERE token = $1"

const updateTokenUsesAllowedAndExpiryTimeSQL = "" +
	"UPDATE userapi_registration_tokens SET uses_allowed = $2, expiry_time = $3 WHERE token = $1"

const updateTokenUsesAllowedSQL = "" +
	"UPDATE userapi_registration_tokens SET uses_allowed = $2 WHERE token = $1"

const updateTokenExpiryTimeSQL = "" +
	"UPDATE userapi_registration_tokens SET expiry_time = $2 WHERE token = $1"

// registrationTokensTable implements tables.RegistrationTokensTable using GORM and a connection manager.
type registrationTokensTable struct {
	cm *sqlutil.Connections

	selectTokenSQL                         string
	insertTokenSQL                         string
	listAllTokensSQL                       string
	listValidTokensSQL                     string
	listInvalidTokensSQL                   string
	getTokenSQL                            string
	deleteTokenSQL                         string
	updateTokenUsesAllowedAndExpiryTimeSQL string
	updateTokenUsesAllowedSQL              string
	updateTokenExpiryTimeSQL               string
}

// NewPostgresRegistrationTokensTable returns a new RegistrationTokensTable using the provided connection manager.
func NewPostgresRegistrationTokensTable(cm *sqlutil.Connections) tables.RegistrationTokensTable {
	return &registrationTokensTable{
		cm:                                     cm,
		selectTokenSQL:                         selectTokenSQL,
		insertTokenSQL:                         insertTokenSQL,
		listAllTokensSQL:                       listAllTokensSQL,
		listValidTokensSQL:                     listValidTokensSQL,
		listInvalidTokensSQL:                   listInvalidTokensSQL,
		getTokenSQL:                            getTokenSQL,
		deleteTokenSQL:                         deleteTokenSQL,
		updateTokenUsesAllowedAndExpiryTimeSQL: updateTokenUsesAllowedAndExpiryTimeSQL,
		updateTokenUsesAllowedSQL:              updateTokenUsesAllowedSQL,
		updateTokenExpiryTimeSQL:               updateTokenExpiryTimeSQL,
	}
}

// RegistrationTokenExists checks if a registration token exists.
func (t *registrationTokensTable) RegistrationTokenExists(ctx context.Context, token string) (bool, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectTokenSQL, token).Row()
	var foundToken string
	if err := row.Scan(&foundToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// InsertRegistrationToken inserts a new registration token.
func (t *registrationTokensTable) InsertRegistrationToken(ctx context.Context, registrationToken *api.RegistrationToken) (bool, error) {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.insertTokenSQL,
		registrationToken.Token,
		registrationToken.UsesAllowed,
		registrationToken.ExpiryTime,
		registrationToken.Pending,
		registrationToken.Completed,
	)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// ListRegistrationTokens lists registration tokens (all, valid, or invalid).
func (t *registrationTokensTable) ListRegistrationTokens(ctx context.Context, returnAll bool, valid bool) ([]api.RegistrationToken, error) {
	db := t.cm.Connection(ctx, true)
	var rows *sql.Rows
	var err error
	if returnAll {
		rows, err = db.Raw(t.listAllTokensSQL).Rows()
	} else if valid {
		rows, err = db.Raw(t.listValidTokensSQL, time.Now().UnixMilli()).Rows()
	} else {
		rows, err = db.Raw(t.listInvalidTokensSQL, time.Now().UnixMilli()).Rows()
	}
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "failed to close rows")
	var tokens []api.RegistrationToken
	for rows.Next() {
		var tkn api.RegistrationToken
		if err := rows.Scan(&tkn.Token, &tkn.Pending, &tkn.Completed, &tkn.UsesAllowed, &tkn.ExpiryTime); err != nil {
			return nil, err
		}
		tokens = append(tokens, tkn)
	}
	return tokens, nil
}

// GetRegistrationToken retrieves a registration token by token string.
func (t *registrationTokensTable) GetRegistrationToken(ctx context.Context, tokenString string) (*api.RegistrationToken, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.getTokenSQL, tokenString).Row()
	var tkn api.RegistrationToken
	tkn.Token = &tokenString
	if err := row.Scan(&tkn.Pending, &tkn.Completed, &tkn.UsesAllowed, &tkn.ExpiryTime); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &tkn, nil
}

// DeleteRegistrationToken deletes a registration token by token string.
func (t *registrationTokensTable) DeleteRegistrationToken(ctx context.Context, tokenString string) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteTokenSQL, tokenString)
	return result.Error
}

// UpdateRegistrationToken updates a registration token's usesAllowed and/or expiryTime.
func (t *registrationTokensTable) UpdateRegistrationToken(ctx context.Context, tokenString string, newAttributes map[string]interface{}) (*api.RegistrationToken, error) {
	db := t.cm.Connection(ctx, false)
	if usesAllowed, ok := newAttributes["uses_allowed"]; ok {
		if expiryTime, ok := newAttributes["expiry_time"]; ok {
			result := db.Exec(t.updateTokenUsesAllowedAndExpiryTimeSQL, tokenString, usesAllowed, expiryTime)
			if result.Error != nil {
				return nil, result.Error
			}
		} else {
			result := db.Exec(t.updateTokenUsesAllowedSQL, tokenString, usesAllowed)
			if result.Error != nil {
				return nil, result.Error
			}
		}
	} else if expiryTime, ok := newAttributes["expiry_time"]; ok {
		result := db.Exec(t.updateTokenExpiryTimeSQL, tokenString, expiryTime)
		if result.Error != nil {
			return nil, result.Error
		}
	}
	return t.GetRegistrationToken(ctx, tokenString)
}

// getInsertValue is a helper for nullable integer pointer values for SQL insert.
func getInsertValue[t constraints.Integer](in *t) any {
	if in == nil {
		return nil
	}
	return *in
}
