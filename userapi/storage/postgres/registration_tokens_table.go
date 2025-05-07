package postgres

import (
	"context"
	"database/sql"
	"errors"
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

// SQL query constants for registration token operations
const (
	// selectTokenSQL checks if a token exists in the database
	selectTokenSQL = "SELECT token FROM userapi_registration_tokens WHERE token = $1"

	// insertTokenSQL inserts a new registration token with its properties
	insertTokenSQL = "INSERT INTO userapi_registration_tokens (token, uses_allowed, expiry_time, pending, completed) VALUES ($1, $2, $3, $4, $5)"

	// listAllTokensSQL retrieves all registration tokens from the database
	listAllTokensSQL = "SELECT * FROM userapi_registration_tokens"

	// listValidTokensSQL retrieves all valid registration tokens (not expired, uses not exceeded)
	listValidTokensSQL = "SELECT * FROM userapi_registration_tokens WHERE" +
		"(uses_allowed > pending + completed OR uses_allowed IS NULL) AND" +
		"(expiry_time > $1 OR expiry_time IS NULL)"

	// listInvalidTokensSQL retrieves all invalid registration tokens (expired or uses exceeded)
	listInvalidTokensSQL = "SELECT * FROM userapi_registration_tokens WHERE" +
		"(uses_allowed <= pending + completed OR expiry_time <= $1)"

	// getTokenSQL retrieves a specific token's details by its string value
	getTokenSQL = "SELECT pending, completed, uses_allowed, expiry_time FROM userapi_registration_tokens WHERE token = $1"

	// deleteTokenSQL removes a registration token from the database
	deleteTokenSQL = "DELETE FROM userapi_registration_tokens WHERE token = $1"

	// updateTokenUsesAllowedAndExpiryTimeSQL updates both uses_allowed and expiry_time for a token
	updateTokenUsesAllowedAndExpiryTimeSQL = "UPDATE userapi_registration_tokens SET uses_allowed = $2, expiry_time = $3 WHERE token = $1"

	// updateTokenUsesAllowedSQL updates only the uses_allowed field for a token
	updateTokenUsesAllowedSQL = "UPDATE userapi_registration_tokens SET uses_allowed = $2 WHERE token = $1"

	// updateTokenExpiryTimeSQL updates only the expiry_time field for a token
	updateTokenExpiryTimeSQL = "UPDATE userapi_registration_tokens SET expiry_time = $2 WHERE token = $1"
)

type registrationTokensTable struct {
	cm *sqlutil.Connections

	selectTokenStmt                     string
	insertTokenStmt                     string
	listAllTokensStmt                   string
	listValidTokensStmt                 string
	listInvalidTokensStmt               string
	getTokenStmt                        string
	deleteTokenStmt                     string
	updateTokenUsesAllowedAndExpiryTimeStmt string
	updateTokenUsesAllowedStmt          string
	updateTokenExpiryTimeStmt           string
}

func NewPostgresRegistrationTokensTable(ctx context.Context, cm *sqlutil.Connections) (tables.RegistrationTokensTable, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(registrationTokensSchema).Error; err != nil {
		return nil, err
	}

	// Initialize table with SQL statements
	t := &registrationTokensTable{
		cm:                                cm,
		selectTokenStmt:                   selectTokenSQL,
		insertTokenStmt:                   insertTokenSQL,
		listAllTokensStmt:                 listAllTokensSQL,
		listValidTokensStmt:               listValidTokensSQL,
		listInvalidTokensStmt:             listInvalidTokensSQL,
		getTokenStmt:                      getTokenSQL,
		deleteTokenStmt:                   deleteTokenSQL,
		updateTokenUsesAllowedAndExpiryTimeStmt: updateTokenUsesAllowedAndExpiryTimeSQL,
		updateTokenUsesAllowedStmt:        updateTokenUsesAllowedSQL,
		updateTokenExpiryTimeStmt:         updateTokenExpiryTimeSQL,
	}

	return t, nil
}

func (t *registrationTokensTable) RegistrationTokenExists(ctx context.Context, token string) (bool, error) {
	var existingToken string

	db := t.cm.Connection(ctx, true)

	row := db.Raw(t.selectTokenStmt, token).Row()
	err := row.Scan(&existingToken)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (t *registrationTokensTable) InsertRegistrationToken(ctx context.Context, registrationToken *api.RegistrationToken) (bool, error) {
	db := t.cm.Connection(ctx, false)

	err := db.Exec(
		t.insertTokenStmt,
		*registrationToken.Token,
		getInsertValue(registrationToken.UsesAllowed),
		getInsertValue(registrationToken.ExpiryTime),
		*registrationToken.Pending,
		*registrationToken.Completed).Error
	if err != nil {
		return false, err
	}
	return true, nil
}

func getInsertValue[t constraints.Integer](in *t) any {
	if in == nil {
		return nil
	}
	return *in
}

func (t *registrationTokensTable) ListRegistrationTokens(ctx context.Context, returnAll bool, valid bool) ([]api.RegistrationToken, error) {
	var tokens []api.RegistrationToken
	var tokenString string
	var pending, completed, usesAllowed *int32
	var expiryTime *int64
	var rows *sql.Rows
	var err error

	db := t.cm.Connection(ctx, true)

	// Choose query based on parameters
	if returnAll {
		rows, err = db.Raw(t.listAllTokensStmt).Rows()
	} else if valid {
		rows, err = db.Raw(t.listValidTokensStmt, time.Now().UnixNano()/int64(time.Millisecond)).Rows()
	} else {
		rows, err = db.Raw(t.listInvalidTokensStmt, time.Now().UnixNano()/int64(time.Millisecond)).Rows()
	}

	if err != nil {
		return tokens, err
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&tokenString, &pending, &completed, &usesAllowed, &expiryTime)
		if err != nil {
			return tokens, err
		}
		tokenString := tokenString
		pending := pending
		completed := completed
		usesAllowed := usesAllowed
		expiryTime := expiryTime

		tokenMap := api.RegistrationToken{
			Token:       &tokenString,
			Pending:     pending,
			Completed:   completed,
			UsesAllowed: usesAllowed,
			ExpiryTime:  expiryTime,
		}
		tokens = append(tokens, tokenMap)
	}
	return tokens, rows.Err()
}

func (t *registrationTokensTable) GetRegistrationToken(ctx context.Context, tokenString string) (*api.RegistrationToken, error) {
	db := t.cm.Connection(ctx, true)

	var pending, completed, usesAllowed *int32
	var expiryTime *int64

	row := db.Raw(t.getTokenStmt, tokenString).Row()
	err := row.Scan(&pending, &completed, &usesAllowed, &expiryTime)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	tokenMap := &api.RegistrationToken{
		Token:       &tokenString,
		Pending:     pending,
		Completed:   completed,
		UsesAllowed: usesAllowed,
		ExpiryTime:  expiryTime,
	}
	return tokenMap, nil
}

func (t *registrationTokensTable) DeleteRegistrationToken(ctx context.Context, tokenString string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteTokenStmt, tokenString).Error
}

func (t *registrationTokensTable) UpdateRegistrationToken(ctx context.Context, tokenString string, newAttributes map[string]interface{}) (*api.RegistrationToken, error) {
	db := t.cm.Connection(ctx, false)

	// Check if the token exists first
	tokenOld, err := t.GetRegistrationToken(ctx, tokenString)
	if err != nil {
		return nil, err
	}
	if tokenOld == nil {
		return nil, nil
	}

	// Update attributes based on what was provided
	if usesAllowed, ok := newAttributes["uses_allowed"]; ok {
		if expiryTime, ok := newAttributes["expiry_time"]; ok {
			// Update both uses_allowed and expiry_time
			err = db.Exec(t.updateTokenUsesAllowedAndExpiryTimeStmt, tokenString, usesAllowed, expiryTime).Error
		} else {
			// Update only uses_allowed
			err = db.Exec(t.updateTokenUsesAllowedStmt, tokenString, usesAllowed).Error
		}
	} else if expiryTime, ok := newAttributes["expiry_time"]; ok {
		// Update only expiry_time
		err = db.Exec(t.updateTokenExpiryTimeStmt, tokenString, expiryTime).Error
	}

	if err != nil {
		return nil, err
	}

	// Get and return the updated token
	return t.GetRegistrationToken(ctx, tokenString)
}
