package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/antinvestor/matrix/clientapi/api"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/pitabwire/frame"
	"golang.org/x/exp/constraints"
)

// registrationTokensSchema defines the schema for registration tokens storage
const registrationTokensSchema = `
CREATE TABLE IF NOT EXISTS userapi_registration_tokens (
	token TEXT PRIMARY KEY,
	pending BIGINT,
	completed BIGINT,
	uses_allowed BIGINT,
	expiry_time BIGINT
);
`

// registrationTokensSchemaRevert defines the revert operation for the registration tokens schema
const registrationTokensSchemaRevert = `
DROP TABLE IF EXISTS userapi_registration_tokens;
`

// SQL query constants
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

// registrationTokenTable represents a registration tokens table
type registrationTokenTable struct {
	cm                                  *sqlutil.Connections
	selectToken                         string
	insertToken                         string
	listAllTokens                       string
	listValidTokens                     string
	listInvalidTokens                   string
	getToken                            string
	deleteToken                         string
	updateTokenUsesAllowedAndExpiryTime string
	updateTokenUsesAllowed              string
	updateTokenExpiryTime               string
}

// NewPostgresRegistrationTokensTable creates a new postgres registration tokens table
func NewPostgresRegistrationTokensTable(ctx context.Context, cm *sqlutil.Connections) (tables.RegistrationTokensTable, error) {
	// Perform schema migration first
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "userapi_registration_tokens_table_schema_001",
		Patch:       registrationTokensSchema,
		RevertPatch: registrationTokensSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Create table implementation after migration is successful
	s := &registrationTokenTable{
		cm:                                  cm,
		selectToken:                         selectTokenSQL,
		insertToken:                         insertTokenSQL,
		listAllTokens:                       listAllTokensSQL,
		listValidTokens:                     listValidTokensSQL,
		listInvalidTokens:                   listInvalidTokensSQL,
		getToken:                            getTokenSQL,
		deleteToken:                         deleteTokenSQL,
		updateTokenUsesAllowedAndExpiryTime: updateTokenUsesAllowedAndExpiryTimeSQL,
		updateTokenUsesAllowed:              updateTokenUsesAllowedSQL,
		updateTokenExpiryTime:               updateTokenExpiryTimeSQL,
	}

	return s, nil
}

// RegistrationTokenExists checks if a registration token exists in the database
func (s *registrationTokenTable) RegistrationTokenExists(ctx context.Context, token string) (bool, error) {
	var existingToken string
	db := s.cm.Connection(ctx, true)
	row := db.Raw(s.selectToken, token).Row()
	err := row.Scan(&existingToken)
	if err != nil {
		if sqlutil.ErrorIsNoRows(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// InsertRegistrationToken inserts a new registration token into the database
func (s *registrationTokenTable) InsertRegistrationToken(ctx context.Context, registrationToken *api.RegistrationToken) (bool, error) {
	db := s.cm.Connection(ctx, false)
	err := db.Exec(
		s.insertToken,
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

// ListRegistrationTokens lists all registration tokens in the database
func (s *registrationTokenTable) ListRegistrationTokens(ctx context.Context, returnAll bool, valid bool) ([]api.RegistrationToken, error) {
	var tokens []api.RegistrationToken
	var tokenString string
	var pending, completed, usesAllowed *int32
	var expiryTime *int64
	var rows *sql.Rows
	var err error

	db := s.cm.Connection(ctx, true)

	if returnAll {
		rows, err = db.Raw(s.listAllTokens).Rows()
	} else if valid {
		rows, err = db.Raw(s.listValidTokens, time.Now().UnixNano()/int64(time.Millisecond)).Rows()
	} else {
		rows, err = db.Raw(s.listInvalidTokens, time.Now().UnixNano()/int64(time.Millisecond)).Rows()
	}
	if err != nil {
		return tokens, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "ListRegistrationTokens: rows.close() failed")
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

// GetRegistrationToken retrieves a registration token from the database
func (s *registrationTokenTable) GetRegistrationToken(ctx context.Context, tokenString string) (*api.RegistrationToken, error) {
	db := s.cm.Connection(ctx, true)
	var pending, completed, usesAllowed *int32
	var expiryTime *int64
	row := db.Raw(s.getToken, tokenString).Row()
	err := row.Scan(&pending, &completed, &usesAllowed, &expiryTime)
	if err != nil {
		return nil, err
	}
	token := api.RegistrationToken{
		Token:       &tokenString,
		Pending:     pending,
		Completed:   completed,
		UsesAllowed: usesAllowed,
		ExpiryTime:  expiryTime,
	}
	return &token, nil
}

// DeleteRegistrationToken deletes a registration token from the database
func (s *registrationTokenTable) DeleteRegistrationToken(ctx context.Context, tokenString string) error {
	db := s.cm.Connection(ctx, false)
	err := db.Exec(s.deleteToken, tokenString).Error
	if err != nil {
		return err
	}
	return nil
}

// UpdateRegistrationToken updates a registration token in the database
func (s *registrationTokenTable) UpdateRegistrationToken(ctx context.Context, tokenString string, newAttributes map[string]interface{}) (*api.RegistrationToken, error) {
	db := s.cm.Connection(ctx, false)

	usesAllowed, usesAllowedPresent := newAttributes["usesAllowed"]
	expiryTime, expiryTimePresent := newAttributes["expiryTime"]

	if usesAllowedPresent && expiryTimePresent {
		err := db.Exec(s.updateTokenUsesAllowedAndExpiryTime, tokenString, usesAllowed, expiryTime).Error
		if err != nil {
			return nil, err
		}
	} else if usesAllowedPresent {
		err := db.Exec(s.updateTokenUsesAllowed, tokenString, usesAllowed).Error
		if err != nil {
			return nil, err
		}
	} else if expiryTimePresent {
		err := db.Exec(s.updateTokenExpiryTime, tokenString, expiryTime).Error
		if err != nil {
			return nil, err
		}
	}

	return s.GetRegistrationToken(ctx, tokenString)
}
