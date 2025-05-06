// Copyright 2017 Vector Creations Ltd
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
	"database/sql"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/userutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

const accountsSchema = `
-- Stores data about accounts.
CREATE TABLE IF NOT EXISTS userapi_accounts (
    -- The Global user ID localpart for this account
    localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
    -- When this account was first created, as a unix timestamp (ms resolution).
    created_ts BIGINT NOT NULL,
    -- The password hash for this account. Can be NULL if this is a passwordless account.
    password_hash TEXT,
    -- Identifies which application service this account belongs to, if any.
    appservice_id TEXT,
    -- If the account is currently active
    is_deactivated BOOLEAN DEFAULT FALSE,
	-- The account_type (user = 1, guest = 2, admin = 3, appservice = 4)
	account_type SMALLINT NOT NULL DEFAULT 1
    -- TODO:
    -- upgraded_ts, devices, any email reset stuff?
);

CREATE UNIQUE INDEX IF NOT EXISTS userapi_accounts_idx ON userapi_accounts(localpart, server_name);
`

const accountsSchemaRevert = "DROP TABLE IF EXISTS userapi_accounts CASCADE;"

const insertAccountSQL = "" +
	"INSERT INTO userapi_accounts(localpart, server_name, created_ts, password_hash, appservice_id, account_type) VALUES ($1, $2, $3, $4, $5, $6)"

const updatePasswordSQL = "" +
	"UPDATE userapi_accounts SET password_hash = $1 WHERE localpart = $2 AND server_name = $3"

const deactivateAccountSQL = "" +
	"UPDATE userapi_accounts SET is_deactivated = TRUE WHERE localpart = $1 AND server_name = $2"

const selectAccountByLocalpartSQL = "" +
	"SELECT localpart, server_name, appservice_id, account_type FROM userapi_accounts WHERE localpart = $1 AND server_name = $2"

const selectPasswordHashSQL = "" +
	"SELECT password_hash FROM userapi_accounts WHERE localpart = $1 AND server_name = $2 AND is_deactivated = FALSE"

const selectNewNumericLocalpartSQL = "" +
	"SELECT COALESCE(MAX(localpart::bigint), 0) FROM userapi_accounts WHERE localpart ~ '^[0-9]{1,}$' AND server_name = $1"

// accountsTable implements tables.AccountsTable using GORM and a connection manager.
type accountsTable struct {
	cm *sqlutil.Connections

	insertAccountSQL             string
	updatePasswordSQL            string
	deactivateAccountSQL         string
	selectAccountByLocalpartSQL  string
	selectPasswordHashSQL        string
	selectNewNumericLocalpartSQL string
}

// NewPostgresAccountsTable returns a new AccountsTable using the provided connection manager.
func NewPostgresAccountsTable(cm *sqlutil.Connections) tables.AccountsTable {
	return &accountsTable{
		cm:                           cm,
		insertAccountSQL:             insertAccountSQL,
		updatePasswordSQL:            updatePasswordSQL,
		deactivateAccountSQL:         deactivateAccountSQL,
		selectAccountByLocalpartSQL:  selectAccountByLocalpartSQL,
		selectPasswordHashSQL:        selectPasswordHashSQL,
		selectNewNumericLocalpartSQL: selectNewNumericLocalpartSQL,
	}
}

// InsertAccount inserts a new account and returns the created account struct.
func (t *accountsTable) InsertAccount(ctx context.Context, localpart string, serverName spec.ServerName, hash, appserviceID string, accountType api.AccountType) (*api.Account, error) {
	createdTimeMS := time.Now().UnixNano() / 1e6
	db := t.cm.Connection(ctx, false)
	err := db.Exec(t.insertAccountSQL, localpart, serverName, createdTimeMS, hash, appserviceID, int32(accountType)).Error
	if err != nil {
		return nil, err
	}
	acc := &api.Account{
		Localpart:    localpart,
		UserID:       userutil.MakeUserID(localpart, serverName),
		ServerName:   serverName,
		AppServiceID: appserviceID,
		AccountType:  accountType,
	}
	return acc, nil
}

// UpdatePassword updates the password hash for an account.
func (t *accountsTable) UpdatePassword(ctx context.Context, localpart string, serverName spec.ServerName, passwordHash string) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.updatePasswordSQL, passwordHash, localpart, serverName)
	return result.Error
}

// DeactivateAccount sets the account as deactivated.
func (t *accountsTable) DeactivateAccount(ctx context.Context, localpart string, serverName spec.ServerName) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deactivateAccountSQL, localpart, serverName)
	return result.Error
}

// SelectAccountByLocalpart retrieves an account by localpart and server name.
func (t *accountsTable) SelectAccountByLocalpart(ctx context.Context, localpart string, serverName spec.ServerName) (*api.Account, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectAccountByLocalpartSQL, localpart, serverName).Row()
	var appserviceID sql.NullString
	var acc api.Account
	if err := row.Scan(&acc.Localpart, &acc.ServerName, &appserviceID, &acc.AccountType); err != nil {
		return nil, err
	}
	if appserviceID.Valid {
		acc.AppServiceID = appserviceID.String
	}
	acc.UserID = userutil.MakeUserID(acc.Localpart, acc.ServerName)
	return &acc, nil
}

// SelectPasswordHash retrieves the password hash for an account.
func (t *accountsTable) SelectPasswordHash(ctx context.Context, localpart string, serverName spec.ServerName) (string, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectPasswordHashSQL, localpart, serverName).Row()
	var passwordHash sql.NullString
	if err := row.Scan(&passwordHash); err != nil {
		return "", err
	}
	if passwordHash.Valid {
		return passwordHash.String, nil
	}
	return "", nil
}

// SelectNewNumericLocalpart retrieves the next available numeric localpart for a server.
func (t *accountsTable) SelectNewNumericLocalpart(ctx context.Context, serverName spec.ServerName) (int64, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectNewNumericLocalpartSQL, serverName).Row()
	var localpart int64
	if err := row.Scan(&localpart); err != nil {
		return 0, err
	}
	return localpart + 1, nil
}
