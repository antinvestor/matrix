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
	"fmt"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/userutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/pitabwire/frame"
)

// accountsSchema defines the schema for the accounts table.
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

// accountsSchemaRevert defines how to revert the accounts table schema.
const accountsSchemaRevert = `
DROP TABLE IF EXISTS userapi_accounts;
`

// insertAccountSQL is used to insert a new account into the database.
const insertAccountSQL = `
INSERT INTO userapi_accounts(localpart, server_name, created_ts, password_hash, appservice_id, account_type) VALUES ($1, $2, $3, $4, $5, $6)
`

// updatePasswordSQL is used to update the password hash for an account.
const updatePasswordSQL = `
UPDATE userapi_accounts SET password_hash = $1 WHERE localpart = $2 AND server_name = $3
`

// deactivateAccountSQL is used to deactivate an account.
const deactivateAccountSQL = `
UPDATE userapi_accounts SET is_deactivated = TRUE WHERE localpart = $1 AND server_name = $2
`

// selectAccountByLocalpartSQL is used to retrieve an account by its localpart.
const selectAccountByLocalpartSQL = `
SELECT localpart, server_name, appservice_id, account_type FROM userapi_accounts WHERE localpart = $1 AND server_name = $2
`

// selectPasswordHashSQL is used to retrieve the password hash for an account.
const selectPasswordHashSQL = `
SELECT password_hash FROM userapi_accounts WHERE localpart = $1 AND server_name = $2 AND is_deactivated = FALSE
`

// selectNewNumericLocalpartSQL is used to find the highest numeric localpart.
const selectNewNumericLocalpartSQL = `
SELECT COALESCE(MAX(localpart::bigint), 0) FROM userapi_accounts WHERE localpart ~ '^[0-9]{1,}$' AND server_name = $1
`

type accountsTable struct {
	cm                        sqlutil.ConnectionManager
	serverName                spec.ServerName
	insertAccountStmt         string
	updatePasswordStmt        string
	deactivateAccountStmt     string
	selectAccountByLocalpart  string
	selectPasswordHash        string
	selectNewNumericLocalpart string
}

// NewPostgresAccountsTable creates a new postgres accounts table.
func NewPostgresAccountsTable(ctx context.Context, cm sqlutil.ConnectionManager, serverName spec.ServerName) (tables.AccountsTable, error) {
	s := &accountsTable{
		cm:                        cm,
		serverName:                serverName,
		insertAccountStmt:         insertAccountSQL,
		updatePasswordStmt:        updatePasswordSQL,
		deactivateAccountStmt:     deactivateAccountSQL,
		selectAccountByLocalpart:  selectAccountByLocalpartSQL,
		selectPasswordHash:        selectPasswordHashSQL,
		selectNewNumericLocalpart: selectNewNumericLocalpartSQL,
	}

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "userapi_accounts_table_schema_001",
		Patch:       accountsSchema,
		RevertPatch: accountsSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertAccount creates a new account. 'hash' should be the password hash for this account. If it is missing,
// this account will be passwordless. Returns an error if this account already exists. Returns the account
// on success.
func (s *accountsTable) InsertAccount(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	hash, appserviceID string, accountType api.AccountType,
) (*api.Account, error) {
	createdTimeMS := time.Now().UnixNano() / 1000000

	db := s.cm.Connection(ctx, false)

	var err error
	if accountType != api.AccountTypeAppService {
		err = db.Exec(s.insertAccountStmt, localpart, serverName, createdTimeMS, hash, nil, accountType).Error
	} else {
		err = db.Exec(s.insertAccountStmt, localpart, serverName, createdTimeMS, hash, appserviceID, accountType).Error
	}
	if err != nil {
		return nil, fmt.Errorf("insertAccountStmt: %w", err)
	}

	return &api.Account{
		Localpart:    localpart,
		UserID:       userutil.MakeUserID(localpart, serverName),
		ServerName:   serverName,
		AppServiceID: appserviceID,
		AccountType:  accountType,
	}, nil
}

// UpdatePassword updates the password hash for an account.
func (s *accountsTable) UpdatePassword(
	ctx context.Context, localpart string, serverName spec.ServerName,
	passwordHash string,
) (err error) {
	db := s.cm.Connection(ctx, false)
	err = db.Exec(s.updatePasswordStmt, passwordHash, localpart, serverName).Error
	return
}

// DeactivateAccount deactivates an account.
func (s *accountsTable) DeactivateAccount(
	ctx context.Context, localpart string, serverName spec.ServerName,
) (err error) {
	db := s.cm.Connection(ctx, false)
	err = db.Exec(s.deactivateAccountStmt, localpart, serverName).Error
	return
}

// SelectPasswordHash retrieves the password hash for an account.
func (s *accountsTable) SelectPasswordHash(
	ctx context.Context, localpart string, serverName spec.ServerName,
) (hash string, err error) {
	db := s.cm.Connection(ctx, true)
	row := db.Raw(s.selectPasswordHash, localpart, serverName).Row()
	err = row.Scan(&hash)
	return
}

// SelectAccountByLocalpart retrieves an account by its localpart.
func (s *accountsTable) SelectAccountByLocalpart(
	ctx context.Context, localpart string, serverName spec.ServerName,
) (*api.Account, error) {
	var appserviceIDPtr sql.NullString
	var acc api.Account

	log := frame.Log(ctx)

	db := s.cm.Connection(ctx, true)
	row := db.Raw(s.selectAccountByLocalpart, localpart, serverName).Row()
	err := row.Scan(&acc.Localpart, &acc.ServerName, &appserviceIDPtr, &acc.AccountType)
	if err != nil {
		if !sqlutil.ErrorIsNoRows(err) {
			log.WithError(err).Error("Unable to retrieve user from the db")
		}
		return nil, err
	}
	if appserviceIDPtr.Valid {
		acc.AppServiceID = appserviceIDPtr.String
	}

	acc.UserID = userutil.MakeUserID(acc.Localpart, acc.ServerName)
	return &acc, nil
}

// SelectNewNumericLocalpart finds the highest numeric localpart and returns its value + 1.
func (s *accountsTable) SelectNewNumericLocalpart(
	ctx context.Context, serverName spec.ServerName,
) (id int64, err error) {
	db := s.cm.Connection(ctx, true)
	row := db.Raw(s.selectNewNumericLocalpart, serverName).Row()
	err = row.Scan(&id)
	return id + 1, err
}
