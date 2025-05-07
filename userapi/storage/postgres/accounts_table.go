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
	"errors"
	"fmt"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/userutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"

	log "github.com/sirupsen/logrus"
)

const accountsSchema = `
-- Stores data about accounts.
CREATE TABLE IF NOT EXISTS userapi_accounts (
    -- The Matrix user ID localpart for this account
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

// SQL query constants for accounts operations
const (
	// insertAccountSQL creates a new account
	insertAccountSQL = "INSERT INTO userapi_accounts(localpart, server_name, created_ts, password_hash, appservice_id, account_type) VALUES ($1, $2, $3, $4, $5, $6)"

	// updatePasswordSQL updates the password hash for an account
	updatePasswordSQL = "UPDATE userapi_accounts SET password_hash = $1 WHERE localpart = $2 AND server_name = $3"

	// deactivateAccountSQL marks an account as deactivated
	deactivateAccountSQL = "UPDATE userapi_accounts SET is_deactivated = TRUE WHERE localpart = $1 AND server_name = $2"

	// selectAccountByLocalpartSQL retrieves account details by localpart
	selectAccountByLocalpartSQL = "SELECT localpart, server_name, appservice_id, account_type FROM userapi_accounts WHERE localpart = $1 AND server_name = $2"

	// selectPasswordHashSQL retrieves password hash for an active account
	selectPasswordHashSQL = "SELECT password_hash FROM userapi_accounts WHERE localpart = $1 AND server_name = $2 AND is_deactivated = FALSE"

	// selectNewNumericLocalpartSQL finds the highest numeric localpart to generate a new one
	selectNewNumericLocalpartSQL = "SELECT COALESCE(MAX(localpart::bigint), 0) FROM userapi_accounts WHERE localpart ~ '^[0-9]{1,}$' AND server_name = $1"
)

type accountsTable struct {
	cm                     *sqlutil.Connections
	serverName             spec.ServerName
	
	// SQL statements directly as fields
	insertAccountStmt             string
	updatePasswordStmt            string
	deactivateAccountStmt         string
	selectAccountByLocalpartStmt  string
	selectPasswordHashStmt        string
	selectNewNumericLocalpartStmt string
}

func NewPostgresAccountsTable(ctx context.Context, cm *sqlutil.Connections, serverName spec.ServerName) (tables.AccountsTable, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(accountsSchema).Error; err != nil {
		return nil, err
	}
	
	// Run migrations
	m := sqlutil.NewMigrator(db.DB())
	err := m.Up(ctx)
	if err != nil {
		return nil, err
	}

	// Initialize table with SQL statements
	t := &accountsTable{
		cm:         cm,
		serverName: serverName,
		insertAccountStmt:             insertAccountSQL,
		updatePasswordStmt:            updatePasswordSQL,
		deactivateAccountStmt:         deactivateAccountSQL,
		selectAccountByLocalpartStmt:  selectAccountByLocalpartSQL,
		selectPasswordHashStmt:        selectPasswordHashSQL,
		selectNewNumericLocalpartStmt: selectNewNumericLocalpartSQL,
	}
	
	return t, nil
}

// insertAccount creates a new account. 'hash' should be the password hash for this account. If it is missing,
// this account will be passwordless. Returns an error if this account already exists. Returns the account
// on success.
func (t *accountsTable) InsertAccount(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	hash, appserviceID string, accountType api.AccountType,
) (*api.Account, error) {
	createdTimeMS := time.Now().UnixNano() / 1000000
	
	db := t.cm.Connection(ctx, false)

	var err error
	if accountType != api.AccountTypeAppService {
		err = db.Exec(t.insertAccountStmt, localpart, serverName, createdTimeMS, hash, nil, accountType).Error
	} else {
		err = db.Exec(t.insertAccountStmt, localpart, serverName, createdTimeMS, hash, appserviceID, accountType).Error
	}
	if err != nil {
		return nil, fmt.Errorf("insertAccountSQL: %w", err)
	}

	return &api.Account{
		Localpart:    localpart,
		UserID:       userutil.MakeUserID(localpart, serverName),
		ServerName:   serverName,
		AppServiceID: appserviceID,
		AccountType:  accountType,
	}, nil
}

func (t *accountsTable) UpdatePassword(
	ctx context.Context, localpart string, serverName spec.ServerName,
	passwordHash string,
) (err error) {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.updatePasswordStmt, passwordHash, localpart, serverName).Error
}

func (t *accountsTable) DeactivateAccount(
	ctx context.Context, localpart string, serverName spec.ServerName,
) (err error) {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deactivateAccountStmt, localpart, serverName).Error
}

func (t *accountsTable) SelectPasswordHash(
	ctx context.Context, localpart string, serverName spec.ServerName,
) (hash string, err error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectPasswordHashStmt, localpart, serverName).Row()
	err = row.Scan(&hash)
	return
}

func (t *accountsTable) SelectAccountByLocalpart(
	ctx context.Context, localpart string, serverName spec.ServerName,
) (*api.Account, error) {
	var appserviceIDPtr sql.NullString
	var acc api.Account

	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectAccountByLocalpartStmt, localpart, serverName).Row()
	err := row.Scan(&acc.Localpart, &acc.ServerName, &appserviceIDPtr, &acc.AccountType)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
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

func (t *accountsTable) SelectNewNumericLocalpart(
	ctx context.Context, serverName spec.ServerName,
) (id int64, err error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectNewNumericLocalpartStmt, serverName).Row()
	if err = row.Scan(&id); err != nil {
		return 0, fmt.Errorf("SelectNewNumericLocalpart: %w", err)
	}
	return id + 1, nil
}
