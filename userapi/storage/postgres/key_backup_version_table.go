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
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

const keyBackupVersionTableSchema = `
CREATE SEQUENCE IF NOT EXISTS userapi_key_backup_versions_seq;

-- the metadata for each generation of encrypted e2e session backups
CREATE TABLE IF NOT EXISTS userapi_key_backup_versions (
    user_id TEXT NOT NULL,
	-- this means no 2 users will ever have the same version of e2e session backups which strictly
	-- isn't necessary, but this is easy to do rather than SELECT MAX(version)+1.
    version BIGINT DEFAULT nextval('userapi_key_backup_versions_seq'),
    algorithm TEXT NOT NULL,
    auth_data TEXT NOT NULL,
	etag TEXT NOT NULL,
    deleted SMALLINT DEFAULT 0 NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS userapi_key_backup_versions_idx ON userapi_key_backup_versions(user_id, version);
`

// SQL query constants for key backup version operations
const (
	// insertKeyBackupSQL inserts a new key backup version
	insertKeyBackupSQL = "INSERT INTO userapi_key_backup_versions(user_id, algorithm, auth_data, etag) VALUES ($1, $2, $3, $4) RETURNING version"

	// updateKeyBackupAuthDataSQL updates the auth data for a key backup version
	updateKeyBackupAuthDataSQL = "UPDATE userapi_key_backup_versions SET auth_data = $1 WHERE user_id = $2 AND version = $3"

	// updateKeyBackupETagSQL updates the etag for a key backup version
	updateKeyBackupETagSQL = "UPDATE userapi_key_backup_versions SET etag = $1 WHERE user_id = $2 AND version = $3"

	// deleteKeyBackupSQL marks a key backup version as deleted
	deleteKeyBackupSQL = "UPDATE userapi_key_backup_versions SET deleted=1 WHERE user_id = $1 AND version = $2"

	// selectKeyBackupSQL retrieves a key backup version
	selectKeyBackupSQL = "SELECT algorithm, auth_data, etag, deleted FROM userapi_key_backup_versions WHERE user_id = $1 AND version = $2"

	// selectLatestVersionSQL retrieves the latest version for a user
	selectLatestVersionSQL = "SELECT MAX(version) FROM userapi_key_backup_versions WHERE user_id = $1"
)

type keyBackupVersionTable struct {
	cm *sqlutil.Connections

	insertKeyBackupStmt         string
	updateKeyBackupAuthDataStmt string
	deleteKeyBackupStmt         string
	selectKeyBackupStmt         string
	selectLatestVersionStmt     string
	updateKeyBackupETagStmt     string
}

func NewPostgresKeyBackupVersionTable(ctx context.Context, cm *sqlutil.Connections) (tables.KeyBackupVersionTable, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(keyBackupVersionTableSchema).Error; err != nil {
		return nil, err
	}

	// Initialize table with SQL statements
	t := &keyBackupVersionTable{
		cm:                          cm,
		insertKeyBackupStmt:         insertKeyBackupSQL,
		updateKeyBackupAuthDataStmt: updateKeyBackupAuthDataSQL,
		deleteKeyBackupStmt:         deleteKeyBackupSQL,
		selectKeyBackupStmt:         selectKeyBackupSQL,
		selectLatestVersionStmt:     selectLatestVersionSQL,
		updateKeyBackupETagStmt:     updateKeyBackupETagSQL,
	}

	return t, nil
}

func (t *keyBackupVersionTable) InsertKeyBackup(
	ctx context.Context, userID, algorithm string, authData json.RawMessage, etag string,
) (version string, err error) {
	db := t.cm.Connection(ctx, false)

	var versionInt int64
	row := db.Raw(t.insertKeyBackupStmt, userID, algorithm, string(authData), etag).Row()
	err = row.Scan(&versionInt)
	return strconv.FormatInt(versionInt, 10), err
}

func (t *keyBackupVersionTable) UpdateKeyBackupAuthData(
	ctx context.Context, userID, version string, authData json.RawMessage,
) error {
	db := t.cm.Connection(ctx, false)

	versionInt, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid version")
	}

	return db.Exec(t.updateKeyBackupAuthDataStmt, string(authData), userID, versionInt).Error
}

func (t *keyBackupVersionTable) UpdateKeyBackupETag(
	ctx context.Context, userID, version, etag string,
) error {
	db := t.cm.Connection(ctx, false)

	versionInt, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid version")
	}

	return db.Exec(t.updateKeyBackupETagStmt, etag, userID, versionInt).Error
}

func (t *keyBackupVersionTable) DeleteKeyBackup(
	ctx context.Context, userID, version string,
) (bool, error) {
	db := t.cm.Connection(ctx, false)

	versionInt, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return false, fmt.Errorf("invalid version")
	}

	result := db.Exec(t.deleteKeyBackupStmt, userID, versionInt)
	if err := result.Error; err != nil {
		return false, err
	}

	rowsAffected := result.RowsAffected
	return rowsAffected == 1, nil
}

func (t *keyBackupVersionTable) SelectKeyBackup(
	ctx context.Context, userID, version string,
) (versionResult, algorithm string, authData json.RawMessage, etag string, deleted bool, err error) {
	db := t.cm.Connection(ctx, true)

	var versionInt int64
	if version == "" {
		var v *int64 // allows nulls
		row := db.Raw(t.selectLatestVersionStmt, userID).Row()
		if err = row.Scan(&v); err != nil {
			return
		}
		if v == nil {
			err = sql.ErrNoRows
			return
		}
		versionInt = *v
	} else {
		if versionInt, err = strconv.ParseInt(version, 10, 64); err != nil {
			return
		}
	}

	versionResult = strconv.FormatInt(versionInt, 10)
	var deletedInt int
	var authDataStr string

	row := db.Raw(t.selectKeyBackupStmt, userID, versionInt).Row()
	err = row.Scan(&algorithm, &authDataStr, &etag, &deletedInt)
	deleted = deletedInt == 1
	authData = json.RawMessage(authDataStr)
	return
}
