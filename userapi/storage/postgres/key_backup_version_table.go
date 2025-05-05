// Copyright 2021 The Global.org Foundation C.I.C.
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

const keyBackupVersionTableSchemaRevert = "DROP TABLE IF EXISTS userapi_key_backup_versions CASCADE; DROP SEQUENCE IF EXISTS userapi_key_backup_versions_seq; DROP INDEX IF EXISTS userapi_key_backup_versions_idx;"

// SQL: Insert key backup version
const insertKeyBackupSQL = "INSERT INTO userapi_key_backup_versions(user_id, algorithm, auth_data, etag) VALUES ($1, $2, $3, $4) RETURNING version"

// SQL: Update key backup version auth data
const updateKeyBackupAuthDataSQL = "UPDATE userapi_key_backup_versions SET auth_data = $1 WHERE user_id = $2 AND version = $3"

// SQL: Update key backup version etag
const updateKeyBackupETagSQL = "UPDATE userapi_key_backup_versions SET etag = $1 WHERE user_id = $2 AND version = $3"

// SQL: Delete key backup version
const deleteKeyBackupSQL = "UPDATE userapi_key_backup_versions SET deleted=1 WHERE user_id = $1 AND version = $2"

// SQL: Select key backup version
const selectKeyBackupSQL = "SELECT algorithm, auth_data, etag, deleted FROM userapi_key_backup_versions WHERE user_id = $1 AND version = $2"

// SQL: Select latest version
const selectLatestVersionSQL = "SELECT MAX(version) FROM userapi_key_backup_versions WHERE user_id = $1"

// keyBackupVersionTable implements tables.KeyBackupVersionTable using GORM and a connection manager.
type keyBackupVersionTable struct {
	cm *sqlutil.Connections

	insertKeyBackupSQL         string
	updateKeyBackupAuthDataSQL string
	updateKeyBackupETagSQL     string
	deleteKeyBackupSQL         string
	selectKeyBackupSQL         string
	selectLatestVersionSQL     string
}

// NewPostgresKeyBackupVersionTable returns a new KeyBackupVersionTable using the provided connection manager.
func NewPostgresKeyBackupVersionTable(cm *sqlutil.Connections) tables.KeyBackupVersionTable {
	return &keyBackupVersionTable{
		cm:                         cm,
		insertKeyBackupSQL:         insertKeyBackupSQL,
		updateKeyBackupAuthDataSQL: updateKeyBackupAuthDataSQL,
		updateKeyBackupETagSQL:     updateKeyBackupETagSQL,
		deleteKeyBackupSQL:         deleteKeyBackupSQL,
		selectKeyBackupSQL:         selectKeyBackupSQL,
		selectLatestVersionSQL:     selectLatestVersionSQL,
	}
}

// InsertKeyBackup inserts a new key backup version.
func (t *keyBackupVersionTable) InsertKeyBackup(
	ctx context.Context, userID, algorithm string, authData json.RawMessage, etag string,
) (version string, err error) {
	db := t.cm.Connection(ctx, false)
	var versionInt int64
	err = db.Raw(t.insertKeyBackupSQL, userID, algorithm, string(authData), etag).Row().Scan(&versionInt)
	return strconv.FormatInt(versionInt, 10), err
}

// UpdateKeyBackupAuthData updates an existing key backup version auth data.
func (t *keyBackupVersionTable) UpdateKeyBackupAuthData(
	ctx context.Context, userID, version string, authData json.RawMessage,
) error {
	versionInt, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid version")
	}
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.updateKeyBackupAuthDataSQL, string(authData), userID, versionInt)
	return result.Error
}

// UpdateKeyBackupETag updates an existing key backup version etag.
func (t *keyBackupVersionTable) UpdateKeyBackupETag(
	ctx context.Context, userID, version, etag string,
) error {
	versionInt, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid version")
	}
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.updateKeyBackupETagSQL, etag, userID, versionInt)
	return result.Error
}

// DeleteKeyBackup deletes a key backup version.
func (t *keyBackupVersionTable) DeleteKeyBackup(
	ctx context.Context, userID, version string,
) (bool, error) {
	versionInt, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return false, fmt.Errorf("invalid version")
	}
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteKeyBackupSQL, userID, versionInt)
	if err = result.Error; err != nil {
		return false, err
	}
	return result.RowsAffected == 1, nil
}

// SelectKeyBackup retrieves a key backup version.
func (t *keyBackupVersionTable) SelectKeyBackup(
	ctx context.Context, userID, version string,
) (versionResult, algorithm string, authData json.RawMessage, etag string, deleted bool, err error) {
	db := t.cm.Connection(ctx, true)
	var versionInt int64
	if version == "" {
		var v *int64 // allows nulls
		if err = db.Raw(t.selectLatestVersionSQL, userID).Row().Scan(&v); err != nil {
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
	err = db.Raw(t.selectKeyBackupSQL, userID, versionInt).Row().Scan(&algorithm, &authDataStr, &etag, &deletedInt)
	deleted = deletedInt == 1
	authData = json.RawMessage(authDataStr)
	return
}
