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
	"github.com/pitabwire/frame"
)

// keyBackupVersionTableSchema defines the schema for the key backup versions table.
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

// keyBackupVersionTableSchemaRevert defines how to revert the key backup versions table schema.
const keyBackupVersionTableSchemaRevert = `
DROP TABLE IF EXISTS userapi_key_backup_versions;
DROP SEQUENCE IF EXISTS userapi_key_backup_versions_seq;
`

// insertKeyBackupSQL is used to insert a new key backup into the database.
const insertKeyBackupSQL = "" +
	"INSERT INTO userapi_key_backup_versions(user_id, algorithm, auth_data, etag) VALUES ($1, $2, $3, $4) RETURNING version"

// updateKeyBackupAuthDataSQL is used to update the auth data for a key backup.
const updateKeyBackupAuthDataSQL = "" +
	"UPDATE userapi_key_backup_versions SET auth_data = $1 WHERE user_id = $2 AND version = $3"

// updateKeyBackupETagSQL is used to update the etag for a key backup.
const updateKeyBackupETagSQL = "" +
	"UPDATE userapi_key_backup_versions SET etag = $1 WHERE user_id = $2 AND version = $3"

// deleteKeyBackupSQL is used to mark a key backup as deleted.
const deleteKeyBackupSQL = "" +
	"UPDATE userapi_key_backup_versions SET deleted=1 WHERE user_id = $1 AND version = $2"

// selectKeyBackupSQL is used to retrieve a key backup.
const selectKeyBackupSQL = "" +
	"SELECT algorithm, auth_data, etag, deleted FROM userapi_key_backup_versions WHERE user_id = $1 AND version = $2"

// selectLatestVersionSQL is used to retrieve the latest version for a user.
const selectLatestVersionSQL = "" +
	"SELECT MAX(version) FROM userapi_key_backup_versions WHERE user_id = $1"

type keyBackupVersionTable struct {
	cm                         *sqlutil.Connections
	insertKeyBackupSQL         string
	updateKeyBackupAuthDataSQL string
	updateKeyBackupETagSQL     string
	deleteKeyBackupSQL         string
	selectKeyBackupSQL         string
	selectLatestVersionSQL     string
}

// NewPostgresKeyBackupVersionTable creates a new postgres key backup version table.
func NewPostgresKeyBackupVersionTable(ctx context.Context, cm *sqlutil.Connections) (tables.KeyBackupVersionTable, error) {

	// Perform schema migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "userapi_key_backup_versions_table_schema_001",
		Patch:       keyBackupVersionTableSchema,
		RevertPatch: keyBackupVersionTableSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	t := &keyBackupVersionTable{
		cm:                         cm,
		insertKeyBackupSQL:         insertKeyBackupSQL,
		updateKeyBackupAuthDataSQL: updateKeyBackupAuthDataSQL,
		updateKeyBackupETagSQL:     updateKeyBackupETagSQL,
		deleteKeyBackupSQL:         deleteKeyBackupSQL,
		selectKeyBackupSQL:         selectKeyBackupSQL,
		selectLatestVersionSQL:     selectLatestVersionSQL,
	}

	return t, nil
}

// InsertKeyBackup inserts a new key backup version into the database.
func (t *keyBackupVersionTable) InsertKeyBackup(
	ctx context.Context, userID, algorithm string, authData json.RawMessage, etag string,
) (version string, err error) {
	var versionInt int64
	db := t.cm.Connection(ctx, false)
	row := db.Raw(t.insertKeyBackupSQL, userID, algorithm, string(authData), etag).Row()
	err = row.Scan(&versionInt)
	return strconv.FormatInt(versionInt, 10), err
}

// UpdateKeyBackupAuthData updates the auth data for a key backup version.
func (t *keyBackupVersionTable) UpdateKeyBackupAuthData(
	ctx context.Context, userID, version string, authData json.RawMessage,
) error {
	versionInt, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid version")
	}
	db := t.cm.Connection(ctx, false)
	err = db.Exec(t.updateKeyBackupAuthDataSQL, string(authData), userID, versionInt).Error
	return err
}

// UpdateKeyBackupETag updates the etag for a key backup version.
func (t *keyBackupVersionTable) UpdateKeyBackupETag(
	ctx context.Context, userID, version, etag string,
) error {
	versionInt, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid version")
	}
	db := t.cm.Connection(ctx, false)
	err = db.Exec(t.updateKeyBackupETagSQL, etag, userID, versionInt).Error
	return err
}

// DeleteKeyBackup marks a key backup version as deleted.
func (t *keyBackupVersionTable) DeleteKeyBackup(
	ctx context.Context, userID, version string,
) (bool, error) {
	versionInt, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return false, fmt.Errorf("invalid version")
	}
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteKeyBackupSQL, userID, versionInt)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected == 1, nil
}

// SelectKeyBackup retrieves a key backup version from the database.
func (t *keyBackupVersionTable) SelectKeyBackup(
	ctx context.Context, userID, version string,
) (versionResult, algorithm string, authData json.RawMessage, etag string, deleted bool, err error) {
	var versionInt int64
	db := t.cm.Connection(ctx, true)

	if version == "" {
		var v *int64 // allows nulls
		row := db.Raw(t.selectLatestVersionSQL, userID).Row()
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

	row := db.Raw(t.selectKeyBackupSQL, userID, versionInt).Row()
	err = row.Scan(&algorithm, &authDataStr, &etag, &deletedInt)
	deleted = deletedInt == 1
	authData = json.RawMessage(authDataStr)
	return
}
