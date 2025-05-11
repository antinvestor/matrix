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
	"fmt"

	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/antinvestor/matrix/userapi/types"
	"github.com/pitabwire/frame"
)

// crossSigningKeysSchema defines the schema for the cross signing keys table.
const crossSigningKeysSchema = `
CREATE TABLE IF NOT EXISTS keyserver_cross_signing_keys (
    user_id TEXT NOT NULL,
	key_type SMALLINT NOT NULL,
	key_data TEXT NOT NULL,
	PRIMARY KEY (user_id, key_type)
);
`

// crossSigningKeysSchemaRevert defines how to revert the cross signing keys table schema.
const crossSigningKeysSchemaRevert = `
DROP TABLE IF EXISTS keyserver_cross_signing_keys;
`

// selectCrossSigningKeysForUserSQL is used to retrieve cross signing keys for a specific user.
const selectCrossSigningKeysForUserSQL = "" +
	"SELECT key_type, key_data FROM keyserver_cross_signing_keys" +
	" WHERE user_id = $1"

// upsertCrossSigningKeysForUserSQL is used to insert or update cross signing keys for a specific user.
const upsertCrossSigningKeysForUserSQL = "" +
	"INSERT INTO keyserver_cross_signing_keys (user_id, key_type, key_data)" +
	" VALUES($1, $2, $3)" +
	" ON CONFLICT (user_id, key_type) DO UPDATE SET key_data = $3"

type crossSigningKeysTable struct {
	cm                               sqlutil.ConnectionManager
	selectCrossSigningKeysForUserSQL string
	upsertCrossSigningKeysForUserSQL string
}

// NewPostgresCrossSigningKeysTable creates a new postgres cross signing keys table.
func NewPostgresCrossSigningKeysTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.CrossSigningKeys, error) {

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "keyserver_cross_signing_keys_table_schema_001",
		Patch:       crossSigningKeysSchema,
		RevertPatch: crossSigningKeysSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	t := &crossSigningKeysTable{
		cm:                               cm,
		selectCrossSigningKeysForUserSQL: selectCrossSigningKeysForUserSQL,
		upsertCrossSigningKeysForUserSQL: upsertCrossSigningKeysForUserSQL,
	}

	return t, nil
}

// SelectCrossSigningKeysForUser retrieves cross signing keys for a specific user.
func (t *crossSigningKeysTable) SelectCrossSigningKeysForUser(
	ctx context.Context, userID string,
) (r types.CrossSigningKeyMap, err error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectCrossSigningKeysForUserSQL, userID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectCrossSigningKeysForUser: rows.close() failed")

	r = types.CrossSigningKeyMap{}
	for rows.Next() {
		var keyTypeInt int16
		var keyData spec.Base64Bytes
		if err = rows.Scan(&keyTypeInt, &keyData); err != nil {
			return nil, err
		}
		keyType, ok := types.KeyTypeIntToPurpose[keyTypeInt]
		if !ok {
			return nil, fmt.Errorf("unknown key purpose int %d", keyTypeInt)
		}
		r[keyType] = keyData
	}
	err = rows.Err()
	return
}

// UpsertCrossSigningKeysForUser inserts or updates cross signing keys for a specific user.
func (t *crossSigningKeysTable) UpsertCrossSigningKeysForUser(
	ctx context.Context, userID string, keyType fclient.CrossSigningKeyPurpose, keyData spec.Base64Bytes,
) error {
	keyTypeInt, ok := types.KeyTypePurposeToInt[keyType]
	if !ok {
		return fmt.Errorf("unknown key purpose %q", keyType)
	}

	db := t.cm.Connection(ctx, false)
	if err := db.Exec(t.upsertCrossSigningKeysForUserSQL, userID, keyTypeInt, keyData).Error; err != nil {
		return fmt.Errorf("failed to upsert cross signing keys: %w", err)
	}
	return nil
}
