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
	"fmt"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"

	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/antinvestor/matrix/userapi/types"
)

// SQL: Create the cross-signing keys table
const crossSigningKeysSchema = `
CREATE TABLE IF NOT EXISTS keyserver_cross_signing_keys (
    user_id TEXT NOT NULL,
	key_type SMALLINT NOT NULL,
	key_data TEXT NOT NULL,
	PRIMARY KEY (user_id, key_type)
);
`

// SQL: Drop the cross-signing keys table
const crossSigningKeysSchemaRevert = "DROP TABLE IF EXISTS keyserver_cross_signing_keys CASCADE;"

// SQL: Select all cross-signing keys for a user
const selectCrossSigningKeysForUserSQL = "SELECT key_type, key_data FROM keyserver_cross_signing_keys WHERE user_id = $1"

// SQL: Upsert (insert or update) a cross-signing key for a user
const upsertCrossSigningKeysForUserSQL = "INSERT INTO keyserver_cross_signing_keys (user_id, key_type, key_data) VALUES($1, $2, $3) ON CONFLICT (user_id, key_type) DO UPDATE SET key_data = $3"

// crossSigningKeysTable implements tables.CrossSigningKeys using GORM and a connection manager.
type crossSigningKeysTable struct {
	cm *sqlutil.Connections

	// SQL queries for cross-signing keys operations
	selectCrossSigningKeysForUserSQL string
	upsertCrossSigningKeysForUserSQL string
}

// NewPostgresCrossSigningKeysTable returns a new CrossSigningKeys table using the provided connection manager.
func NewPostgresCrossSigningKeysTable(cm *sqlutil.Connections) tables.CrossSigningKeys {
	return &crossSigningKeysTable{
		cm:                               cm,
		selectCrossSigningKeysForUserSQL: selectCrossSigningKeysForUserSQL,
		upsertCrossSigningKeysForUserSQL: upsertCrossSigningKeysForUserSQL,
	}
}

// SelectCrossSigningKeysForUser retrieves all cross-signing keys for a user.
// Returns a map of key purpose to key data.
func (t *crossSigningKeysTable) SelectCrossSigningKeysForUser(ctx context.Context, userID string) (types.CrossSigningKeyMap, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectCrossSigningKeysForUserSQL, userID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "failed to close rows")
	r := types.CrossSigningKeyMap{}
	for rows.Next() {
		var keyTypeInt int16
		var keyData spec.Base64Bytes
		if err := rows.Scan(&keyTypeInt, &keyData); err != nil {
			return nil, err
		}
		keyType, ok := types.KeyTypeIntToPurpose[keyTypeInt]
		if !ok {
			return nil, fmt.Errorf("unknown key purpose int %d", keyTypeInt)
		}
		r[keyType] = keyData
	}
	return r, nil
}

// UpsertCrossSigningKeysForUser inserts or updates cross-signing keys for a user.
func (t *crossSigningKeysTable) UpsertCrossSigningKeysForUser(ctx context.Context, userID string, keyType fclient.CrossSigningKeyPurpose, keyData spec.Base64Bytes) error {
	db := t.cm.Connection(ctx, false)
	keyTypeInt, ok := types.KeyTypePurposeToInt[keyType]
	if !ok {
		return fmt.Errorf("unknown key purpose %q", keyType)
	}
	result := db.Exec(t.upsertCrossSigningKeysForUserSQL, userID, keyTypeInt, keyData)
	return result.Error
}
