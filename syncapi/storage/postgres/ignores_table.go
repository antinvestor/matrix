// Copyright 2022 The Matrix.org Foundation C.I.C.
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
	"encoding/json"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/pitabwire/frame"
)

// Schema for creating the ignores table
const ignoresSchema = `
-- Stores data about ignoress
CREATE TABLE IF NOT EXISTS syncapi_ignores (
	-- The user ID whose ignore list this belongs to.
	user_id TEXT NOT NULL,
	ignores_json TEXT NOT NULL,
	PRIMARY KEY(user_id)
);
`

// Revert schema for ignores table
const ignoresSchemaRevert = `
DROP TABLE IF EXISTS syncapi_ignores;
`

// SQL query to select user ignores
const selectIgnoresSQL = `
SELECT ignores_json FROM syncapi_ignores WHERE user_id = $1
`

// SQL query to insert or update user ignores
const upsertIgnoresSQL = `
INSERT INTO syncapi_ignores (user_id, ignores_json) VALUES ($1, $2)
ON CONFLICT (user_id) DO UPDATE set ignores_json = $2
`

// ignoresTable implements tables.Ignores
type ignoresTable struct {
	cm               sqlutil.ConnectionManager
	selectIgnoresSQL string
	upsertIgnoresSQL string
}

// NewPostgresIgnoresTable creates a new ignores table
func NewPostgresIgnoresTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.Ignores, error) {
	t := &ignoresTable{
		cm:               cm,
		selectIgnoresSQL: selectIgnoresSQL,
		upsertIgnoresSQL: upsertIgnoresSQL,
	}

	// Perform the migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "syncapi_ignores_table_schema_001",
		Patch:       ignoresSchema,
		RevertPatch: ignoresSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// SelectIgnores retrieves the ignored users for a given user
func (t *ignoresTable) SelectIgnores(
	ctx context.Context, userID string,
) (*types.IgnoredUsers, error) {
	var ignoresData []byte
	db := t.cm.Connection(ctx, true)

	row := db.Raw(t.selectIgnoresSQL, userID).Row()
	err := row.Scan(&ignoresData)
	if err != nil {
		return nil, err
	}

	var ignores types.IgnoredUsers
	if err = json.Unmarshal(ignoresData, &ignores); err != nil {
		return nil, err
	}
	return &ignores, nil
}

// UpsertIgnores updates or inserts the ignored users for a given user
func (t *ignoresTable) UpsertIgnores(
	ctx context.Context, userID string, ignores *types.IgnoredUsers,
) error {
	ignoresJSON, err := json.Marshal(ignores)
	if err != nil {
		return err
	}

	db := t.cm.Connection(ctx, false)
	return db.Exec(t.upsertIgnoresSQL, userID, ignoresJSON).Error
}
