// Copyright 2020 The Global.org Foundation C.I.C.
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
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/pitabwire/frame"
)

// keyChangesSchema defines the schema for key changes table
const keyChangesSchema = `
-- Stores key change information about users. Used to determine when to send updated device lists to clients.
CREATE SEQUENCE IF NOT EXISTS keyserver_key_changes_seq;
CREATE TABLE IF NOT EXISTS keyserver_key_changes (
	change_id BIGINT PRIMARY KEY DEFAULT nextval('keyserver_key_changes_seq'),
    user_id TEXT NOT NULL,
    CONSTRAINT keyserver_key_changes_unique_per_user UNIQUE (user_id)
);
`

// keyChangesSchemaRevert defines the revert operation for the key changes schema
const keyChangesSchemaRevert = `
DROP TABLE IF EXISTS keyserver_key_changes;
DROP SEQUENCE IF EXISTS keyserver_key_changes_seq;
`

// SQL query constants
const upsertKeyChangeSQL = `
INSERT INTO keyserver_key_changes (user_id)
VALUES ($1)
ON CONFLICT ON CONSTRAINT keyserver_key_changes_unique_per_user
DO UPDATE SET change_id = nextval('keyserver_key_changes_seq')
RETURNING change_id
`

const selectKeyChangesSQL = `
SELECT user_id, change_id FROM keyserver_key_changes WHERE change_id > $1 AND change_id <= $2
`

type keyChangesTable struct {
	cm                  sqlutil.ConnectionManager
	upsertKeyChangeSQL  string
	selectKeyChangesSQL string
}

// NewPostgresKeyChangesTable creates a new key changes table
func NewPostgresKeyChangesTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.KeyChanges, error) {

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "keyserver_key_changes_table_schema_001",
		Patch:       keyChangesSchema,
		RevertPatch: keyChangesSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	t := &keyChangesTable{
		cm:                  cm,
		upsertKeyChangeSQL:  upsertKeyChangeSQL,
		selectKeyChangesSQL: selectKeyChangesSQL,
	}

	return t, nil
}

// InsertKeyChange inserts a key change for the given user ID.
func (s *keyChangesTable) InsertKeyChange(ctx context.Context, userID string) (changeID int64, err error) {
	db := s.cm.Connection(ctx, false)
	return changeID, db.Raw(s.upsertKeyChangeSQL, userID).Row().Scan(&changeID)
}

// SelectKeyChanges selects key changes for all users from the given offset to the target offset.
func (s *keyChangesTable) SelectKeyChanges(
	ctx context.Context, fromOffset, toOffset int64,
) (userIDs []string, latestOffset int64, err error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectKeyChangesSQL, fromOffset, toOffset).Rows()
	if err != nil {
		return nil, 0, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectKeyChanges: rows.close() failed")

	userIDs = []string{}
	for rows.Next() {
		var userID string
		var offset int64
		if err = rows.Scan(&userID, &offset); err != nil {
			return nil, 0, err
		}
		userIDs = append(userIDs, userID)
		if offset > latestOffset {
			latestOffset = offset
		}
	}
	return userIDs, latestOffset, rows.Err()
}
