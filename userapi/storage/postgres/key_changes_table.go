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
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

// SQL: Create key changes table
const keyChangesSchema = `
-- Stores key change information about users. Used to determine when to send updated device lists to clients.
CREATE SEQUENCE IF NOT EXISTS keyserver_key_changes_seq;
CREATE TABLE IF NOT EXISTS keyserver_key_changes (
	change_id BIGINT PRIMARY KEY DEFAULT nextval('keyserver_key_changes_seq'),
    user_id TEXT NOT NULL,
    CONSTRAINT keyserver_key_changes_unique_per_user UNIQUE (user_id)
);
`

// SQL: Drop key changes table
const keyChangesSchemaRevert = "DROP TABLE IF EXISTS keyserver_key_changes CASCADE;"

// SQL: Insert key change
const upsertKeyChangeSQL = "" +
	"INSERT INTO keyserver_key_changes (user_id)" +
	" VALUES ($1)" +
	" ON CONFLICT ON CONSTRAINT keyserver_key_changes_unique_per_user" +
	" DO UPDATE SET change_id = nextval('keyserver_key_changes_seq')" +
	" RETURNING change_id"

// SQL: Select key changes
const selectKeyChangesSQL = "" +
	"SELECT user_id, change_id FROM keyserver_key_changes WHERE change_id > $1 AND change_id <= $2"

// keyChangesTable implements tables.KeyChangesTable using GORM and a connection manager.
type keyChangesTable struct {
	cm *sqlutil.Connections

	upsertKeyChangeSQL  string
	selectKeyChangesSQL string
}

// NewPostgresKeyChangesTable returns a new KeyChangesTable using the provided connection manager.
func NewPostgresKeyChangesTable(cm *sqlutil.Connections) tables.KeyChanges {
	return &keyChangesTable{
		cm:                  cm,
		upsertKeyChangeSQL:  upsertKeyChangeSQL,
		selectKeyChangesSQL: selectKeyChangesSQL,
	}
}

// InsertKeyChange inserts a key change record.
func (t *keyChangesTable) InsertKeyChange(ctx context.Context, userID string) (changeID int64, err error) {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.upsertKeyChangeSQL, userID)
	err = result.Error
	if err != nil {
		return
	}
	err = result.Row().Scan(&changeID)
	return
}

// SelectKeyChanges retrieves key changes for a user after a specific offset.
func (t *keyChangesTable) SelectKeyChanges(ctx context.Context, fromOffset, toOffset int64) (userIDs []string, latestOffset int64, err error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectKeyChangesSQL, fromOffset, toOffset).Rows()
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var userID string
		var offset int64
		if err = rows.Scan(&userID, &offset); err != nil {
			return
		}
		if offset > latestOffset {
			latestOffset = offset
		}
		userIDs = append(userIDs, userID)
	}
	err = rows.Err()
	return
}
