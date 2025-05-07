// Copyright 2020 The Matrix.org Foundation C.I.C.
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

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/postgres/deltas"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

var keyChangesSchema = `
-- Stores key change information about users. Used to determine when to send updated device lists to clients.
CREATE SEQUENCE IF NOT EXISTS keyserver_key_changes_seq;
CREATE TABLE IF NOT EXISTS keyserver_key_changes (
	change_id BIGINT PRIMARY KEY DEFAULT nextval('keyserver_key_changes_seq'),
    user_id TEXT NOT NULL,
    CONSTRAINT keyserver_key_changes_unique_per_user UNIQUE (user_id)
);
`

// SQL query constants for key changes operations
const (
	upsertKeyChangeSQL = "INSERT INTO keyserver_key_changes (user_id)" +
		" VALUES ($1)" +
		" ON CONFLICT ON CONSTRAINT keyserver_key_changes_unique_per_user" +
		" DO UPDATE SET change_id = nextval('keyserver_key_changes_seq')" +
		" RETURNING change_id"

	selectKeyChangesSQL = "SELECT user_id, change_id FROM keyserver_key_changes WHERE change_id > $1 AND change_id <= $2"
)

type keyChangesTable struct {
	cm *sqlutil.Connections
	
	upsertKeyChangeStmt  string
	selectKeyChangesStmt string
}

func NewPostgresKeyChangesTable(ctx context.Context, cm *sqlutil.Connections) (tables.KeyChanges, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(keyChangesSchema).Error; err != nil {
		return nil, err
	}

	// Initialize migration
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	
	if err = executeMigration(ctx, sqlDB); err != nil {
		return nil, err
	}
	
	// Initialize table with SQL statements
	t := &keyChangesTable{
		cm:                  cm,
		upsertKeyChangeStmt: upsertKeyChangeSQL,
		selectKeyChangesStmt: selectKeyChangesSQL,
	}
	
	return t, nil
}

func executeMigration(ctx context.Context, db *sql.DB) error {
	// TODO: Remove when we are sure we are not having goose artefacts in the db
	// This forces an error, which indicates the migration is already applied, since the
	// column partition was removed from the table
	migrationName := "keyserver: refactor key changes"

	var cName string
	err := db.QueryRowContext(ctx, "select column_name from information_schema.columns where table_name = 'keyserver_key_changes' AND column_name = 'partition'").Scan(&cName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("failed to check for column: %w", err)
	}
	
	// If we are here, it means the column exists and we need to drop it
	m := sqlutil.NewMigrator(db)
	m.AddMigrations([]sqlutil.Migration{
		{
			Version: migrationName,
			Up:      deltas.UpKeyChangesRefactor,
		},
	})
	return m.Up(ctx)
}

func (t *keyChangesTable) InsertKeyChange(ctx context.Context, userID string) (changeID int64, err error) {
	db := t.cm.Connection(ctx, false)
	err = db.Raw(t.upsertKeyChangeStmt, userID).Row().Scan(&changeID)
	return
}

func (t *keyChangesTable) SelectKeyChanges(
	ctx context.Context, fromOffset, toOffset int64,
) (userIDs []string, latestOffset int64, err error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectKeyChangesStmt, fromOffset, toOffset).Rows()
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
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
