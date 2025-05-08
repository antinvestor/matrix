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

package sqlutil

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/antinvestor/matrix/internal"
	"github.com/sirupsen/logrus"
	"sync"
)

const createDBMigrationsSQL = "" +
	"CREATE TABLE IF NOT EXISTS db_migrations (" +
	" version TEXT PRIMARY KEY NOT NULL," +
	" time TEXT NOT NULL," +
	" dendrite_version TEXT NOT NULL" +
	");"

const insertVersionSQL = "" +
	"INSERT INTO db_migrations (version, time, dendrite_version)" +
	" VALUES ($1, $2, $3)"

const selectDBMigrationsSQL = "SELECT version FROM db_migrations"

const createNecessaryExtensionsSQL = `
	CREATE EXTENSION IF NOT EXISTS pg_search;
	CREATE EXTENSION IF NOT EXISTS pg_ivm;
	CREATE EXTENSION IF NOT EXISTS vector;
	CREATE EXTENSION IF NOT EXISTS postgis;
	CREATE EXTENSION IF NOT EXISTS postgis_topology;
	CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
	CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;`

// Migration defines a migration to be run.
type Migration struct {
	// Version is a simple description/name of this migration.
	Version string
	// Up defines the function to execute for an upgrade.
	Up func(ctx context.Context, txn *sql.Tx) error
	// Down defines the function to execute for a downgrade (not implemented yet).
	Down func(ctx context.Context, txn *sql.Tx) error
}

// Migrator contains fields required to run migrations.
type Migrator struct {
	db              *sql.DB
	migrations      []Migration
	knownMigrations map[string]struct{}
	mutex           *sync.Mutex
	insertStmt      *sql.Stmt
}

// NewMigrator creates a new Cm migrator.
func NewMigrator(db *sql.DB) *Migrator {
	return &Migrator{
		db:              db,
		migrations:      []Migration{},
		knownMigrations: make(map[string]struct{}),
		mutex:           &sync.Mutex{},
	}
}

// AddMigrations appends migrations to the list of migrations. Migrations are executed
// in the order they are added to the list. De-duplicates migrations using their Version field.
func (m *Migrator) AddMigrations(migrations ...Migration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, mig := range migrations {
		if _, ok := m.knownMigrations[mig.Version]; !ok {
			m.migrations = append(m.migrations, mig)
			m.knownMigrations[mig.Version] = struct{}{}
		}
	}
}

// Up executes all migrations in order they were added.
func (m *Migrator) Up(ctx context.Context) error {
	// ensure there is a table for known migrations
	executedMigrations, err := m.ExecutedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("unable to create/get migrations: %w", err)
	}
	// ensure we close the insert statement, as it's not needed anymore
	defer m.close(ctx)
	return WithTransaction(ctx, m.db, func(txn *sql.Tx) error {
		for i := range m.migrations {
			migration := m.migrations[i]
			// Skip migration if it was already executed
			if _, ok := executedMigrations[migration.Version]; ok {
				continue
			}
			logrus.Debugf("Executing database migration '%s'", migration.Version)

			if err = migration.Up(ctx, txn); err != nil {
				return fmt.Errorf("unable to execute migration '%s': %w", migration.Version, err)
			}
			if err = m.insertMigration(ctx, migration.Version); err != nil {
				return fmt.Errorf("unable to insert executed migrations: %w", err)
			}
		}
		return nil
	})
}

func (m *Migrator) insertMigration(ctx context.Context, migrationName string) error {
	if m.insertStmt == nil {
		var stmt *sql.Stmt
		var err error
		if txn == nil {
			stmt, err = m.db.PrepareContext(ctx, insertVersionSQL)
		} else {
			stmt, err = txn.PrepareContext(ctx, insertVersionSQL)
		}
		if err != nil {
			return fmt.Errorf("unable to prepare insert statement: %w", err)
		}
		m.insertStmt = stmt
	}
	//stmt := TxStmtContext(ctx, m.insertStmt)
	//_, err := stmt.ExecContext(ctx,
	//	migrationName,
	//	time.Now().Format(time.RFC3339),
	//	internal.VersionString(),
	//)
	return nil
}

// ExecutedMigrations returns a map with already executed migrations in addition to creating the
// migrations table, if it doesn't exist.
func (m *Migrator) ExecutedMigrations(ctx context.Context) (map[string]struct{}, error) {
	result := make(map[string]struct{})
	_, err := m.db.ExecContext(ctx, createNecessaryExtensionsSQL)
	if err != nil {
		return nil, fmt.Errorf("unable to create necessary extensions: %w", err)
	}
	_, err = m.db.ExecContext(ctx, createDBMigrationsSQL)
	if err != nil {
		return nil, fmt.Errorf("unable to create db_migrations: %w", err)
	}
	rows, err := m.db.QueryContext(ctx, selectDBMigrationsSQL)
	if err != nil {
		return nil, fmt.Errorf("unable to query db_migrations: %w", err)
	}
	defer internal.CloseAndLogIfError(ctx, rows, "ExecutedMigrations: rows.close() failed")
	var version string
	for rows.Next() {
		if err = rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("unable to scan version: %w", err)
		}
		result[version] = struct{}{}
	}

	return result, rows.Err()
}

// InsertMigration creates the migrations table if it doesn't exist and
// inserts a migration given their name to the database.
// This should only be used when manually inserting migrations.
func InsertMigration(ctx context.Context, db *sql.DB, migrationName string) error {
	m := NewMigrator(db)
	defer m.close(ctx)
	existingMigrations, err := m.ExecutedMigrations(ctx)
	if err != nil {
		return err
	}
	if _, ok := existingMigrations[migrationName]; ok {
		return nil
	}
	return m.insertMigration(ctx, nil, migrationName)
}

func (m *Migrator) close(ctx context.Context) {
	if m.insertStmt != nil {
		internal.CloseAndLogIfError(ctx, m.insertStmt, "unable to close insert statement")
	}
}
