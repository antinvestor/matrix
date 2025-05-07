// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Matrix.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	// Import the postgres database driver.
	_ "github.com/lib/pq"

	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres/deltas"
	"github.com/antinvestor/matrix/roomserver/storage/shared"
	"github.com/antinvestor/matrix/setup/config"
)

// A Database is used to store room events and stream offsets.
type Database struct {
	shared.Database
}

// Open a postgres database.
func Open(ctx context.Context, cm *sqlutil.Connections, dbProperties *config.DatabaseOptions, cache caching.RoomServerCaches) (*Database, error) {
	var d Database

	// Then prepare the statements. Now that the migrations have run, any columns referred
	// to in the database code should now exist.
	if err := d.prepare(ctx, cm, cache); err != nil {
		return nil, err
	}

	return &d, nil
}

func executeMigration(ctx context.Context, db *sql.DB) error {
	// TODO: Remove when we are sure we are not having goose artefacts in the db
	// This forces an error, which indicates the migration is already applied, since the
	// column event_nid was removed from the table
	migrationName := "roomserver: state blocks refactor"

	var cName string
	err := db.QueryRowContext(ctx, "select column_name from information_schema.columns where table_name = 'roomserver_state_block' AND column_name = 'event_nid'").Scan(&cName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) { // migration was already executed, as the column was removed
			if err = sqlutil.InsertMigration(ctx, db, migrationName); err != nil {
				return fmt.Errorf("unable to manually insert migration '%s': %w", migrationName, err)
			}
			return nil
		}
		return err
	}
	m := sqlutil.NewMigrator(db)
	m.AddMigrations(sqlutil.Migration{
		Version: migrationName,
		Up:      deltas.UpStateBlocksRefactor,
	})

	return m.Up(ctx)
}

func (d *Database) prepare(ctx context.Context, cm *sqlutil.Connections, cache caching.RoomServerCaches) error {
	// Use the new constructor method for eventStateKeys
	eventStateKeys, err := NewPostgresEventStateKeysTable(ctx, cm)
	if err != nil {
		return err
	}
	// Use the new constructor method for eventTypes
	eventTypes, err := NewPostgresEventTypesTable(ctx, cm)
	if err != nil {
		return err
	}
	// Use the new constructor method for eventJSON
	eventJSON, err := NewPostgresEventJSONTable(ctx, cm)
	if err != nil {
		return err
	}
	// Use the new constructor method for events
	events, err := NewPostgresEventsTable(ctx, cm)
	if err != nil {
		return err
	}
	// Use the new constructor method for rooms
	rooms, err := NewPostgresRoomsTable(ctx, cm)
	if err != nil {
		return err
	}
	// Use the new constructor method for stateBlock
	stateBlock, err := NewPostgresStateBlockTable(ctx, cm)
	if err != nil {
		return err
	}
	// Use the new constructor method for stateSnapshot
	stateSnapshot, err := NewPostgresStateSnapshotsTable(ctx, cm)
	if err != nil {
		return err
	}
	// Use the new constructor method for prevEvents
	prevEvents, err := NewPostgresPreviousEventsTable(ctx, cm)
	if err != nil {
		return err
	}
	// Use the new constructor method for roomAliases
	roomAliases, err := NewPostgresRoomAliasesTable(ctx, cm)
	if err != nil {
		return err
	}
	// Use the new consolidated constructor method for invites
	invites, err := NewPostgresInvitesTable(ctx, cm)
	if err != nil {
		return err
	}
	// Use the new constructor method for membership
	membership, err := NewPostgresMembershipTable(ctx, cm)
	if err != nil {
		return err
	}
	// Use the new constructor method for published
	published, err := NewPostgresPublishedTable(ctx, cm)
	if err != nil {
		return err
	}
	// Use the new constructor method for redactions
	redactions, err := NewPostgresRedactionsTable(ctx, cm)
	if err != nil {
		return err
	}
	purge, err := NewPostgresqlPurgeStatements(cm)
	if err != nil {
		return err
	}
	// Use the new constructor method for userRoomKeys
	userRoomKeys, err := NewPostgresUserRoomKeysTable(ctx, cm)
	if err != nil {
		return err
	}
	// Use the new constructor method for reportedEvents
	reportedEvents, err := NewPostgresReportedEventsTable(ctx, cm)
	if err != nil {
		return err
	}

	d.Database = shared.Database{
		Cm: cm,
		EventDatabase: shared.EventDatabase{
			Cm:                  cm,
			Cache:               cache,
			Writer:              cm.Writer,
			EventsTable:         events,
			EventJSONTable:      eventJSON,
			EventTypesTable:     eventTypes,
			EventStateKeysTable: eventStateKeys,
			PrevEventsTable:     prevEvents,
			RedactionsTable:     redactions,
			ReportedEventsTable: reportedEvents,
		},
		Cache:              cache,
		Writer:             cm.Writer,
		RoomsTable:         rooms,
		StateBlockTable:    stateBlock,
		StateSnapshotTable: stateSnapshot,
		RoomAliasesTable:   roomAliases,
		InvitesTable:       invites,
		MembershipTable:    membership,
		PublishedTable:     published,
		Purge:              purge,
		UserRoomKeyTable:   userRoomKeys,
	}
	return nil
}
