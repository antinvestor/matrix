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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implie
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
func Open(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions, cache caching.RoomServerCaches) (*Database, error) {
	var d Database
	var err error
	db, writer, err := conMan.Connection(ctx, dbProperties)
	if err != nil {
		return nil, fmt.Errorf("sqlutil.Open: %w", err)
	}

	// Create the tables.
	if err = d.create(ctx, db); err != nil {
		return nil, err
	}

	// Special case, since this migration uses several tables, so it needs to
	// be sure that all tables are created first.
	if err = executeMigration(ctx, db); err != nil {
		return nil, err
	}

	// Then prepare the statements. Now that the migrations have run, any columns referred
	// to in the database code should now exist.
	if err = d.prepare(ctx, db, writer, cache); err != nil {
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

func (d *Database) create(ctx context.Context, db *sql.DB) error {
	if err := CreateEventStateKeysTable(ctx, db); err != nil {
		return err
	}
	if err := CreateEventTypesTable(ctx, db); err != nil {
		return err
	}
	if err := CreateEventJSONTable(ctx, db); err != nil {
		return err
	}
	if err := CreateEventsTable(ctx, db); err != nil {
		return err
	}
	if err := CreateRoomsTable(ctx, db); err != nil {
		return err
	}
	if err := CreateStateBlockTable(ctx, db); err != nil {
		return err
	}
	if err := CreateStateSnapshotTable(ctx, db); err != nil {
		return err
	}
	if err := CreatePrevEventsTable(ctx, db); err != nil {
		return err
	}
	if err := CreateRoomAliasesTable(ctx, db); err != nil {
		return err
	}
	if err := CreateInvitesTable(ctx, db); err != nil {
		return err
	}
	if err := CreateMembershipTable(ctx, db); err != nil {
		return err
	}
	if err := CreatePublishedTable(ctx, db); err != nil {
		return err
	}
	if err := CreateRedactionsTable(ctx, db); err != nil {
		return err
	}
	if err := CreateUserRoomKeysTable(ctx, db); err != nil {
		return err
	}
	if err := CreateReportedEventsTable(ctx, db); err != nil {
		return err
	}

	return nil
}

func (d *Database) prepare(ctx context.Context, db *sql.DB, writer sqlutil.Writer, cache caching.RoomServerCaches) error {
	eventStateKeys, err := PrepareEventStateKeysTable(ctx, db)
	if err != nil {
		return err
	}
	eventTypes, err := PrepareEventTypesTable(ctx, db)
	if err != nil {
		return err
	}
	eventJSON, err := PrepareEventJSONTable(ctx, db)
	if err != nil {
		return err
	}
	events, err := PrepareEventsTable(ctx, db)
	if err != nil {
		return err
	}
	rooms, err := PrepareRoomsTable(ctx, db)
	if err != nil {
		return err
	}
	stateBlock, err := PrepareStateBlockTable(ctx, db)
	if err != nil {
		return err
	}
	stateSnapshot, err := PrepareStateSnapshotTable(ctx, db)
	if err != nil {
		return err
	}
	prevEvents, err := PreparePrevEventsTable(ctx, db)
	if err != nil {
		return err
	}
	roomAliases, err := PrepareRoomAliasesTable(ctx, db)
	if err != nil {
		return err
	}
	invites, err := PrepareInvitesTable(ctx, db)
	if err != nil {
		return err
	}
	membership, err := PrepareMembershipTable(ctx, db)
	if err != nil {
		return err
	}
	published, err := PreparePublishedTable(ctx, db)
	if err != nil {
		return err
	}
	redactions, err := PrepareRedactionsTable(ctx, db)
	if err != nil {
		return err
	}
	purge, err := PreparePurgeStatements(db)
	if err != nil {
		return err
	}
	userRoomKeys, err := PrepareUserRoomKeysTable(ctx, db)
	if err != nil {
		return err
	}
	reportedEvents, err := PrepareReportedEventsTable(ctx, db)
	if err != nil {
		return err
	}

	d.Database = shared.Database{
		DB: db,
		EventDatabase: shared.EventDatabase{
			DB:                  db,
			Cache:               cache,
			Writer:              writer,
			EventsTable:         events,
			EventJSONTable:      eventJSON,
			EventTypesTable:     eventTypes,
			EventStateKeysTable: eventStateKeys,
			PrevEventsTable:     prevEvents,
			RedactionsTable:     redactions,
			ReportedEventsTable: reportedEvents,
		},
		Cache:              cache,
		Writer:             writer,
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
