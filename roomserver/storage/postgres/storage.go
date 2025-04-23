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
	"fmt"

	// Import the postgres database driver.
	_ "github.com/lib/pq"

	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/shared"
	"github.com/antinvestor/matrix/setup/config"
)

// A Database is used to store room events and stream offsets.
type Database struct {
	shared.Database
}

var Migrations = []sqlutil.Migration{
	{
		Version:   "roomserver_001_create_event_json_table",
		QueryUp:   eventJSONSchema,
		QueryDown: eventJSONSchemaRevert,
	},
	{
		Version:   "roomserver_002_create_event_state_keys_table",
		QueryUp:   eventStateKeysSchema,
		QueryDown: eventStateKeysSchemaRevert,
	},
	{
		Version:   "roomserver_003_create_event_types_table",
		QueryUp:   eventTypesSchema,
		QueryDown: eventTypesSchemaRevert,
	},
	{
		Version:   "roomserver_004_create_events_table",
		QueryUp:   eventsSchema,
		QueryDown: eventsSchemaRevert,
	},
	{
		Version:   "roomserver_005_create_invite_table",
		QueryUp:   inviteSchema,
		QueryDown: inviteSchemaRevert,
	},
	{
		Version:   "roomserver_006_create_membership_table",
		QueryUp:   membershipSchema,
		QueryDown: membershipSchemaRevert,
	},
	{
		Version:   "roomserver_007_create_previous_events_table",
		QueryUp:   previousEventSchema,
		QueryDown: previousEventSchemaRevert,
	},
	{
		Version:   "roomserver_008_create_published_table",
		QueryUp:   publishedSchema,
		QueryDown: publishedSchemaRevert,
	},
	{
		Version:   "roomserver_009_create_redactions_table",
		QueryUp:   redactionsSchema,
		QueryDown: redactionsSchemaRevert,
	},
	{
		Version:   "roomserver_010_create_reported_events_table",
		QueryUp:   reportedEventsSchema,
		QueryDown: reportedEventsSchemaRevert,
	},
	{
		Version:   "roomserver_011_create_room_aliases_table",
		QueryUp:   roomAliasesSchema,
		QueryDown: roomAliasesSchemaRevert,
	},
	{
		Version:   "roomserver_012_create_rooms_table",
		QueryUp:   roomsSchema,
		QueryDown: roomsSchemaRevert,
	},
	{
		Version:   "roomserver_013_create_state_block_table",
		QueryUp:   stateDataSchema,
		QueryDown: stateDataSchemaRevert,
	},
	{
		Version:   "roomserver_014_create_state_snapshot_table",
		QueryUp:   stateSnapshotSchema,
		QueryDown: stateSnapshotSchemaRevert,
	},
	{
		Version:   "roomserver_015_create_user_room_keys_table",
		QueryUp:   userRoomKeysSchema,
		QueryDown: userRoomKeysSchemaRevert,
	},
}

// Open a postgres database.
func Open(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions, cache caching.RoomServerCaches) (*Database, error) {
	var d Database
	var err error
	db, writer, err := conMan.Connection(ctx, dbProperties)
	if err != nil {
		return nil, fmt.Errorf("sqlutil.Open: %w", err)
	}

	m := sqlutil.NewMigrator(db)
	m.AddMigrations(Migrations...)
	if err = m.Up(ctx); err != nil {
		return nil, err
	}

	// Then prepare the statements. Now that the migrations have run, any columns referred
	// to in the database code should now exist.
	if err = d.prepare(ctx, db, writer, cache); err != nil {
		return nil, err
	}

	return &d, nil
}

func (d *Database) prepare(ctx context.Context, db *sql.DB, writer sqlutil.Writer, cache caching.RoomServerCaches) error {
	eventStateKeys, err := NewPostgresEventStateKeysTable(ctx, db)
	if err != nil {
		return err
	}
	eventTypes, err := NewPostgresEventTypesTable(ctx, db)
	if err != nil {
		return err
	}
	eventJSON, err := NewPostgresEventJSONTable(ctx, db)
	if err != nil {
		return err
	}
	events, err := NewPostgresEventsTable(ctx, db)
	if err != nil {
		return err
	}
	rooms, err := NewPostgresRoomsTable(ctx, db)
	if err != nil {
		return err
	}
	stateBlock, err := NewPostgresStateBlockTable(ctx, db)
	if err != nil {
		return err
	}
	stateSnapshot, err := NewPostgresStateSnapshotTable(ctx, db)
	if err != nil {
		return err
	}
	prevEvents, err := NewPostgresPreviousEventsTable(ctx, db)
	if err != nil {
		return err
	}
	roomAliases, err := NewPostgresRoomAliasesTable(ctx, db)
	if err != nil {
		return err
	}
	invites, err := NewPostgresInvitesTable(ctx, db)
	if err != nil {
		return err
	}
	membership, err := NewPostgresMembershipTable(ctx, db)
	if err != nil {
		return err
	}
	published, err := NewPostgresPublishedTable(ctx, db)
	if err != nil {
		return err
	}
	redactions, err := NewPostgresRedactionsTable(ctx, db)
	if err != nil {
		return err
	}
	purge, err := NewPostgresPurgeStatements(db)
	if err != nil {
		return err
	}
	userRoomKeys, err := NewPostgresUserRoomKeysTable(ctx, db)
	if err != nil {
		return err
	}
	reportedEvents, err := NewPostgresReportedEventsTable(ctx, db)
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
