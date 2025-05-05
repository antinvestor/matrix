// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Global.org Foundation C.I.C.
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
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/shared"
	"github.com/pitabwire/frame"
)

// A Database is used to store room events and stream offsets.
type Database struct {
	shared.Database
}

var Migrations = []frame.MigrationPatch{
	{
		Name:        "roomserver_001_create_event_json_table",
		Patch:       eventJSONSchema,
		RevertPatch: eventJSONSchemaRevert,
	},
	{
		Name:        "roomserver_002_create_event_state_keys_table",
		Patch:       eventStateKeysSchema,
		RevertPatch: eventStateKeysSchemaRevert,
	},
	{
		Name:        "roomserver_003_create_event_types_table",
		Patch:       eventTypesSchema,
		RevertPatch: eventTypesSchemaRevert,
	},
	{
		Name:        "roomserver_004_create_events_table",
		Patch:       eventsSchema,
		RevertPatch: eventsSchemaRevert,
	},
	{
		Name:        "roomserver_005_create_invite_table",
		Patch:       inviteSchema,
		RevertPatch: inviteSchemaRevert,
	},
	{
		Name:        "roomserver_006_create_membership_table",
		Patch:       membershipSchema,
		RevertPatch: membershipSchemaRevert,
	},
	{
		Name:        "roomserver_007_create_previous_events_table",
		Patch:       previousEventSchema,
		RevertPatch: previousEventSchemaRevert,
	},
	{
		Name:        "roomserver_008_create_published_table",
		Patch:       publishedSchema,
		RevertPatch: publishedSchemaRevert,
	},
	{
		Name:        "roomserver_009_create_redactions_table",
		Patch:       redactionsSchema,
		RevertPatch: redactionsSchemaRevert,
	},
	{
		Name:        "roomserver_010_create_reported_events_table",
		Patch:       reportedEventsSchema,
		RevertPatch: reportedEventsSchemaRevert,
	},
	{
		Name:        "roomserver_011_create_room_aliases_table",
		Patch:       roomAliasesSchema,
		RevertPatch: roomAliasesSchemaRevert,
	},
	{
		Name:        "roomserver_012_create_rooms_table",
		Patch:       roomsSchema,
		RevertPatch: roomsSchemaRevert,
	},
	{
		Name:        "roomserver_013_create_state_block_table",
		Patch:       stateDataSchema,
		RevertPatch: stateDataSchemaRevert,
	},
	{
		Name:        "roomserver_014_create_state_snapshot_table",
		Patch:       stateSnapshotSchema,
		RevertPatch: stateSnapshotSchemaRevert,
	},
	{
		Name:        "roomserver_015_create_user_room_keys_table",
		Patch:       userRoomKeysSchema,
		RevertPatch: userRoomKeysSchemaRevert,
	},
}

// Open a postgres database.
func Open(ctx context.Context, cm *sqlutil.Connections, cache caching.RoomServerCaches) (*Database, error) {
	var d Database

	err := cm.MigrateStrings(ctx, Migrations...)
	if err != nil {
		return nil, err
	}

	// Then prepare the statements. Now that the migrations have run, any columns referred
	// to in the database code should now exist.
	if err := d.prepare(ctx, cm, cache); err != nil {
		return nil, err
	}

	return &d, nil
}

func (d *Database) prepare(ctx context.Context, cm *sqlutil.Connections, cache caching.RoomServerCaches) error {
	eventStateKeys := NewPostgresEventStateKeysTable(cm)

	eventTypes := NewPostgresEventTypesTable(cm)

	eventJSON := NewPostgresEventJSONTable(cm)

	events := NewPostgresEventsTable(cm)

	rooms := NewPostgresRoomsTable(cm)

	stateBlock := NewPostgresStateBlockTable(cm)

	stateSnapshot := NewPostgresStateSnapshotTable(cm)

	prevEvents := NewPostgresPreviousEventsTable(cm)

	roomAliases := NewPostgresRoomAliasesTable(cm)

	invites := NewPostgresInvitesTable(cm)

	membership := NewPostgresMembershipTable(cm)

	published := NewPostgresPublishedTable(cm)

	redactions := NewPostgresRedactionsTable(cm)

	purge := NewPostgresPurgeTable(cm)
	userRoomKeys := NewPostgresUserRoomKeysTable(cm)

	reportedEvents := NewPostgresReportedEventsTable(cm)

	d.Database = shared.Database{
		Pool: cm,
		EventDatabase: shared.EventDatabase{
			Pool:                cm,
			Cache:               cache,
			EventsTable:         events,
			EventJSONTable:      eventJSON,
			EventTypesTable:     eventTypes,
			EventStateKeysTable: eventStateKeys,
			PrevEventsTable:     prevEvents,
			RedactionsTable:     redactions,
			ReportedEventsTable: reportedEvents,
		},
		Cache:              cache,
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
