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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgres

import (
	"context"
	"database/sql"

	// Import the postgres database driver.
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/syncapi/storage/postgres/deltas"
	"github.com/antinvestor/matrix/syncapi/storage/shared"
	_ "github.com/lib/pq"
)

// Migrations - All syncapi migrations for the postgres module
var Migrations = []sqlutil.Migration{
	{
		Version:   "syncapi_001_create_account_data_table",
		QueryUp:   accountDataSchema,
		QueryDown: accountDataSchemaRevert,
	},
	{
		Version:   "syncapi_002_create_backwards_extremities_table",
		QueryUp:   backwardExtremitiesSchema,
		QueryDown: backwardExtremitiesSchemaRevert,
	},
	{
		Version:   "syncapi_003_create_current_room_state_table",
		QueryUp:   currentRoomStateSchema,
		QueryDown: currentRoomStateSchemaRevert,
	},
	{
		Version:   "syncapi_004_create_filter_table",
		QueryUp:   filterSchema,
		QueryDown: filterSchemaRevert,
	},
	{
		Version:   "syncapi_005_create_ignores_table",
		QueryUp:   ignoresSchema,
		QueryDown: ignoresSchemaRevert,
	},
	{
		Version:   "syncapi_006_create_invites_table",
		QueryUp:   inviteEventsSchema,
		QueryDown: invitesSchemaRevert,
	},
	{
		Version:   "syncapi_007_create_memberships_table",
		QueryUp:   membershipsSchema,
		QueryDown: membershipsSchemaRevert,
	},
	{
		Version:   "syncapi_008_create_notification_data_table",
		QueryUp:   notificationDataSchema,
		QueryDown: notificationDataSchemaRevert,
	},
	{
		Version:   "syncapi_009_create_output_room_events_table",
		QueryUp:   outputRoomEventsSchema,
		QueryDown: outputRoomEventsSchemaRevert,
	},
	{
		Version:   "syncapi_010_create_output_room_events_topology_table",
		QueryUp:   outputRoomEventsTopologySchema,
		QueryDown: outputRoomEventsTopologySchemaRevert,
	},
	{
		Version:   "syncapi_011_create_peeks_table",
		QueryUp:   peeksSchema,
		QueryDown: peeksSchemaRevert,
	},
	{
		Version:   "syncapi_012_create_presence_table",
		QueryUp:   presenceSchema,
		QueryDown: presenceSchemaRevert,
	},
	{
		Version:   "syncapi_013_create_receipt_table",
		QueryUp:   receiptsSchema,
		QueryDown: receiptSchemaRevert,
	},
	{
		Version:   "syncapi_014_create_relations_table",
		QueryUp:   relationsSchema,
		QueryDown: relationsSchemaRevert,
	},
	{
		Version:   "syncapi_015_create_send_to_device_table",
		QueryUp:   sendToDeviceSchema,
		QueryDown: sendToDeviceSchemaRevert,
	},
	{
		Version: "syncapi: set history visibility for existing events",
		Up:      deltas.UpSetHistoryVisibility, // Requires current_room_state and output_room_events to be created.
	},
}

// SyncServerDatasource represents a sync server datasource which manages
// both the database for PDUs and caches for EDUs.
type SyncServerDatasource struct {
	shared.Database
	db     *sql.DB
	writer sqlutil.Writer
}

// NewDatabase creates a new sync server database
func NewDatabase(ctx context.Context, cm *sqlutil.Connections, dbProperties *config.DatabaseOptions) (*SyncServerDatasource, error) {
	var d SyncServerDatasource
	var err error
	if d.db, d.writer, err = cm.Connection(ctx, dbProperties); err != nil {
		return nil, err
	}

	m := sqlutil.NewMigrator(d.db)
	m.AddMigrations(Migrations...)
	err = m.Up(ctx)
	if err != nil {
		return nil, err
	}

	accountData, err := NewPostgresAccountDataTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	events, err := NewPostgresEventsTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	currState, err := NewPostgresCurrentRoomStateTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	invites, err := NewPostgresInvitesTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	peeks, err := NewPostgresPeeksTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	topology, err := NewPostgresTopologyTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	backwardExtremities, err := NewPostgresBackwardsExtremitiesTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	sendToDevice, err := NewPostgresSendToDeviceTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	filter, err := NewPostgresFilterTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	receipts, err := NewPostgresReceiptsTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	memberships, err := NewPostgresMembershipsTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	notificationData, err := NewPostgresNotificationDataTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	ignores, err := NewPostgresIgnoresTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	presence, err := NewPostgresPresenceTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	relations, err := NewPostgresRelationsTable(ctx, d.db)
	if err != nil {
		return nil, err
	}

	d.Database = shared.Database{
		DB:                  d.db,
		Writer:              d.writer,
		Invites:             invites,
		Peeks:               peeks,
		AccountData:         accountData,
		OutputEvents:        events,
		Topology:            topology,
		CurrentRoomState:    currState,
		BackwardExtremities: backwardExtremities,
		Filter:              filter,
		SendToDevice:        sendToDevice,
		Receipts:            receipts,
		Memberships:         memberships,
		NotificationData:    notificationData,
		Ignores:             ignores,
		Presence:            presence,
		Relations:           relations,
	}
	return &d, nil
}
