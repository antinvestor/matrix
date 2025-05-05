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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgres

import (
	"context"
	"github.com/pitabwire/frame"

	// Import the postgres database driver.
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/shared"
	_ "github.com/lib/pq"
)

// Migrations - All syncapi migrations for the postgres module
var Migrations = []frame.MigrationPatch{
	{
		Name:        "syncapi_001_create_account_data_table",
		Patch:       accountDataSchema,
		RevertPatch: accountDataSchemaRevert,
	},
	{
		Name:        "syncapi_002_create_backwards_extremities_table",
		Patch:       backwardExtremitiesSchema,
		RevertPatch: backwardExtremitiesSchemaRevert,
	},
	{
		Name:        "syncapi_003_create_current_room_state_table",
		Patch:       currentRoomStateSchema,
		RevertPatch: currentRoomStateSchemaRevert,
	},
	{
		Name:        "syncapi_004_create_filter_table",
		Patch:       filterSchema,
		RevertPatch: filterSchemaRevert,
	},
	{
		Name:        "syncapi_005_create_ignores_table",
		Patch:       ignoresSchema,
		RevertPatch: ignoresSchemaRevert,
	},
	{
		Name:        "syncapi_006_create_invites_table",
		Patch:       inviteEventsSchema,
		RevertPatch: invitesSchemaRevert,
	},
	{
		Name:        "syncapi_007_create_memberships_table",
		Patch:       membershipsSchema,
		RevertPatch: membershipsSchemaRevert,
	},
	{
		Name:        "syncapi_008_create_notification_data_table",
		Patch:       notificationDataSchema,
		RevertPatch: notificationDataSchemaRevert,
	},
	{
		Name:        "syncapi_009_create_output_room_events_table",
		Patch:       outputRoomEventsSchema,
		RevertPatch: outputRoomEventsSchemaRevert,
	},
	{
		Name:        "syncapi_010_create_output_room_events_topology_table",
		Patch:       outputRoomEventsTopologySchema,
		RevertPatch: outputRoomEventsTopologySchemaRevert,
	},
	{
		Name:        "syncapi_011_create_peeks_table",
		Patch:       peeksSchema,
		RevertPatch: peeksSchemaRevert,
	},
	{
		Name:        "syncapi_012_create_presence_table",
		Patch:       presenceSchema,
		RevertPatch: presenceSchemaRevert,
	},
	{
		Name:        "syncapi_013_create_receipt_table",
		Patch:       receiptsSchema,
		RevertPatch: receiptSchemaRevert,
	},
	{
		Name:        "syncapi_014_create_relations_table",
		Patch:       relationsSchema,
		RevertPatch: relationsSchemaRevert,
	},
	{
		Name:        "syncapi_015_create_send_to_device_table",
		Patch:       sendToDeviceSchema,
		RevertPatch: sendToDeviceSchemaRevert,
	},
}

// SyncServerDatasource represents a sync server datasource which manages
// both the database for PDUs and caches for EDUs.
type SyncServerDatasource struct {
	shared.Database
}

// NewDatabase creates a new sync server database
func NewDatabase(ctx context.Context, cm *sqlutil.Connections) (*SyncServerDatasource, error) {
	var d SyncServerDatasource

	err := cm.MigrateStrings(ctx, Migrations...)
	if err != nil {
		return nil, err
	}

	accountData := NewPostgresAccountDataTable(cm)
	events := NewPostgresEventsTable(cm)
	currState := NewPostgresCurrentRoomStateTable(cm)
	invites := NewPostgresInvitesTable(cm)
	peeks := NewPostgresPeeksTable(cm)
	topology := NewPostgresTopologyTable(cm)
	backwardExtremities := NewPostgresBackwardsExtremitiesTable(cm)
	sendToDevice := NewPostgresSendToDeviceTable(cm)
	filter := NewPostgresFilterTable(cm)
	receipts := NewPostgresReceiptsTable(cm)
	memberships := NewPostgresMembershipsTable(cm)
	notificationData := NewPostgresNotificationDataTable(cm)
	ignores := NewPostgresIgnoresTable(cm)
	presence := NewPostgresPresenceTable(cm)
	relations := NewPostgresRelationsTable(cm)

	d.Database = shared.Database{
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
