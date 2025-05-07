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

	// Import the postgres database driver.
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/syncapi/storage/postgres/deltas"
	"github.com/antinvestor/matrix/syncapi/storage/shared"
	_ "github.com/lib/pq"
)

// SyncServerDatasource represents a sync server datasource which manages
// both the database for PDUs and caches for EDUs.
type SyncServerDatasource struct {
	shared.Database
	db     *sqlutil.Connections
	writer sqlutil.Writer
}

// NewDatabase creates a new sync server database
func NewDatabase(ctx context.Context, cm *sqlutil.Connections, dbProperties *config.DatabaseOptions) (*SyncServerDatasource, error) {
	var d SyncServerDatasource
	var err error
	
	// Use the connection manager for database operations
	d.db = cm
	
	// Initialize all database tables with the connection manager
	accountData, err := NewPostgresAccountDataTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	
	events, err := NewPostgresEventsTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	
	currState, err := NewPostgresCurrentRoomStateTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	
	invites, err := NewPostgresInvitesTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	
	peeks, err := NewPostgresPeeksTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	
	topology, err := NewPostgresTopologyTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	
	backwardExtremities, err := NewPostgresBackwardsExtremitiesTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	
	sendToDevice, err := NewPostgresSendToDeviceTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	
	filter, err := NewPostgresFilterTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	
	receipts, err := NewPostgresReceiptsTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	
	memberships, err := NewPostgresMembershipsTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	
	notificationData, err := NewPostgresNotificationDataTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	
	ignores, err := NewPostgresIgnoresTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	
	presence, err := NewPostgresPresenceTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	
	relations, err := NewPostgresRelationsTable(ctx, cm)
	if err != nil {
		return nil, err
	}

	// apply migrations which need multiple tables
	// Get a regular database connection for migrations
	dbConn := cm.Connection(ctx, false)
	m := sqlutil.NewMigrator(dbConn.DB())
	m.AddMigrations(
		sqlutil.Migration{
			Version: "syncapi: set history visibility for existing events",
			Up:      deltas.UpSetHistoryVisibility, // Requires current_room_state and output_room_events to be created.
		},
	)
	err = m.Up(ctx)
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
