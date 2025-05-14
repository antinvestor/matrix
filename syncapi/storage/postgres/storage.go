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

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/shared"
)

// SyncServerDatasource represents a sync server datasource which manages
// both the database for PDUs and caches for EDUs.
type SyncServerDatasource struct {
	shared.Database
}

// NewDatabase creates a new sync server database
func NewDatabase(ctx context.Context, cm sqlutil.ConnectionManager) (*SyncServerDatasource, error) {
	var d SyncServerDatasource

	// Initialise all tables with the connection manager
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

	err = cm.Migrate(ctx)
	if err != nil {
		return nil, err
	}

	d.Database = shared.Database{
		Cm:                  cm,
		AccountData:         accountData,
		OutputEvents:        events,
		Topology:            topology,
		CurrentRoomState:    currState,
		BackwardExtremities: backwardExtremities,
		Filter:              filter,
		SendToDevice:        sendToDevice,
		Invites:             invites,
		Peeks:               peeks,
		Receipts:            receipts,
		Memberships:         memberships,
		NotificationData:    notificationData,
		Ignores:             ignores,
		Presence:            presence,
		Relations:           relations,
	}
	return &d, nil
}
