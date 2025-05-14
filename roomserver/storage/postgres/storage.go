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
	// Import the postgres database driver.
	_ "github.com/lib/pq"

	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/shared"
)

// A Database is used to store room events and stream offsets.
type Database struct {
	shared.Database
}

// NewDatabase a postgres database.
func NewDatabase(ctx context.Context, cm sqlutil.ConnectionManager, cache caching.RoomServerCaches) (*Database, error) {
	var d Database

	eventsJSON, err := NewPostgresEventJSONTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	invites, err := NewPostgresInvitesTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	membership, err := NewPostgresMembershipTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	published, err := NewPostgresPublishedTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	redactions, err := NewPostgresRedactionsTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	roomAliases, err := NewPostgresRoomAliasesTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	prevEvents, err := NewPostgresPreviousEventsTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	eventStateKeys, err := NewPostgresEventStateKeysTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	eventTypes, err := NewPostgresEventTypesTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	rooms, err := NewPostgresRoomsTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	events, err := NewPostgresEventsTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	stateBlock, err := NewPostgresStateBlockTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	stateSnapshot, err := NewPostgresStateSnapshotTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	purge, err := NewPostgresPurgeTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	userRoomKeys, err := NewPostgresUserRoomKeysTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	reportedEvents, err := NewPostgresReportedEventsTable(ctx, cm)
	if err != nil {
		return nil, err
	}

	err = cm.Migrate(ctx)
	if err != nil {
		return nil, err
	}

	d.Database = shared.Database{
		EventDatabase: shared.EventDatabase{
			Cm:                  cm,
			Cache:               cache,
			EventsTable:         events,
			EventJSONTable:      eventsJSON,
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
	return &d, nil
}
