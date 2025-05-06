// Copyright 2021 The Global.org Foundation C.I.C.
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
	"fmt"
	"github.com/antinvestor/matrix/internal"

	"github.com/antinvestor/matrix/internal/sqlutil"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
)

// The memberships table is designed to track the last time that
// the user was a given state. This allows us to find out the
// most recent time that a user was invited to, joined or left
// a room, either by choice or otherwise. This is important for
// building history visibility.

const membershipsSchema = `
CREATE TABLE IF NOT EXISTS syncapi_memberships (
    -- The 'room_id' key for the state event.
    room_id TEXT NOT NULL,
    -- The state event ID
	user_id TEXT NOT NULL,
	-- The status of the membership
	membership TEXT NOT NULL,
	-- The event ID that last changed the membership
	event_id TEXT NOT NULL,
	-- The stream position of the change
	stream_pos BIGINT NOT NULL,
	-- The topological position of the change in the room
	topological_pos BIGINT NOT NULL,
	-- Unique index
	CONSTRAINT syncapi_memberships_unique UNIQUE (room_id, user_id, membership)
);
`

const membershipsSchemaRevert = `DROP TABLE IF EXISTS syncapi_memberships;`

const upsertMembershipSQL = "" +
	"INSERT INTO syncapi_memberships (room_id, user_id, membership, event_id, stream_pos, topological_pos)" +
	" VALUES ($1, $2, $3, $4, $5, $6)" +
	" ON CONFLICT ON CONSTRAINT syncapi_memberships_unique" +
	" DO UPDATE SET event_id = $4, stream_pos = $5, topological_pos = $6"

const selectMembershipCountSQL = "" +
	"SELECT COUNT(*) FROM (" +
	" SELECT DISTINCT ON (room_id, user_id) room_id, user_id, membership FROM syncapi_memberships WHERE room_id = $1 AND stream_pos <= $2 ORDER BY room_id, user_id, stream_pos DESC" +
	") t WHERE t.membership = $3"

const selectMembershipBeforeSQL = "" +
	"SELECT membership, topological_pos FROM syncapi_memberships WHERE room_id = $1 and user_id = $2 AND topological_pos <= $3 ORDER BY topological_pos DESC LIMIT 1"

const purgeMembershipsSQL = "" +
	"DELETE FROM syncapi_memberships WHERE room_id = $1"

const selectMembersSQL = `
	SELECT event_id FROM (
		SELECT DISTINCT ON (room_id, user_id) room_id, user_id, event_id, membership FROM syncapi_memberships WHERE room_id = $1 AND topological_pos <= $2 ORDER BY room_id, user_id, stream_pos DESC  
	) t 
	WHERE ($3::text IS NULL OR t.membership = $3)
		AND ($4::text IS NULL OR t.membership <> $4)
`

// membershipsTable implements tables.Memberships using a connection manager and SQL constants.
type membershipsTable struct {
	cm                        *sqlutil.Connections
	upsertMembershipSQL       string
	selectMembershipCountSQL  string
	selectMembershipBeforeSQL string
	purgeMembershipsSQL       string
	selectMembersSQL          string
}

// NewPostgresMembershipsTable creates a new Memberships table using a connection manager.
func NewPostgresMembershipsTable(cm *sqlutil.Connections) tables.Memberships {
	return &membershipsTable{
		cm:                        cm,
		upsertMembershipSQL:       upsertMembershipSQL,
		selectMembershipCountSQL:  selectMembershipCountSQL,
		selectMembershipBeforeSQL: selectMembershipBeforeSQL,
		purgeMembershipsSQL:       purgeMembershipsSQL,
		selectMembersSQL:          selectMembersSQL,
	}
}

// UpsertMembership inserts or updates a membership event for a user in a room.
func (t *membershipsTable) UpsertMembership(
	ctx context.Context,
	event *rstypes.HeaderedEvent,
	streamPos, topologicalPos types.StreamPosition,
) error {
	membership, err := event.Membership()
	if err != nil {
		return fmt.Errorf("event.Membership: %w", err)
	}
	db := t.cm.Connection(ctx, false)
	return db.Exec(
		t.upsertMembershipSQL,
		event.RoomID().String(),
		event.StateKeyResolved,
		membership,
		event.EventID(),
		streamPos,
		topologicalPos,
	).Error
}

// SelectMembershipCount returns the count of users with a given membership in a room up to a stream position.
func (t *membershipsTable) SelectMembershipCount(
	ctx context.Context,
	roomID, membership string,
	pos types.StreamPosition,
) (count int, err error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectMembershipCountSQL, roomID, pos, membership).Row()
	err = row.Scan(&count)
	return
}

// SelectMembershipForUser returns the membership of the user before and including the given position.
// If no membership can be found, returns "leave", the topological position and no error.
// If an error occurs, other than sql.ErrNoRows, returns that and an empty string as the membership.
func (t *membershipsTable) SelectMembershipForUser(
	ctx context.Context,
	roomID, userID string,
	pos int64,
) (membership string, topologyPos int64, err error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectMembershipBeforeSQL, roomID, userID, pos).Row()
	err = row.Scan(&membership, &topologyPos)
	if err != nil {
		if err == sql.ErrNoRows {
			return "leave", 0, nil
		}
		return "", 0, err
	}
	return membership, topologyPos, nil
}

// PurgeMemberships deletes all memberships for a given room.
func (t *membershipsTable) PurgeMemberships(
	ctx context.Context,
	roomID string,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.purgeMembershipsSQL, roomID).Error
}

// SelectMemberships returns event IDs for memberships in a room at a given topology position, optionally filtered by membership type.
func (t *membershipsTable) SelectMemberships(
	ctx context.Context,
	roomID string,
	pos types.TopologyToken,
	membership, notMembership *string,
) (eventIDs []string, err error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectMembersSQL, roomID, pos.Depth, membership, notMembership).Rows()
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "failed to close rows")
	var eventID string
	for rows.Next() {
		if err = rows.Scan(&eventID); err != nil {
			return
		}
		eventIDs = append(eventIDs, eventID)
	}
	return eventIDs, rows.Err()
}
