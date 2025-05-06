// Copyright 2018 New Vector Ltd
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
	"errors"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
)

const outputRoomEventsTopologySchema = `
-- Stores output room events received from the roomserver.
CREATE TABLE IF NOT EXISTS syncapi_output_room_events_topology (
	-- The event ID for the event.
    event_id TEXT PRIMARY KEY,
	-- The place of the event in the room's topology. This can usually be determined
	-- from the event's depth.
	topological_position BIGINT NOT NULL,
	stream_position BIGINT NOT NULL,
    -- The 'room_id' key for the event.
    room_id TEXT NOT NULL
);
-- The topological order will be used in events selection and ordering
CREATE UNIQUE INDEX IF NOT EXISTS syncapi_event_topological_position_idx ON syncapi_output_room_events_topology(topological_position, stream_position, room_id);
`

const outputRoomEventsTopologySchemaRevert = `DROP TABLE IF EXISTS syncapi_output_room_events_topology;`

const insertEventInTopologySQL = "" +
	"INSERT INTO syncapi_output_room_events_topology (event_id, topological_position, room_id, stream_position)" +
	" VALUES ($1, $2, $3, $4)" +
	" ON CONFLICT (topological_position, stream_position, room_id) DO UPDATE SET event_id = $1" +
	" RETURNING topological_position"

const selectEventIDsInRangeASCSQL = "" +
	"SELECT event_id, topological_position, stream_position FROM syncapi_output_room_events_topology" +
	" WHERE room_id = $1 AND (" +
	"(topological_position > $2 AND topological_position < $3) OR" +
	"(topological_position = $4 AND stream_position >= $5)" +
	") ORDER BY topological_position ASC, stream_position ASC LIMIT $6"

const selectEventIDsInRangeDESCSQL = "" +
	"SELECT event_id, topological_position, stream_position FROM syncapi_output_room_events_topology" +
	" WHERE room_id = $1 AND (" +
	"(topological_position > $2 AND topological_position < $3) OR" +
	"(topological_position = $4 AND stream_position <= $5)" +
	") ORDER BY topological_position DESC, stream_position DESC LIMIT $6"

const selectPositionInTopologySQL = "" +
	"SELECT topological_position, stream_position FROM syncapi_output_room_events_topology" +
	" WHERE event_id = $1"

const selectStreamToTopologicalPositionAscSQL = "" +
	"SELECT topological_position FROM syncapi_output_room_events_topology WHERE room_id = $1 AND stream_position >= $2 ORDER BY topological_position ASC LIMIT 1;"

const selectStreamToTopologicalPositionDescSQL = "" +
	"SELECT topological_position FROM syncapi_output_room_events_topology WHERE room_id = $1 AND stream_position <= $2 ORDER BY topological_position DESC LIMIT 1;"

const purgeEventsTopologySQL = "" +
	"DELETE FROM syncapi_output_room_events_topology WHERE room_id = $1"

// outputRoomEventsTopologyTable implements tables.Topology using a connection manager and SQL constants.
type outputRoomEventsTopologyTable struct {
	cm *sqlutil.Connections
	insertEventInTopologySQL string
	selectEventIDsInRangeASCSQL string
	selectEventIDsInRangeDESCSQL string
	selectPositionInTopologySQL string
	selectStreamToTopologicalPositionAscSQL string
	selectStreamToTopologicalPositionDescSQL string
	purgeEventsTopologySQL string
}

// NewPostgresTopologyTable creates a new Topology table using a connection manager.
func NewPostgresTopologyTable(cm *sqlutil.Connections) tables.Topology {
	return &outputRoomEventsTopologyTable{
		cm: cm,
		insertEventInTopologySQL: insertEventInTopologySQL,
		selectEventIDsInRangeASCSQL: selectEventIDsInRangeASCSQL,
		selectEventIDsInRangeDESCSQL: selectEventIDsInRangeDESCSQL,
		selectPositionInTopologySQL: selectPositionInTopologySQL,
		selectStreamToTopologicalPositionAscSQL: selectStreamToTopologicalPositionAscSQL,
		selectStreamToTopologicalPositionDescSQL: selectStreamToTopologicalPositionDescSQL,
		purgeEventsTopologySQL: purgeEventsTopologySQL,
	}
}

// InsertEventInTopology inserts the given event in the room's topology, based on the event's depth.
func (t *outputRoomEventsTopologyTable) InsertEventInTopology(ctx context.Context, event *rstypes.HeaderedEvent, pos types.StreamPosition) (topoPos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, false)
	err = db.Raw(t.insertEventInTopologySQL, event.EventID(), event.Depth(), event.RoomID().String(), pos).Row().Scan(&topoPos)
	return
}

// SelectEventIDsInRange selects the IDs of events which positions are within a given range in a given room's topological order.
func (t *outputRoomEventsTopologyTable) SelectEventIDsInRange(ctx context.Context, roomID string, minDepth, maxDepth, maxStreamPos types.StreamPosition, limit int, chronologicalOrder bool) (eventIDs []string, start, end types.TopologyToken, err error) {
	db := t.cm.Connection(ctx, true)
	var rows *sql.Rows
	if chronologicalOrder {
		rows, err = db.Raw(t.selectEventIDsInRangeASCSQL, roomID, minDepth, maxDepth, maxDepth, maxStreamPos, limit).Rows()
	} else {
		rows, err = db.Raw(t.selectEventIDsInRangeDESCSQL, roomID, minDepth, maxDepth, maxDepth, maxStreamPos, limit).Rows()
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []string{}, start, end, nil
		}
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectEventIDsInRange: rows.close() failed")
	var eventID string
	var token types.TopologyToken
	var tokens []types.TopologyToken
	for rows.Next() {
		if err = rows.Scan(&eventID, &token.Depth, &token.PDUPosition); err != nil {
			return
		}
		eventIDs = append(eventIDs, eventID)
		tokens = append(tokens, token)
	}
	if len(tokens) > 0 {
		start = tokens[0]
		end = tokens[len(tokens)-1]
	}
	return eventIDs, start, end, rows.Err()
}

// SelectPositionInTopology returns the position of a given event in the topology of the room it belongs to.
func (t *outputRoomEventsTopologyTable) SelectPositionInTopology(ctx context.Context, eventID string) (pos, spos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, true)
	err = db.Raw(t.selectPositionInTopologySQL, eventID).Row().Scan(&pos, &spos)
	return
}

// SelectStreamToTopologicalPosition returns the closest position of a given event in the topology from the given stream position.
func (t *outputRoomEventsTopologyTable) SelectStreamToTopologicalPosition(ctx context.Context, roomID string, streamPos types.StreamPosition, backwardOrdering bool) (topoPos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, true)
	if backwardOrdering {
		err = db.Raw(t.selectStreamToTopologicalPositionDescSQL, roomID, streamPos).Row().Scan(&topoPos)
	} else {
		err = db.Raw(t.selectStreamToTopologicalPositionAscSQL, roomID, streamPos).Row().Scan(&topoPos)
	}
	return
}

// PurgeEventsTopology deletes all topology events for a given room.
func (t *outputRoomEventsTopologyTable) PurgeEventsTopology(ctx context.Context, roomID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.purgeEventsTopologySQL, roomID).Error
}
