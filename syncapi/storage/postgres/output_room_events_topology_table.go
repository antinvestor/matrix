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

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/pitabwire/frame"
)

// Schema for the output_room_events_topology table
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

// Revert schema for the output_room_events_topology table
const outputRoomEventsTopologySchemaRevert = `
DROP TABLE IF EXISTS syncapi_output_room_events_topology;
`

// SQL query to insert an event into the topology table
const insertEventInTopologySQL = "" +
	"INSERT INTO syncapi_output_room_events_topology (event_id, topological_position, room_id, stream_position)" +
	" VALUES ($1, $2, $3, $4)" +
	" ON CONFLICT (topological_position, stream_position, room_id) DO UPDATE SET event_id = $1" +
	" RETURNING topological_position"

// SQL query to select event IDs in ascending order within a range
const selectEventIDsInRangeASCSQL = "" +
	"SELECT event_id, topological_position, stream_position FROM syncapi_output_room_events_topology" +
	" WHERE room_id = $1 AND (" +
	"(topological_position > $2 AND topological_position < $3) OR" +
	"(topological_position = $4 AND stream_position >= $5)" +
	") ORDER BY topological_position ASC, stream_position ASC LIMIT $6"

// SQL query to select event IDs in descending order within a range
const selectEventIDsInRangeDESCSQL = "" +
	"SELECT event_id, topological_position, stream_position FROM syncapi_output_room_events_topology" +
	" WHERE room_id = $1 AND (" +
	"(topological_position > $2 AND topological_position < $3) OR" +
	"(topological_position = $4 AND stream_position <= $5)" +
	") ORDER BY topological_position DESC, stream_position DESC LIMIT $6"

// SQL query to select the position of an event in the topology
const selectPositionInTopologySQL = "" +
	"SELECT topological_position, stream_position FROM syncapi_output_room_events_topology" +
	" WHERE event_id = $1"

// SQL query to select topological position from stream position in ascending order
const selectStreamToTopologicalPositionAscSQL = "" +
	"SELECT topological_position FROM syncapi_output_room_events_topology WHERE room_id = $1 AND stream_position >= $2 ORDER BY topological_position ASC LIMIT 1;"

// SQL query to select topological position from stream position in descending order
const selectStreamToTopologicalPositionDescSQL = "" +
	"SELECT topological_position FROM syncapi_output_room_events_topology WHERE room_id = $1 AND stream_position <= $2 ORDER BY topological_position DESC LIMIT 1;"

// SQL query to purge events for a room
const purgeEventsTopologySQL = "" +
	"DELETE FROM syncapi_output_room_events_topology WHERE room_id = $1"

// outputRoomEventsTopologyTable represents a table storing topology of room events
type outputRoomEventsTopologyTable struct {
	cm sqlutil.ConnectionManager

	insertEventInTopologySQL                 string
	selectEventIDsInRangeASCSQL              string
	selectEventIDsInRangeDESCSQL             string
	selectPositionInTopologySQL              string
	selectStreamToTopologicalPositionAscSQL  string
	selectStreamToTopologicalPositionDescSQL string
	purgeEventsTopologySQL                   string
}

// NewPostgresTopologyTable creates a new topology table
func NewPostgresTopologyTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.Topology, error) {

	// Run migrations
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "syncapi_output_room_events_topology_table_schema_001",
		Patch:       outputRoomEventsTopologySchema,
		RevertPatch: outputRoomEventsTopologySchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	t := &outputRoomEventsTopologyTable{
		cm: cm,

		// Initialise SQL query fields
		insertEventInTopologySQL:                 insertEventInTopologySQL,
		selectEventIDsInRangeASCSQL:              selectEventIDsInRangeASCSQL,
		selectEventIDsInRangeDESCSQL:             selectEventIDsInRangeDESCSQL,
		selectPositionInTopologySQL:              selectPositionInTopologySQL,
		selectStreamToTopologicalPositionAscSQL:  selectStreamToTopologicalPositionAscSQL,
		selectStreamToTopologicalPositionDescSQL: selectStreamToTopologicalPositionDescSQL,
		purgeEventsTopologySQL:                   purgeEventsTopologySQL,
	}

	return t, nil
}

// InsertEventInTopology inserts the given event in the room's topology, based
// on the event's depth.
func (t *outputRoomEventsTopologyTable) InsertEventInTopology(
	ctx context.Context, event *rstypes.HeaderedEvent, pos types.StreamPosition,
) (topoPos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, false)
	row := db.Raw(
		t.insertEventInTopologySQL,
		event.EventID(),
		event.Depth(),
		event.RoomID().String(),
		pos,
	).Row()
	err = row.Scan(&topoPos)
	return
}

// SelectEventIDsInRange selects the IDs of events which positions are within a
// given range in a given room's topological order. Returns the start/end topological tokens for
// the returned eventIDs.
// Returns an empty slice if no events match the given range.
func (t *outputRoomEventsTopologyTable) SelectEventIDsInRange(
	ctx context.Context, roomID string, minDepth, maxDepth, maxStreamPos types.StreamPosition,
	limit int, chronologicalOrder bool,
) (eventIDs []string, start, end types.TopologyToken, err error) {
	db := t.cm.Connection(ctx, true)

	// Decide on the selection's order according to whether chronological order
	// is requested or not.
	var query string
	if chronologicalOrder {
		query = t.selectEventIDsInRangeASCSQL
	} else {
		query = t.selectEventIDsInRangeDESCSQL
	}

	// Query the event IDs.
	rows, err := db.Raw(query, roomID, minDepth, maxDepth, maxDepth, maxStreamPos, limit).Rows()
	if sqlutil.ErrorIsNoRows(err) {
		// If no event matched the request, return an empty slice.
		return []string{}, start, end, nil
	} else if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectEventIDsInRange: rows.close() failed")

	// Return the IDs.
	var eventID string
	var token types.TopologyToken
	var tokens []types.TopologyToken
	for rows.Next() {
		err = rows.Scan(&eventID, &token.Depth, &token.PDUPosition)
		if err != nil {
			return
		}
		eventIDs = append(eventIDs, eventID)
		tokens = append(tokens, token)
	}

	// The values are already ordered by SQL, so we can use them as is.
	if len(tokens) > 0 {
		start = tokens[0]
		end = tokens[len(tokens)-1]
	}

	return eventIDs, start, end, rows.Err()
}

// SelectPositionInTopology returns the position of a given event in the
// topology of the room it belongs to.
func (t *outputRoomEventsTopologyTable) SelectPositionInTopology(
	ctx context.Context, eventID string,
) (pos, spos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectPositionInTopologySQL, eventID).Row()
	err = row.Scan(&pos, &spos)
	return
}

// SelectStreamToTopologicalPosition returns the closest position of a given event
// in the topology of the room it belongs to from the given stream position.
func (t *outputRoomEventsTopologyTable) SelectStreamToTopologicalPosition(
	ctx context.Context, roomID string, streamPos types.StreamPosition, backwardOrdering bool,
) (topoPos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, true)

	var row *sql.Row
	if backwardOrdering {
		row = db.Raw(t.selectStreamToTopologicalPositionDescSQL, roomID, streamPos).Row()
	} else {
		row = db.Raw(t.selectStreamToTopologicalPositionAscSQL, roomID, streamPos).Row()
	}
	err = row.Scan(&topoPos)
	return
}

// PurgeEventsTopology removes all events for a room ID
func (t *outputRoomEventsTopologyTable) PurgeEventsTopology(
	ctx context.Context, roomID string,
) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.purgeEventsTopologySQL, roomID)
	return result.Error
}
