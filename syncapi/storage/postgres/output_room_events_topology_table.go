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
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
)

// SQL statements for the topology table
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

type outputRoomEventsTopologyTable struct {
	cm *sqlutil.Connections
	
	// SQL queries stored as struct fields
	insertEventInTopologyStmt              string
	selectEventIDsInRangeASCStmt           string
	selectEventIDsInRangeDESCStmt          string
	selectPositionInTopologyStmt           string
	selectStreamToTopologicalPositionAscStmt string
	selectStreamToTopologicalPositionDescStmt string
	purgeEventsTopologyStmt                string
}

func NewPostgresTopologyTable(ctx context.Context, cm *sqlutil.Connections) (tables.Topology, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(outputRoomEventsTopologySchema).Error; err != nil {
		return nil, err
	}

	return &outputRoomEventsTopologyTable{
		cm: cm,
		insertEventInTopologyStmt: "INSERT INTO syncapi_output_room_events_topology (event_id, topological_position, room_id, stream_position)" +
			" VALUES ($1, $2, $3, $4)" +
			" ON CONFLICT (topological_position, stream_position, room_id) DO UPDATE SET event_id = $1" +
			" RETURNING topological_position",
		selectEventIDsInRangeASCStmt: "SELECT event_id, topological_position, stream_position FROM syncapi_output_room_events_topology" +
			" WHERE room_id = $1 AND (" +
			"(topological_position > $2 AND topological_position < $3) OR" +
			"(topological_position = $4 AND stream_position >= $5)" +
			") ORDER BY topological_position ASC, stream_position ASC LIMIT $6",
		selectEventIDsInRangeDESCStmt: "SELECT event_id, topological_position, stream_position FROM syncapi_output_room_events_topology" +
			" WHERE room_id = $1 AND (" +
			"(topological_position > $2 AND topological_position < $3) OR" +
			"(topological_position = $4 AND stream_position <= $5)" +
			") ORDER BY topological_position DESC, stream_position DESC LIMIT $6",
		selectPositionInTopologyStmt: "SELECT topological_position, stream_position FROM syncapi_output_room_events_topology" +
			" WHERE event_id = $1",
		selectStreamToTopologicalPositionAscStmt: "SELECT topological_position FROM syncapi_output_room_events_topology WHERE room_id = $1 AND stream_position >= $2 ORDER BY topological_position ASC LIMIT 1;",
		selectStreamToTopologicalPositionDescStmt: "SELECT topological_position FROM syncapi_output_room_events_topology WHERE room_id = $1 AND stream_position <= $2 ORDER BY topological_position DESC LIMIT 1;",
		purgeEventsTopologyStmt: "DELETE FROM syncapi_output_room_events_topology WHERE room_id = $1",
	}, nil
}

// InsertEventInTopology inserts the given event in the room's topology, based
// on the event's depth.
func (s *outputRoomEventsTopologyTable) InsertEventInTopology(
	ctx context.Context, event *rstypes.HeaderedEvent, pos types.StreamPosition,
) (topoPos types.StreamPosition, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)

	// Execute query and scan the returned topological position
	err = db.Raw(
		s.insertEventInTopologyStmt,
		event.EventID(), event.Depth(), event.RoomID().String(), pos,
	).Row().Scan(&topoPos)

	return
}

// SelectEventIDsInRange selects the IDs of events which positions are within a
// given range in a given room's topological order. Returns the start/end topological tokens for
// the returned eventIDs.
// Returns an empty slice if no events match the given range.
func (s *outputRoomEventsTopologyTable) SelectEventIDsInRange(
	ctx context.Context, roomID string, minDepth, maxDepth, maxStreamPos types.StreamPosition,
	limit int, chronologicalOrder bool,
) (eventIDs []string, start, end types.TopologyToken, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)

	// Decide on the selection's order according to whether chronological order
	// is requested or not.
	var sqlQuery string
	if chronologicalOrder {
		sqlQuery = s.selectEventIDsInRangeASCStmt
	} else {
		sqlQuery = s.selectEventIDsInRangeDESCStmt
	}

	// Query the event IDs.
	rows, err := db.Raw(sqlQuery, roomID, minDepth, maxDepth, maxDepth, maxStreamPos, limit).Rows()
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectEventIDsInRange: rows.close() failed")

	// Return the IDs.
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

	if err = rows.Err(); err != nil {
		return
	}

	// The values are already ordered by SQL, so we can use them as is.
	if len(tokens) > 0 {
		start = tokens[0]
		end = tokens[len(tokens)-1]
	}

	return eventIDs, start, end, nil
}

// SelectPositionInTopology returns the position of a given event in the
// topology of the room it belongs to.
func (s *outputRoomEventsTopologyTable) SelectPositionInTopology(
	ctx context.Context, eventID string,
) (pos, spos types.StreamPosition, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)

	// Execute query and scan the returned positions
	err = db.Raw(s.selectPositionInTopologyStmt, eventID).Row().Scan(&pos, &spos)

	return
}

// SelectStreamToTopologicalPosition returns the closest position of a given event
// in the topology of the room it belongs to from the given stream position.
func (s *outputRoomEventsTopologyTable) SelectStreamToTopologicalPosition(
	ctx context.Context, roomID string, streamPos types.StreamPosition, backwardOrdering bool,
) (topoPos types.StreamPosition, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)

	// Choose the appropriate SQL query based on ordering
	var sqlQuery string
	if backwardOrdering {
		sqlQuery = s.selectStreamToTopologicalPositionDescStmt
	} else {
		sqlQuery = s.selectStreamToTopologicalPositionAscStmt
	}

	// Execute query and scan the returned topological position
	err = db.Raw(sqlQuery, roomID, streamPos).Row().Scan(&topoPos)

	return
}

// PurgeEventsTopology removes all event entries from the topology table for a room
func (s *outputRoomEventsTopologyTable) PurgeEventsTopology(
	ctx context.Context, roomID string,
) error {
	// Get database connection
	db := s.cm.Connection(ctx, false)

	// Execute purge query
	return db.Exec(s.purgeEventsTopologyStmt, roomID).Error
}
