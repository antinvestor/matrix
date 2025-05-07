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
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres/deltas"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
)

const eventsSchema = `
-- The events table holds metadata for each event, the actual JSON is stored
-- separately to keep the size of the rows small.
CREATE SEQUENCE IF NOT EXISTS roomserver_event_nid_seq;
CREATE TABLE IF NOT EXISTS roomserver_events (
    -- Local numeric ID for the event.
    event_nid BIGINT PRIMARY KEY DEFAULT nextval('roomserver_event_nid_seq'),
    -- Local numeric ID for the room the event is in.
    -- This is never 0.
    room_nid BIGINT NOT NULL,
    -- Local numeric ID for the type of the event.
    -- This is never 0.
    event_type_nid BIGINT NOT NULL,
    -- Local numeric ID for the state_key of the event
    -- This is 0 if the event is not a state event.
    event_state_key_nid BIGINT NOT NULL,
    -- Whether the event has been written to the output log.
    sent_to_output BOOLEAN NOT NULL DEFAULT FALSE,
    -- Local numeric ID for the state at the event.
    -- This is 0 if we don't know the state at the event.
    -- If the state is not 0 then this event is part of the contiguous
    -- part of the event graph
    -- Since many different events can have the same state we store the
    -- state into a separate state table and refer to it by numeric ID.
    state_snapshot_nid BIGINT NOT NULL DEFAULT 0,
    -- Depth of the event in the event graph.
    depth BIGINT NOT NULL,
    -- The textual event id.
    -- Used to lookup the numeric ID when processing requests.
    -- Needed for state resolution.
    -- An event may only appear in this table once.
    event_id TEXT NOT NULL CONSTRAINT roomserver_event_id_unique UNIQUE,
    -- A list of numeric IDs for events that can authenticate this event.
	auth_event_nids BIGINT[] NOT NULL,
	is_rejected BOOLEAN NOT NULL DEFAULT FALSE
);

-- Create an index which helps in resolving membership events (event_type_nid = 5) - (used for history visibility)
CREATE INDEX IF NOT EXISTS roomserver_events_memberships_idx ON roomserver_events (room_nid, event_state_key_nid) WHERE (event_type_nid = 5);

-- The following indexes are used by bulkSelectStateEventByNIDSQL 
CREATE INDEX IF NOT EXISTS roomserver_event_event_type_nid_idx ON roomserver_events (event_type_nid);
CREATE INDEX IF NOT EXISTS roomserver_event_state_key_nid_idx ON roomserver_events (event_state_key_nid);
`

// SQL query constants for events table operations
const (
	// Insert a new event with associated metadata
	insertEventSQL = "" +
		"INSERT INTO roomserver_events (room_nid, event_type_nid, event_state_key_nid, event_id, auth_event_nids, depth, is_rejected)" +
		" VALUES ($1, $2, $3, $4, $5, $6, $7)" +
		" RETURNING event_nid, state_snapshot_nid"

	// Select event metadata by event ID
	selectEventSQL = "" +
		"SELECT event_nid, state_snapshot_nid FROM roomserver_events WHERE event_id = $1"

	// Select state snapshots from event IDs
	bulkSelectSnapshotsForEventIDsSQL = "" +
		"SELECT event_id, state_snapshot_nid FROM roomserver_events WHERE event_id = ANY($1)"

	// Select state events by ID for state resolution
	bulkSelectStateEventByIDSQL = "" +
		"SELECT event_type_nid, event_state_key_nid, event_nid FROM roomserver_events WHERE event_id = ANY($1)"

	// Select state events by ID excluding rejected events
	bulkSelectStateEventByIDExcludingRejectedSQL = "" +
		"SELECT event_type_nid, event_state_key_nid, event_nid FROM roomserver_events WHERE event_id = ANY($1) AND is_rejected = FALSE"

	// Select state events by NID with type/state key filters
	bulkSelectStateEventByNIDSQL = "" +
		"SELECT event_type_nid, event_state_key_nid, event_nid FROM roomserver_events WHERE event_nid = ANY($1)"

	// Select state at events by ID
	bulkSelectStateAtEventByIDSQL = "" +
		"SELECT event_id, state_snapshot_nid, event_nid FROM roomserver_events WHERE event_id = ANY($1)"

	// Update state snapshot for an event
	updateEventStateSQL = "" +
		"UPDATE roomserver_events SET state_snapshot_nid = $2 WHERE event_nid = $1"

	// Check if an event has been sent to the output log
	selectEventSentToOutputSQL = "" +
		"SELECT sent_to_output FROM roomserver_events WHERE event_nid = $1"

	// Mark an event as sent to the output log
	updateEventSentToOutputSQL = "" +
		"UPDATE roomserver_events SET sent_to_output = TRUE WHERE event_nid = $1"

	// Get event ID from event NID
	selectEventIDSQL = "" +
		"SELECT event_id FROM roomserver_events WHERE event_nid = $1"

	// Get state and reference data for events
	bulkSelectStateAtEventAndReferenceSQL = "" +
		"SELECT event_id, state_snapshot_nid, room_nid, event_type_nid, event_state_key_nid FROM roomserver_events WHERE event_nid = ANY($1)"

	// Get event IDs from NIDs
	bulkSelectEventIDSQL = "" +
		"SELECT event_nid, event_id FROM roomserver_events WHERE event_nid = ANY($1)"

	// Get event NIDs from IDs
	bulkSelectEventNIDSQL = "" +
		"SELECT event_id, event_nid, room_nid, state_snapshot_nid FROM roomserver_events WHERE event_id = ANY($1)"

	// Get unsent event NIDs from IDs
	bulkSelectUnsentEventNIDSQL = "" +
		"SELECT event_id, event_nid, room_nid, state_snapshot_nid FROM roomserver_events WHERE event_id = ANY($1) AND sent_to_output = FALSE"

	// Get maximum depth for a set of events
	selectMaxEventDepthSQL = "" +
		"SELECT MAX(depth) FROM roomserver_events WHERE event_nid = ANY($1)"

	// Get room NIDs for event NIDs
	selectRoomNIDsForEventNIDsSQL = "" +
		"SELECT event_nid, room_nid FROM roomserver_events WHERE event_nid = ANY($1)"

	// Check if an event is rejected
	selectEventRejectedSQL = "" +
		"SELECT is_rejected FROM roomserver_events WHERE room_nid = $1 AND event_id = $2"

	// Get rooms with specific event type
	selectRoomsWithEventTypeNIDSQL = "" +
		"SELECT DISTINCT room_nid FROM roomserver_events WHERE event_type_nid = $1"
)

// eventsStatements implements tables.Events using PostgreSQL
type eventsStatements struct {
	cm *sqlutil.Connections
	
	// SQL statements stored as struct fields
	insertEventStmt                        string
	selectEventStmt                        string
	bulkSelectSnapshotsForEventIDsStmt     string
	bulkSelectStateEventByIDStmt           string
	bulkSelectStateEventByIDExcludingRejectedStmt string
	bulkSelectStateEventByNIDStmt          string
	bulkSelectStateAtEventByIDStmt         string
	updateEventStateStmt                   string
	selectEventSentToOutputStmt            string
	updateEventSentToOutputStmt            string
	selectEventIDStmt                      string
	bulkSelectStateAtEventAndReferenceStmt string
	bulkSelectEventIDStmt                  string
	bulkSelectEventNIDStmt                 string
	bulkSelectUnsentEventNIDStmt           string
	selectMaxEventDepthStmt                string
	selectRoomNIDsForEventNIDsStmt         string
	selectEventRejectedStmt                string
	selectRoomsWithEventTypeNIDStmt        string
}

// NewPostgresEventsTable creates a new postgres events table and prepares all statements
func NewPostgresEventsTable(ctx context.Context, cm *sqlutil.Connections) (tables.Events, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(eventsSchema).Error; err != nil {
		return nil, err
	}
	
	// Run migrations
	m := sqlutil.NewMigrator(db.DB())
	m.AddMigrations(sqlutil.Migration{
		Version: "roomserver: events table schema",
		Up:      deltas.UpStateBlocksRefactor,
	})
	if err := m.Up(ctx); err != nil {
		return nil, err
	}
	
	// Initialize statements
	s := &eventsStatements{
		cm: cm,
		
		// Initialize SQL statement fields with the constants
		insertEventStmt:                        insertEventSQL,
		selectEventStmt:                        selectEventSQL,
		bulkSelectSnapshotsForEventIDsStmt:     bulkSelectSnapshotsForEventIDsSQL,
		bulkSelectStateEventByIDStmt:           bulkSelectStateEventByIDSQL,
		bulkSelectStateEventByIDExcludingRejectedStmt: bulkSelectStateEventByIDExcludingRejectedSQL,
		bulkSelectStateEventByNIDStmt:          bulkSelectStateEventByNIDSQL,
		bulkSelectStateAtEventByIDStmt:         bulkSelectStateAtEventByIDSQL,
		updateEventStateStmt:                   updateEventStateSQL,
		selectEventSentToOutputStmt:            selectEventSentToOutputSQL,
		updateEventSentToOutputStmt:            updateEventSentToOutputSQL,
		selectEventIDStmt:                      selectEventIDSQL,
		bulkSelectStateAtEventAndReferenceStmt: bulkSelectStateAtEventAndReferenceSQL,
		bulkSelectEventIDStmt:                  bulkSelectEventIDSQL,
		bulkSelectEventNIDStmt:                 bulkSelectEventNIDSQL,
		bulkSelectUnsentEventNIDStmt:           bulkSelectUnsentEventNIDSQL,
		selectMaxEventDepthStmt:                selectMaxEventDepthSQL,
		selectRoomNIDsForEventNIDsStmt:         selectRoomNIDsForEventNIDsSQL,
		selectEventRejectedStmt:                selectEventRejectedSQL,
		selectRoomsWithEventTypeNIDStmt:        selectRoomsWithEventTypeNIDSQL,
	}
	
	return s, nil
}

func (s *eventsStatements) InsertEvent(
	ctx context.Context,
	roomNID types.RoomNID,
	eventTypeNID types.EventTypeNID,
	eventStateKeyNID types.EventStateKeyNID,
	eventID string,
	authEventNIDs []types.EventNID,
	depth int64,
	isRejected bool,
) (types.EventNID, types.StateSnapshotNID, error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	var eventNID int64
	var stateNID int64

	// Execute query
	err := db.Raw(
		s.insertEventStmt,
		int64(roomNID),
		int64(eventTypeNID),
		int64(eventStateKeyNID),
		eventID,
		eventNIDsAsArray(authEventNIDs),
		depth,
		isRejected,
	).Row().Scan(&eventNID, &stateNID)

	return types.EventNID(eventNID), types.StateSnapshotNID(stateNID), err
}

func (s *eventsStatements) SelectEvent(
	ctx context.Context,
	eventID string,
) (types.EventNID, types.StateSnapshotNID, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	var eventNID int64
	var stateNID int64
	
	// Execute query
	err := db.Raw(
		s.selectEventStmt,
		eventID,
	).Row().Scan(&eventNID, &stateNID)
	
	return types.EventNID(eventNID), types.StateSnapshotNID(stateNID), err
}

func (s *eventsStatements) BulkSelectSnapshotsFromEventIDs(
	ctx context.Context,
	eventIDs []string,
) (map[types.StateSnapshotNID][]string, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute query
	rows, err := db.Raw(
		s.bulkSelectSnapshotsForEventIDsStmt,
		pq.StringArray(eventIDs),
	).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectSnapshotsFromEventIDs: rows.close() failed")
	
	results := make(map[types.StateSnapshotNID][]string)
	
	// Process results
	var eventID string
	var stateNID int64
	
	for rows.Next() {
		if err = rows.Scan(&eventID, &stateNID); err != nil {
			return nil, err
		}
		
		results[types.StateSnapshotNID(stateNID)] = append(
			results[types.StateSnapshotNID(stateNID)],
			eventID,
		)
	}
	
	return results, rows.Err()
}

func (s *eventsStatements) BulkSelectStateEventByID(
	ctx context.Context,
	eventIDs []string,
) ([]types.StateEntry, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute query
	rows, err := db.Raw(
		s.bulkSelectStateEventByIDStmt,
		pq.StringArray(eventIDs),
	).Rows()

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectStateEventByID: rows.close() failed")

	var result []types.StateEntry
	var eventTypeNID int64
	var eventStateKeyNID int64
	var eventNID int64
	for rows.Next() {
		if err = rows.Scan(
			&eventTypeNID, &eventStateKeyNID, &eventNID,
		); err != nil {
			return nil, err
		}
		result = append(result, types.StateEntry{
			StateKeyTuple: types.StateKeyTuple{
				EventTypeNID:     types.EventTypeNID(eventTypeNID),
				EventStateKeyNID: types.EventStateKeyNID(eventStateKeyNID),
			},
			EventNID: types.EventNID(eventNID),
		})
	}
	return result, rows.Err()
}

func (s *eventsStatements) BulkSelectStateEventByNID(
	ctx context.Context,
	eventNIDs []types.EventNID,
	filterEventTypeNIDs []types.EventTypeNID,
	filterEventStateKeyNIDs []types.EventStateKeyNID,
) ([]types.StateEntry, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Convert event NIDs to array
	var err error
	var rows *sql.Rows

	// Convert filter event type NIDs to array
	typeNIDArray := make([]int64, len(filterEventTypeNIDs))
	for i := range filterEventTypeNIDs {
		typeNIDArray[i] = int64(filterEventTypeNIDs[i])
	}

	// Convert filter event state key NIDs to array
	stateKeyNIDArray := make([]int64, len(filterEventStateKeyNIDs))
	for i := range filterEventStateKeyNIDs {
		stateKeyNIDArray[i] = int64(filterEventStateKeyNIDs[i])
	}

	// Execute query
	rows, err = db.Raw(
		s.bulkSelectStateEventByNIDStmt,
		eventNIDsAsArray(eventNIDs),
		pq.Array(typeNIDArray),
		pq.Array(stateKeyNIDArray),
	).Rows()

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectStateEventByNID: rows.close() failed")

	var result []types.StateEntry
	var eventTypeNID int64
	var eventStateKeyNID int64
	var eventNID int64
	for rows.Next() {
		if err = rows.Scan(
			&eventTypeNID, &eventStateKeyNID, &eventNID,
		); err != nil {
			return nil, err
		}
		result = append(result, types.StateEntry{
			StateKeyTuple: types.StateKeyTuple{
				EventTypeNID:     types.EventTypeNID(eventTypeNID),
				EventStateKeyNID: types.EventStateKeyNID(eventStateKeyNID),
			},
			EventNID: types.EventNID(eventNID),
		})
	}
	return result, rows.Err()
}

func (s *eventsStatements) BulkSelectStateAtEventByID(
	ctx context.Context,
	eventIDs []string,
) ([]types.StateAtEvent, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute query
	rows, err := db.Raw(
		s.bulkSelectStateAtEventByIDStmt,
		pq.StringArray(eventIDs),
	).Rows()

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectStateAtEventByID: rows.close() failed")

	var result []types.StateAtEvent
	var eventTypeNID int64
	var eventStateKeyNID int64
	var eventNID int64
	var stateSnapshotNID int64
	var isRejected bool
	for rows.Next() {
		if err = rows.Scan(
			&eventTypeNID, &eventStateKeyNID, &eventNID, &stateSnapshotNID, &isRejected,
		); err != nil {
			return nil, err
		}
		result = append(result, types.StateAtEvent{
			BeforeStateSnapshotNID: types.StateSnapshotNID(stateSnapshotNID),
			IsRejected:             isRejected,
			StateEntry: types.StateEntry{
				StateKeyTuple: types.StateKeyTuple{
					EventTypeNID:     types.EventTypeNID(eventTypeNID),
					EventStateKeyNID: types.EventStateKeyNID(eventStateKeyNID),
				},
				EventNID: types.EventNID(eventNID),
			},
		})
	}
	return result, rows.Err()
}

func (s *eventsStatements) UpdateEventState(
	ctx context.Context,
	eventNID types.EventNID,
	stateNID types.StateSnapshotNID,
) error {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	// Execute query
	return db.Exec(
		s.updateEventStateStmt,
		int64(eventNID),
		int64(stateNID),
	).Error
}

func (s *eventsStatements) SelectEventSentToOutput(
	ctx context.Context,
	eventNID types.EventNID,
) (sentToOutput bool, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute query
	err = db.Raw(
		s.selectEventSentToOutputStmt,
		int64(eventNID),
	).Row().Scan(&sentToOutput)

	return
}

func (s *eventsStatements) UpdateEventSentToOutput(
	ctx context.Context,
	eventNID types.EventNID,
) error {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	// Execute query
	return db.Exec(
		s.updateEventSentToOutputStmt,
		int64(eventNID),
	).Error
}

func (s *eventsStatements) SelectEventID(
	ctx context.Context,
	eventNID types.EventNID,
) (eventID string, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute query
	err = db.Raw(
		s.selectEventIDStmt,
		int64(eventNID),
	).Row().Scan(&eventID)

	return
}

func (s *eventsStatements) BulkSelectStateAtEventAndReference(
	ctx context.Context,
	eventNIDs []types.EventNID,
) ([]types.StateAtEventAndReference, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute query
	rows, err := db.Raw(
		s.bulkSelectStateAtEventAndReferenceStmt,
		eventNIDsAsArray(eventNIDs),
	).Rows()

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectStateAtEventAndReference: rows.close() failed")

	var result []types.StateAtEventAndReference
	var eventTypeNID int64
	var eventStateKeyNID int64
	var eventNID int64
	var stateSnapshotNID int64
	var eventID string
	for rows.Next() {
		if err = rows.Scan(
			&eventID, &stateSnapshotNID, &eventTypeNID, &eventStateKeyNID, &eventNID,
		); err != nil {
			return nil, err
		}
		result = append(result, types.StateAtEventAndReference{
			EventID: eventID,
			StateAtEvent: types.StateAtEvent{
				BeforeStateSnapshotNID: types.StateSnapshotNID(stateSnapshotNID),
				StateEntry: types.StateEntry{
					StateKeyTuple: types.StateKeyTuple{
						EventTypeNID:     types.EventTypeNID(eventTypeNID),
						EventStateKeyNID: types.EventStateKeyNID(eventStateKeyNID),
					},
					EventNID: types.EventNID(eventNID),
				},
			},
		})
	}
	return result, rows.Err()
}

func (s *eventsStatements) BulkSelectEventID(
	ctx context.Context,
	eventNIDs []types.EventNID,
) (map[types.EventNID]string, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute query
	rows, err := db.Raw(
		s.bulkSelectEventIDStmt,
		eventNIDsAsArray(eventNIDs),
	).Rows()

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectEventID: rows.close() failed")

	result := make(map[types.EventNID]string)
	var eventNID int64
	var eventID string
	for rows.Next() {
		if err = rows.Scan(&eventNID, &eventID); err != nil {
			return nil, err
		}
		result[types.EventNID(eventNID)] = eventID
	}
	return result, rows.Err()
}

func (s *eventsStatements) BulkSelectEventNIDs(
	ctx context.Context,
	eventIDs []string,
) (map[string]types.EventMetadata, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute query
	rows, err := db.Raw(
		s.bulkSelectEventNIDStmt,
		pq.StringArray(eventIDs),
	).Rows()

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectEventNIDs: rows.close() failed")

	result := make(map[string]types.EventMetadata)
	var eventID string
	var eventNID int64
	var roomNID int64
	var stateSnapshotNID int64
	for rows.Next() {
		if err = rows.Scan(&eventID, &eventNID, &roomNID, &stateSnapshotNID); err != nil {
			return nil, err
		}
		result[eventID] = types.EventMetadata{
			EventNID: types.EventNID(eventNID),
			RoomNID:  types.RoomNID(roomNID),
		}
	}
	return result, rows.Err()
}

func (s *eventsStatements) BulkSelectUnsentEventNIDs(
	ctx context.Context,
	eventIDs []string,
) (map[string]types.EventMetadata, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute query
	rows, err := db.Raw(
		s.bulkSelectUnsentEventNIDStmt,
		pq.StringArray(eventIDs),
	).Rows()

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectUnsentEventNIDs: rows.close() failed")

	result := make(map[string]types.EventMetadata)
	var eventID string
	var eventNID int64
	var roomNID int64
	var stateSnapshotNID int64
	for rows.Next() {
		if err = rows.Scan(&eventID, &eventNID, &roomNID, &stateSnapshotNID); err != nil {
			return nil, err
		}
		result[eventID] = types.EventMetadata{
			EventNID: types.EventNID(eventNID),
			RoomNID:  types.RoomNID(roomNID),
		}
	}
	return result, rows.Err()
}

func (s *eventsStatements) SelectMaxEventDepth(
	ctx context.Context,
	eventNIDs []types.EventNID,
) (int64, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	var result int64

	// Execute query
	err := db.Raw(
		s.selectMaxEventDepthStmt,
		eventNIDsAsArray(eventNIDs),
	).Row().Scan(&result)

	return result, err
}

func (s *eventsStatements) SelectRoomNIDsForEventNIDs(
	ctx context.Context,
	eventNIDs []types.EventNID,
) (map[types.EventNID]types.RoomNID, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute query
	rows, err := db.Raw(
		s.selectRoomNIDsForEventNIDsStmt,
		eventNIDsAsArray(eventNIDs),
	).Rows()

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectRoomNIDsForEventNIDs: rows.close() failed")

	result := make(map[types.EventNID]types.RoomNID)
	var eventNID int64
	var roomNID int64
	for rows.Next() {
		if err = rows.Scan(&eventNID, &roomNID); err != nil {
			return nil, err
		}
		result[types.EventNID(eventNID)] = types.RoomNID(roomNID)
	}
	return result, rows.Err()
}

func (s *eventsStatements) IsEventRejected(
	ctx context.Context,
	roomNID types.RoomNID,
	eventID string,
) (rejected bool, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute query
	err = db.Raw(
		s.selectEventRejectedStmt,
		int64(roomNID),
		eventID,
	).Row().Scan(&rejected)

	return
}

func (s *eventsStatements) BulkSelectStateEventByTypeNID(
	ctx context.Context,
	eventTypeNID types.EventTypeNID,
) ([]types.RoomNID, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute query
	rows, err := db.Raw(
		s.selectRoomsWithEventTypeNIDStmt,
		int64(eventTypeNID),
	).Rows()

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "BulkSelectStateEventByTypeNID: rows.close() failed")

	var result []types.RoomNID
	var roomNID int64
	for rows.Next() {
		if err = rows.Scan(&roomNID); err != nil {
			return nil, err
		}
		result = append(result, types.RoomNID(roomNID))
	}
	return result, rows.Err()
}

func (s *eventsStatements) BulkSelectEventStateKeyNIDs(
	ctx context.Context,
	eventIDs []string,
	excludeRejected bool,
) (map[string]types.StateKeyTuple, error) {
	var sql string
	if excludeRejected {
		sql = s.bulkSelectStateEventByIDExcludingRejectedStmt
	} else {
		sql = s.bulkSelectStateEventByIDStmt
	}

	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Execute query
	rows, err := db.Raw(
		sql,
		pq.StringArray(eventIDs),
	).Rows()

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectEventStateKeyNIDs: rows.close() failed")

	eventTypeNIDMap := make(map[string]types.StateKeyTuple)
	i := 0
	var eventTypeNID int64
	var eventStateKeyNID int64
	var eventNID int64

	for rows.Next() {
		if err = rows.Scan(
			&eventTypeNID, &eventStateKeyNID, &eventNID,
		); err != nil {
			return nil, err
		}
		// We need to be careful here as we don't know the order that the
		// rows will be returned in, nor how many times a given event ID may
		// be returned, yet we need to ensure that the event ID at position
		// 'i' of the eventIDs parameter matches the i-th tuple in our response.
		// So we'll populate a map using event NIDs.
		if i < len(eventIDs) {
			eventTypeNIDMap[eventIDs[i]] = types.StateKeyTuple{
				EventTypeNID:     types.EventTypeNID(eventTypeNID),
				EventStateKeyNID: types.EventStateKeyNID(eventStateKeyNID),
			}
		}

		i++
	}

	return eventTypeNIDMap, nil
}

// Helper function to convert event NIDs to an int64 array for PostgreSQL
func eventNIDsAsArray(eventNIDs []types.EventNID) []int64 {
	nids := make([]int64, len(eventNIDs))
	for i := range eventNIDs {
		nids[i] = int64(eventNIDs[i])
	}
	return nids
}
