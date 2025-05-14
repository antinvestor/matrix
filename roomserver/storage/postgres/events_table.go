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
	"fmt"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
	"sort"
)

// The events table holds metadata for each event, the actual JSON is stored
// separately to keep the size of the rows small.
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

const eventsSchemaRevert = `
DROP TABLE IF EXISTS roomserver_events;
DROP SEQUENCE IF EXISTS roomserver_event_nid_seq;
`

// SQL Constants for event table queries
const insertEventSQL = "" +
	"INSERT INTO roomserver_events AS e (room_nid, event_type_nid, event_state_key_nid, event_id, auth_event_nids, depth, is_rejected)" +
	" VALUES ($1, $2, $3, $4, $5, $6, $7)" +
	" ON CONFLICT ON CONSTRAINT roomserver_event_id_unique DO UPDATE" +
	" SET is_rejected = $7 WHERE e.event_id = $4 AND e.is_rejected = TRUE" +
	" RETURNING event_nid, state_snapshot_nid"

const selectEventSQL = "" +
	"SELECT event_nid, state_snapshot_nid FROM roomserver_events WHERE event_id = $1"

const bulkSelectSnapshotsFromEventIDsSQL = "" +
	"SELECT event_id, state_snapshot_nid FROM roomserver_events WHERE event_id = ANY($1)"

// Bulk lookup of events by string ID.
// Sort by the numeric IDs for event type and state key.
// This means we can use binary search to lookup entries by type and state key.
const bulkSelectStateEventByIDSQL = "" +
	"SELECT event_type_nid, event_state_key_nid, event_nid FROM roomserver_events" +
	" WHERE event_id = ANY($1)" +
	" ORDER BY event_type_nid, event_state_key_nid ASC"

// Bulk lookup of events by string ID that aren't excluded.
// Sort by the numeric IDs for event type and state key.
// This means we can use binary search to lookup entries by type and state key.
const bulkSelectStateEventByIDRejectedSQL = "" +
	"SELECT event_type_nid, event_state_key_nid, event_nid FROM roomserver_events" +
	" WHERE event_id = ANY($1) AND is_rejected = FALSE" +
	" ORDER BY event_type_nid, event_state_key_nid ASC"

// Bulk look up of events by event NID, optionally filtering by the event type
// or event state key NIDs if provided. (The CARDINALITY check will return true
// if the provided arrays are empty, ergo no filtering).
const bulkSelectStateEventByNIDSQL = "" +
	"SELECT event_type_nid, event_state_key_nid, event_nid FROM roomserver_events" +
	" WHERE event_nid = ANY($1)" +
	" AND (CARDINALITY($2::bigint[]) = 0 OR event_type_nid = ANY($2))" +
	" AND (CARDINALITY($3::bigint[]) = 0 OR event_state_key_nid = ANY($3))" +
	" ORDER BY event_type_nid, event_state_key_nid ASC"

const bulkSelectStateAtEventByIDSQL = "" +
	"SELECT event_type_nid, event_state_key_nid, event_nid, state_snapshot_nid, is_rejected FROM roomserver_events" +
	" WHERE event_id = ANY($1)"

const updateEventStateSQL = "" +
	"UPDATE roomserver_events SET state_snapshot_nid = $2 WHERE event_nid = $1"

const selectEventSentToOutputSQL = "" +
	"SELECT sent_to_output FROM roomserver_events WHERE event_nid = $1"

const updateEventSentToOutputSQL = "" +
	"UPDATE roomserver_events SET sent_to_output = TRUE WHERE event_nid = $1"

const selectEventIDSQL = "" +
	"SELECT event_id FROM roomserver_events WHERE event_nid = $1"

const bulkSelectStateAtEventAndReferenceSQL = "" +
	"SELECT event_type_nid, event_state_key_nid, event_nid, state_snapshot_nid, event_id" +
	" FROM roomserver_events WHERE event_nid = ANY($1)"

const bulkSelectEventIDSQL = "" +
	"SELECT event_nid, event_id FROM roomserver_events WHERE event_nid = ANY($1)"

const bulkSelectEventNIDSQL = "" +
	"SELECT event_id, event_nid, room_nid FROM roomserver_events WHERE event_id = ANY($1)"

const bulkSelectUnsentEventNIDSQL = "" +
	"SELECT event_id, event_nid, room_nid FROM roomserver_events WHERE event_id = ANY($1) AND sent_to_output = FALSE"

const selectMaxEventDepthSQL = "" +
	"SELECT COALESCE(MAX(depth) + 1, 0) FROM roomserver_events WHERE event_nid = ANY($1)"

const selectRoomNIDsForEventNIDsSQL = "" +
	"SELECT event_nid, room_nid FROM roomserver_events WHERE event_nid = ANY($1)"

const selectEventRejectedSQL = "" +
	"SELECT is_rejected FROM roomserver_events WHERE room_nid = $1 AND event_id = $2"

const selectRoomsWithEventTypeNIDSQL = `SELECT DISTINCT room_nid FROM roomserver_events WHERE event_type_nid = $1`

// eventStatements contains the SQL statements for interacting with the events table.
type eventStatements struct {
	cm sqlutil.ConnectionManager

	insertEventSQL                                   string
	selectEventSQL                                   string
	bulkSelectSnapshotsFromEventIDsSQL               string
	bulkSelectStateEventByIDSQL                      string
	bulkSelectStateEventByIDRejectedSQL              string
	bulkSelectStateEventByNIDSQL                     string
	BulkSelectStateEventByNIDAndNoneEmptyStateKeySQL string
	BulkSelectStateEventByStateKeySQL                string
	bulkSelectStateAtEventByIDSQL                    string
	updateEventStateSQL                              string
	selectEventSentToOutputSQL                       string
	updateEventSentToOutputSQL                       string
	selectEventIDSQL                                 string
	bulkSelectStateAtEventAndReferenceSQL            string
	bulkSelectEventIDSQL                             string
	bulkSelectEventNIDSQL                            string
	bulkSelectUnsentEventNIDSQL                      string
	selectMaxEventDepthSQL                           string
	selectRoomNIDsForEventNIDsSQL                    string
	selectEventRejectedSQL                           string
	selectRoomsWithEventTypeNIDSQL                   string
}

// NewPostgresEventsTable creates a new events table. If the table already exists,
// it will ensure it has the correct schema.
func NewPostgresEventsTable(_ context.Context, cm sqlutil.ConnectionManager) (tables.Events, error) {
	s := &eventStatements{
		cm:                                    cm,
		insertEventSQL:                        insertEventSQL,
		selectEventSQL:                        selectEventSQL,
		bulkSelectSnapshotsFromEventIDsSQL:    bulkSelectSnapshotsFromEventIDsSQL,
		bulkSelectStateEventByIDSQL:           bulkSelectStateEventByIDSQL,
		bulkSelectStateEventByIDRejectedSQL:   bulkSelectStateEventByIDRejectedSQL,
		bulkSelectStateEventByNIDSQL:          bulkSelectStateEventByNIDSQL,
		bulkSelectStateAtEventByIDSQL:         bulkSelectStateAtEventByIDSQL,
		updateEventStateSQL:                   updateEventStateSQL,
		selectEventSentToOutputSQL:            selectEventSentToOutputSQL,
		updateEventSentToOutputSQL:            updateEventSentToOutputSQL,
		selectEventIDSQL:                      selectEventIDSQL,
		bulkSelectStateAtEventAndReferenceSQL: bulkSelectStateAtEventAndReferenceSQL,
		bulkSelectEventIDSQL:                  bulkSelectEventIDSQL,
		bulkSelectEventNIDSQL:                 bulkSelectEventNIDSQL,
		bulkSelectUnsentEventNIDSQL:           bulkSelectUnsentEventNIDSQL,
		selectMaxEventDepthSQL:                selectMaxEventDepthSQL,
		selectRoomNIDsForEventNIDsSQL:         selectRoomNIDsForEventNIDsSQL,
		selectEventRejectedSQL:                selectEventRejectedSQL,
		selectRoomsWithEventTypeNIDSQL:        selectRoomsWithEventTypeNIDSQL,
	}

	// Apply the events table schema
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "roomserver_events_schema_001",
		Patch:       eventsSchema,
		RevertPatch: eventsSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertEvent adds a new event to the events table.
// If the event already exists in the table, it will only update the is_rejected status if the event was previously rejected.
func (s *eventStatements) InsertEvent(
	ctx context.Context,
	roomNID types.RoomNID,
	eventTypeNID types.EventTypeNID,
	eventStateKeyNID types.EventStateKeyNID,
	eventID string,
	authEventNIDs []types.EventNID,
	depth int64,
	isRejected bool,
) (types.EventNID, types.StateSnapshotNID, error) {
	var eventNID int64
	var stateNID int64

	// Get a connection
	db := s.cm.Connection(ctx, false)

	// Insert the event
	row := db.Raw(s.insertEventSQL, int64(roomNID), int64(eventTypeNID), int64(eventStateKeyNID),
		eventID, eventNIDsAsArray(authEventNIDs), depth,
		isRejected).Row()
	err := row.Scan(&eventNID, &stateNID)

	return types.EventNID(eventNID), types.StateSnapshotNID(stateNID), err
}

// SelectEvent retrieves an event from the database by its event ID.
func (s *eventStatements) SelectEvent(
	ctx context.Context, eventID string,
) (types.EventNID, types.StateSnapshotNID, error) {
	var eventNID int64
	var stateNID int64

	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Query the database
	row := db.Raw(s.selectEventSQL, eventID).Row()
	err := row.Scan(&eventNID, &stateNID)
	return types.EventNID(eventNID), types.StateSnapshotNID(stateNID), err
}

// BulkSelectSnapshotsFromEventIDs retrieves the state snapshots for multiple events by their IDs.
func (s *eventStatements) BulkSelectSnapshotsFromEventIDs(
	ctx context.Context, eventIDs []string,
) (map[types.StateSnapshotNID][]string, error) {
	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query to get snapshots for these event IDs
	rows, err := db.Raw(s.bulkSelectSnapshotsFromEventIDsSQL, pq.Array(eventIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "BulkSelectSnapshotsFromEventIDs: rows.close() failed")

	var eventID string
	var stateNID types.StateSnapshotNID
	result := make(map[types.StateSnapshotNID][]string)
	for rows.Next() {
		if err := rows.Scan(&stateNID, &eventID); err != nil {
			return nil, err
		}
		result[stateNID] = append(result[stateNID], eventID)
	}

	return result, rows.Err()
}

// BulkSelectStateEventByID lookups a list of state events by event ID.
// If not excluding rejected events, and any of the requested events are missing from
// the database it returns a types.MissingEventError. If excluding rejected events,
// the events will be silently omitted without error.
func (s *eventStatements) BulkSelectStateEventByID(
	ctx context.Context, eventIDs []string, excludeRejected bool,
) ([]types.StateEntry, error) {
	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Choose the appropriate query based on whether to exclude rejected events
	var querySQL string
	if excludeRejected {
		querySQL = s.bulkSelectStateEventByIDRejectedSQL
	} else {
		querySQL = s.bulkSelectStateEventByIDSQL
	}

	// Execute the query
	rows, err := db.Raw(querySQL, pq.StringArray(eventIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectStateEventByID: rows.close() failed")
	// We know that we will only get as many results as event IDs
	// because of the unique constraint on event IDs.
	// So we can allocate an array of the correct size now.
	// We might get fewer results than IDs so we adjust the length of the slice before returning it.
	results := make([]types.StateEntry, 0, len(eventIDs))
	i := 0
	for ; rows.Next(); i++ {
		var result types.StateEntry
		if err = rows.Scan(
			&result.EventTypeNID,
			&result.EventStateKeyNID,
			&result.EventNID,
		); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if !excludeRejected && i != len(eventIDs) {
		// If there are fewer rows returned than IDs then we were asked to lookup event IDs we don't have.
		// We don't know which ones were missing because we don't return the string IDs in the query.
		// However it should be possible debug this by replaying queries or entries from the input kafka logs.
		// If this turns out to be impossible and we do need the debug information here, it would be better
		// to do it as a separate query rather than slowing down/complicating the internal case.
		return nil, types.MissingEventError(
			fmt.Sprintf("storage: state event IDs missing from the database (%d != %d)", i, len(eventIDs)),
		)
	}
	return results, nil
}

// BulkSelectStateEventByNID lookups a list of state events by event NID.
// If any of the requested events are missing from the database it returns a types.MissingEventError
func (s *eventStatements) BulkSelectStateEventByNID(
	ctx context.Context, eventNIDs []types.EventNID,
	stateKeyTuples []types.StateKeyTuple,
) ([]types.StateEntry, error) {
	tuples := types.StateKeyTupleSorter(stateKeyTuples)
	sort.Sort(tuples)
	eventTypeNIDArray, eventStateKeyNIDArray := tuples.TypesAndStateKeysAsArrays()

	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	rows, err := db.Raw(s.bulkSelectStateEventByNIDSQL, eventNIDsAsArray(eventNIDs), pq.Int64Array(eventTypeNIDArray), pq.Int64Array(eventStateKeyNIDArray)).Rows()

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "BulkSelectStateEventByID: rows.close() failed")
	// We know that we will only get as many results as event IDs
	// because of the unique constraint on event IDs.
	// So we can allocate an array of the correct size now.
	// We might get fewer results than IDs so we adjust the length of the slice before returning it.
	results := make([]types.StateEntry, len(eventNIDs))
	i := 0
	for ; rows.Next(); i++ {
		result := &results[i]
		if err = rows.Scan(
			&result.EventTypeNID,
			&result.EventStateKeyNID,
			&result.EventNID,
		); err != nil {
			return nil, err
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return results[:i], nil
}

// BulkSelectStateAtEventByID lookups the state at a list of events by event ID.
// If any of the requested events are missing from the database it returns a types.MissingEventError.
// If we do not have the state for any of the requested events it returns a types.MissingStateError.
func (s *eventStatements) BulkSelectStateAtEventByID(
	ctx context.Context, eventIDs []string,
) ([]types.StateAtEvent, error) {
	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	rows, err := db.Raw(s.bulkSelectStateAtEventByIDSQL, pq.StringArray(eventIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectStateAtEventByID: rows.close() failed")
	results := make([]types.StateAtEvent, len(eventIDs))
	i := 0
	for ; rows.Next(); i++ {
		result := &results[i]
		if err = rows.Scan(
			&result.EventTypeNID,
			&result.EventStateKeyNID,
			&result.EventNID,
			&result.BeforeStateSnapshotNID,
			&result.IsRejected,
		); err != nil {
			return nil, err
		}
		// Genuine create events are the only case where it's OK to have no previous state.
		isCreate := result.EventTypeNID == types.MRoomCreateNID && result.EventStateKeyNID == 1
		if result.BeforeStateSnapshotNID == 0 && !isCreate {
			return nil, types.MissingStateError(
				fmt.Sprintf("storage: missing state for event NID %d", result.EventNID),
			)
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if i != len(eventIDs) {
		return nil, types.MissingEventError(
			fmt.Sprintf("storage: event IDs missing from the database (%d != %d)", i, len(eventIDs)),
		)
	}
	return results, nil
}

// UpdateEventState updates the state snapshot NID for an event.
func (s *eventStatements) UpdateEventState(
	ctx context.Context, eventNID types.EventNID, stateNID types.StateSnapshotNID,
) error {
	// Get a connection
	db := s.cm.Connection(ctx, false)

	// Execute the update
	err := db.Exec(s.updateEventStateSQL, int64(eventNID), int64(stateNID)).Error
	return err
}

// SelectEventSentToOutput checks if an event has been sent to the output log.
func (s *eventStatements) SelectEventSentToOutput(
	ctx context.Context, eventNID types.EventNID,
) (sentToOutput bool, err error) {
	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	err = db.Raw(s.selectEventSentToOutputSQL, int64(eventNID)).Row().Scan(&sentToOutput)
	return
}

// UpdateEventSentToOutput marks an event as having been sent to the output log.
func (s *eventStatements) UpdateEventSentToOutput(ctx context.Context, eventNID types.EventNID) error {
	// Get a connection
	db := s.cm.Connection(ctx, false)

	// Execute the update
	err := db.Exec(s.updateEventSentToOutputSQL, int64(eventNID)).Error
	return err
}

// SelectEventID gets the event ID for an event NID.
func (s *eventStatements) SelectEventID(
	ctx context.Context, eventNID types.EventNID,
) (eventID string, err error) {
	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	err = db.Raw(s.selectEventIDSQL, int64(eventNID)).Row().Scan(&eventID)
	return
}

// BulkSelectStateAtEventAndReference gets a list of state entries and event references for a list of event NIDs.
func (s *eventStatements) BulkSelectStateAtEventAndReference(
	ctx context.Context, eventNIDs []types.EventNID,
) ([]types.StateAtEventAndReference, error) {
	// Convert event NIDs to int64 array
	nids := make([]int64, len(eventNIDs))
	for i := range eventNIDs {
		nids[i] = int64(eventNIDs[i])
	}

	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	rows, err := db.Raw(s.bulkSelectStateAtEventAndReferenceSQL, pq.Int64Array(nids)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectStateAtEventAndReference: rows.close() failed")

	results := make([]types.StateAtEventAndReference, len(eventNIDs))
	i := 0
	var (
		eventTypeNID     int64
		eventStateKeyNID int64
		eventNID         int64
		stateSnapshotNID int64
		eventID          string
	)
	for ; rows.Next(); i++ {
		if err := rows.Scan(
			&eventTypeNID, &eventStateKeyNID, &eventNID, &stateSnapshotNID, &eventID,
		); err != nil {
			return nil, err
		}
		result := &results[i]
		result.EventTypeNID = types.EventTypeNID(eventTypeNID)
		result.EventStateKeyNID = types.EventStateKeyNID(eventStateKeyNID)
		result.EventNID = types.EventNID(eventNID)
		result.BeforeStateSnapshotNID = types.StateSnapshotNID(stateSnapshotNID)
		result.EventID = eventID
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	// Check if we got all the requested NIDs
	if i != len(eventNIDs) {
		return nil, fmt.Errorf("storage: event NIDs missing from the database (%d != %d)", i, len(eventNIDs))
	}
	return results, nil
}

// BulkSelectEventID returns a map from numeric event ID to string event ID.
func (s *eventStatements) BulkSelectEventID(ctx context.Context, eventNIDs []types.EventNID) (map[types.EventNID]string, error) {
	// Convert event NIDs to int64 array
	nids := make([]int64, len(eventNIDs))
	for i := range eventNIDs {
		nids[i] = int64(eventNIDs[i])
	}

	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	rows, err := db.Raw(s.bulkSelectEventIDSQL, pq.Int64Array(nids)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectEventID: rows.close() failed")

	results := make(map[types.EventNID]string, len(eventNIDs))
	i := 0
	var eventNID int64
	var eventID string
	for ; rows.Next(); i++ {
		if err = rows.Scan(&eventNID, &eventID); err != nil {
			return nil, err
		}
		results[types.EventNID(eventNID)] = eventID
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	// Check if we got all the requested NIDs
	if i != len(eventNIDs) {
		return nil, fmt.Errorf("storage: event NIDs missing from the database (%d != %d)", i, len(eventNIDs))
	}
	return results, nil
}

// BulkSelectEventNID returns a map from string event ID to numeric event ID.
// If an event ID is not in the database then it is omitted from the map.
func (s *eventStatements) BulkSelectEventNID(ctx context.Context, eventIDs []string) (map[string]types.EventMetadata, error) {
	return s.bulkSelectEventNID(ctx, eventIDs, false)
}

// BulkSelectUnsentEventNID returns a map from string event ID to numeric event ID
// only for events that haven't already been sent to the roomserver output.
// If an event ID is not in the database then it is omitted from the map.
func (s *eventStatements) BulkSelectUnsentEventNID(ctx context.Context, eventIDs []string) (map[string]types.EventMetadata, error) {
	return s.bulkSelectEventNID(ctx, eventIDs, true)
}

// bulkSelectEventNID returns a map from string event ID to numeric event ID.
// If an event ID is not in the database then it is omitted from the map.
func (s *eventStatements) bulkSelectEventNID(ctx context.Context, eventIDs []string, onlyUnsent bool) (map[string]types.EventMetadata, error) {
	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Choose the appropriate query based on whether to get only unsent events
	var querySQL string
	if onlyUnsent {
		querySQL = s.bulkSelectUnsentEventNIDSQL
	} else {
		querySQL = s.bulkSelectEventNIDSQL
	}

	// Execute the query
	rows, err := db.Raw(querySQL, pq.StringArray(eventIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectEventNID: rows.close() failed")

	// Process results
	results := make(map[string]types.EventMetadata, len(eventIDs))
	var eventID string
	var eventNID int64
	var roomNID int64
	for rows.Next() {
		if err = rows.Scan(&eventID, &eventNID, &roomNID); err != nil {
			return nil, err
		}
		results[eventID] = types.EventMetadata{
			EventNID: types.EventNID(eventNID),
			RoomNID:  types.RoomNID(roomNID),
		}
	}
	return results, rows.Err()
}

// SelectMaxEventDepth returns the maximum depth of a set of events.
func (s *eventStatements) SelectMaxEventDepth(ctx context.Context, eventNIDs []types.EventNID) (int64, error) {
	// Convert event NIDs to int64 array
	nids := make([]int64, len(eventNIDs))
	for i := range eventNIDs {
		nids[i] = int64(eventNIDs[i])
	}

	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	var result int64
	err := db.Raw(s.selectMaxEventDepthSQL, pq.Int64Array(nids)).Row().Scan(&result)
	if err != nil {
		return 0, err
	}
	return result, nil
}

// SelectRoomNIDsForEventNIDs returns a map from event NID to room NID.
func (s *eventStatements) SelectRoomNIDsForEventNIDs(
	ctx context.Context, eventNIDs []types.EventNID,
) (map[types.EventNID]types.RoomNID, error) {
	// Convert event NIDs to int64 array
	nids := make([]int64, len(eventNIDs))
	for i := range eventNIDs {
		nids[i] = int64(eventNIDs[i])
	}

	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	rows, err := db.Raw(s.selectRoomNIDsForEventNIDsSQL, pq.Int64Array(nids)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectRoomNIDsForEventNIDs: rows.close() failed")

	// Process results
	result := make(map[types.EventNID]types.RoomNID)
	var eventNID types.EventNID
	var roomNID types.RoomNID
	for rows.Next() {
		if err = rows.Scan(&eventNID, &roomNID); err != nil {
			return nil, err
		}
		result[eventNID] = roomNID
	}
	return result, rows.Err()
}

func eventNIDsAsArray(eventNIDs []types.EventNID) pq.Int64Array {
	nids := make([]int64, len(eventNIDs))
	for i := range eventNIDs {
		nids[i] = int64(eventNIDs[i])
	}
	return nids
}

// SelectEventRejected checks if an event has been rejected.
func (s *eventStatements) SelectEventRejected(
	ctx context.Context, roomNID types.RoomNID, eventID string,
) (rejected bool, err error) {
	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	err = db.Raw(s.selectEventRejectedSQL, roomNID, eventID).Row().Scan(&rejected)
	return
}

// SelectRoomsWithEventTypeNID returns a list of room NIDs that contain events of a specific type.
func (s *eventStatements) SelectRoomsWithEventTypeNID(
	ctx context.Context, eventTypeNID types.EventTypeNID,
) ([]types.RoomNID, error) {
	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	rows, err := db.Raw(s.selectRoomsWithEventTypeNIDSQL, eventTypeNID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectRoomsWithEventTypeNID: rows.close() failed")

	// Process results
	var roomNIDs []types.RoomNID
	var roomNID types.RoomNID
	for rows.Next() {
		if err := rows.Scan(&roomNID); err != nil {
			return nil, err
		}
		roomNIDs = append(roomNIDs, roomNID)
	}

	return roomNIDs, rows.Err()
}

// Helper function to convert state key tuples to arrays of event type NIDs and state key NIDs
func stateKeyTuplesToArrays(stateKeyTuples []types.StateKeyTuple) (typeNIDs, stateKeyNIDs []int64) {
	if len(stateKeyTuples) > 0 {
		typeNIDs = make([]int64, len(stateKeyTuples))
		stateKeyNIDs = make([]int64, len(stateKeyTuples))
		for i := range stateKeyTuples {
			typeNIDs[i] = int64(stateKeyTuples[i].EventTypeNID)
			stateKeyNIDs[i] = int64(stateKeyTuples[i].EventStateKeyNID)
		}
	}
	return
}
