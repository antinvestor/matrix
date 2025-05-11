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
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/api"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
)

// SQL schema definition for the output room events table
const outputRoomEventsSchema = `
-- This sequence is shared between all the tables generated from kafka logs.
CREATE SEQUENCE IF NOT EXISTS syncapi_stream_id;

-- Stores output room events received from the roomserver.
CREATE TABLE IF NOT EXISTS syncapi_output_room_events (
  -- An incrementing ID which denotes the position in the log that this event resides at.
  -- NB: 'serial' makes no guarantees to increment by 1 every time, only that it increments.
  --     This isn't a problem for us since we just want to order by this field.
  id BIGINT PRIMARY KEY DEFAULT nextval('syncapi_stream_id'),
  -- The event ID for the event
  event_id TEXT NOT NULL CONSTRAINT syncapi_output_room_event_id_idx UNIQUE,
  -- The 'room_id' key for the event.
  room_id TEXT NOT NULL,
  -- The headered JSON for the event, containing potentially additional metadata such as
  -- the room version. Stored as TEXT because this should be valid UTF-8.
  headered_event_json JSONB NOT NULL,
  -- The event type e.g 'm.room.member'.
  type TEXT NOT NULL,
  -- The 'sender' property of the event.
  sender TEXT NOT NULL,
  -- true if the event content contains a url key.
  contains_url BOOL NOT NULL,
  -- A list of event IDs which represent a delta of added/removed room state. This can be NULL
  -- if there is no delta.
  add_state_ids TEXT[],
  remove_state_ids TEXT[],
  -- The client session that sent the event, if any
  session_id BIGINT,
  -- The transaction id used to send the event, if any
  transaction_id TEXT,
  -- Should the event be excluded from responses to /sync requests. Useful for
  -- events retrieved through backfilling that have a position in the stream
  -- that relates to the moment these were retrieved rather than the moment these
  -- were emitted.
  exclude_from_sync BOOL DEFAULT FALSE,
  -- Excludes edited messages from the search index.  
  exclude_from_search BOOL DEFAULT FALSE,
  -- The history visibility before this event (1 - world_readable; 2 - shared; 3 - invited; 4 - joined)
  history_visibility SMALLINT NOT NULL DEFAULT 2
);

CREATE INDEX IF NOT EXISTS syncapi_output_room_events_type_idx ON syncapi_output_room_events (type);
CREATE INDEX IF NOT EXISTS syncapi_output_room_events_sender_idx ON syncapi_output_room_events (sender);
CREATE INDEX IF NOT EXISTS syncapi_output_room_events_room_id_idx ON syncapi_output_room_events (room_id);
CREATE INDEX IF NOT EXISTS syncapi_output_room_events_exclude_from_sync_idx ON syncapi_output_room_events (exclude_from_sync);
CREATE INDEX IF NOT EXISTS syncapi_output_room_events_add_state_ids_idx ON syncapi_output_room_events ((add_state_ids IS NOT NULL));
CREATE INDEX IF NOT EXISTS syncapi_output_room_events_remove_state_ids_idx ON syncapi_output_room_events ((remove_state_ids IS NOT NULL));
CREATE INDEX IF NOT EXISTS syncapi_output_room_events_recent_events_idx ON syncapi_output_room_events (room_id, exclude_from_sync, id, sender, type);

CREATE INDEX IF NOT EXISTS syncapi_output_room_events_fts_idx ON syncapi_output_room_events
USING bm25 (event_id, id, room_id, headered_event_json, type, exclude_from_search, sender, contains_url)
WITH (
    	key_field='event_id',
    	json_fields = '{ "headered_event_json": {"fast": true, "normalizer": "raw"}}'
    );
`

const outputRoomEventsSchemaRevert = `
DROP TABLE IF EXISTS syncapi_output_room_events;
DROP SEQUENCE IF EXISTS syncapi_stream_id;
`

// SQL query to insert events
const insertEventSQL = "" +
	"INSERT INTO syncapi_output_room_events (" +
	"room_id, event_id, headered_event_json, type, sender, contains_url, add_state_ids, remove_state_ids, session_id, transaction_id, exclude_from_sync, history_visibility" +
	") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) " +
	"ON CONFLICT ON CONSTRAINT syncapi_output_room_event_id_idx DO UPDATE SET exclude_from_sync = (excluded.exclude_from_sync AND $11) " +
	"RETURNING id"

// SQL query to select events
const selectEventsSQL = "" +
	"SELECT id, event_id, headered_event_json, session_id, exclude_from_sync, transaction_id, history_visibility FROM syncapi_output_room_events WHERE event_id = ANY($1)"

// SQL query to select events with filter
const selectEventsWithFilterSQL = "" +
	"SELECT id, event_id, headered_event_json, session_id, exclude_from_sync, transaction_id, history_visibility FROM syncapi_output_room_events WHERE event_id = ANY($1)" +
	" AND ( $2::text[] IS NULL OR     sender  = ANY($2)  )" +
	" AND ( $3::text[] IS NULL OR NOT(sender  = ANY($3)) )" +
	" AND ( $4::text[] IS NULL OR     type LIKE ANY($4)  )" +
	" AND ( $5::text[] IS NULL OR NOT(type LIKE ANY($5)) )" +
	" AND ( $6::bool   IS NULL OR     contains_url = $6 )" +
	" LIMIT $7"

// SQL query to select recent events
const selectRecentEventsSQL = "" +
	"SELECT event_id, id, headered_event_json, session_id, exclude_from_sync, transaction_id, history_visibility FROM syncapi_output_room_events" +
	" WHERE room_id = $1 AND id > $2 AND id <= $3" +
	" AND ( $4::text[] IS NULL OR     sender  = ANY($4)  )" +
	" AND ( $5::text[] IS NULL OR NOT(sender  = ANY($5)) )" +
	" AND ( $6::text[] IS NULL OR     type LIKE ANY($6)  )" +
	" AND ( $7::text[] IS NULL OR NOT(type LIKE ANY($7)) )" +
	" ORDER BY id DESC LIMIT $8"

// SQL query to select recent events for sync with optimization for multiple rooms
const selectRecentEventsForSyncSQL = `
WITH room_ids AS (
     SELECT unnest($1::text[]) AS room_id
)
SELECT    x.*
FROM room_ids,
          LATERAL  (
              SELECT room_id, event_id, id, headered_event_json, session_id, exclude_from_sync, transaction_id, history_visibility
                    FROM syncapi_output_room_events recent_events
                    WHERE
                      recent_events.room_id = room_ids.room_id
                      AND recent_events.exclude_from_sync = FALSE
                      AND id > $2 AND id <= $3
                      AND ( $4::text[] IS NULL OR     sender  = ANY($4)  )
                      AND ( $5::text[] IS NULL OR NOT(sender  = ANY($5)) )
                      AND ( $6::text[] IS NULL OR     type LIKE ANY($6)  )
                      AND ( $7::text[] IS NULL OR NOT(type LIKE ANY($7)) )
                    ORDER BY recent_events.id DESC
                    LIMIT $8
              ) AS x
`

// SQL query to select max event ID
const selectMaxEventIDSQL = "" +
	"SELECT MAX(id) FROM syncapi_output_room_events"

// SQL query to update event JSON
const updateEventJSONSQL = "" +
	"UPDATE syncapi_output_room_events SET headered_event_json=$1 WHERE event_id=$2"

// SQL query to select state in range with filter
const selectStateInRangeFilteredSQL = "" +
	"SELECT event_id, id, headered_event_json, exclude_from_sync, add_state_ids, remove_state_ids, history_visibility" +
	" FROM syncapi_output_room_events" +
	" WHERE (id > $1 AND id <= $2) AND (add_state_ids IS NOT NULL OR remove_state_ids IS NOT NULL)" +
	" AND room_id = ANY($3)" +
	" AND ( $4::text[] IS NULL OR     sender  = ANY($4)  )" +
	" AND ( $5::text[] IS NULL OR NOT(sender  = ANY($5)) )" +
	" AND ( $6::text[] IS NULL OR     type LIKE ANY($6)  )" +
	" AND ( $7::text[] IS NULL OR NOT(type LIKE ANY($7)) )" +
	" AND ( $8::bool IS NULL   OR     contains_url = $8  )" +
	" ORDER BY id ASC"

// SQL query to select state in range
const selectStateInRangeSQL = "" +
	"SELECT event_id, id, headered_event_json, exclude_from_sync, add_state_ids, remove_state_ids, history_visibility" +
	" FROM syncapi_output_room_events" +
	" WHERE (id > $1 AND id <= $2) AND (add_state_ids IS NOT NULL OR remove_state_ids IS NOT NULL)" +
	" AND room_id = ANY($3)" +
	" ORDER BY id ASC"

// SQL query to delete events for room
const deleteEventsForRoomSQL = "" +
	"DELETE FROM syncapi_output_room_events WHERE room_id = $1"

// SQL query to select context event
const selectContextEventSQL = "" +
	"SELECT id, headered_event_json, history_visibility FROM syncapi_output_room_events WHERE room_id = $1 AND event_id = $2"

// SQL query to select context before event
const selectContextBeforeEventSQL = "" +
	"SELECT headered_event_json, history_visibility FROM syncapi_output_room_events WHERE room_id = $1 AND id < $2" +
	" AND ( $4::text[] IS NULL OR     sender  = ANY($4)  )" +
	" AND ( $5::text[] IS NULL OR NOT(sender  = ANY($5)) )" +
	" AND ( $6::text[] IS NULL OR     type LIKE ANY($6)  )" +
	" AND ( $7::text[] IS NULL OR NOT(type LIKE ANY($7)) )" +
	" ORDER BY id DESC LIMIT $3"

// SQL query to select context after event
const selectContextAfterEventSQL = "" +
	"SELECT id, headered_event_json, history_visibility FROM syncapi_output_room_events WHERE room_id = $1 AND id > $2" +
	" AND ( $4::text[] IS NULL OR     sender  = ANY($4)  )" +
	" AND ( $5::text[] IS NULL OR NOT(sender  = ANY($5)) )" +
	" AND ( $6::text[] IS NULL OR     type LIKE ANY($6)  )" +
	" AND ( $7::text[] IS NULL OR NOT(type LIKE ANY($7)) )" +
	" ORDER BY id ASC LIMIT $3"

// SQL query to purge events
const purgeEventsSQL = "" +
	"DELETE FROM syncapi_output_room_events WHERE room_id = $1"

// SQL query to select search
const selectSearchSQL = "SELECT id, event_id, headered_event_json FROM syncapi_output_room_events WHERE id > $1 AND type = ANY($2) ORDER BY id ASC LIMIT $3"

// SQL query to exclude events from search index
const excludeEventsFromIndexSQL = "UPDATE syncapi_output_room_events SET exclude_from_search = true WHERE event_id = ANY($1)"

// SQL query to search for events
const searchEventsSQL = `SELECT id, event_id, headered_event_json, history_visibility, '' AS highlight, paradedb.score(event_id) AS score  
FROM syncapi_output_room_events WHERE %s ORDER BY score LIMIT $%d OFFSET $%d`

// SQL query to count search results
const searchEventsCountSQL = `SELECT COUNT(*) FROM syncapi_output_room_events WHERE %s`

// outputRoomEventsTable represents the table for storing output room events
type outputRoomEventsTable struct {
	cm sqlutil.ConnectionManager

	// SQL query fields
	insertEventSQL                string
	selectEventsSQL               string
	selectEventsWithFilterSQL     string
	selectMaxEventIDSQL           string
	selectRecentEventsSQL         string
	selectRecentEventsForSyncSQL  string
	selectStateInRangeFilteredSQL string
	selectStateInRangeSQL         string
	updateEventJSONSQL            string
	deleteEventsForRoomSQL        string
	selectContextEventSQL         string
	selectContextBeforeEventSQL   string
	selectContextAfterEventSQL    string
	purgeEventsSQL                string
	selectSearchSQL               string
	excludeEventsFromIndexSQL     string
	searchEventsSQL               string
	searchEventsCountSQL          string
}

// NewPostgresEventsTable creates a new events table
func NewPostgresEventsTable(_ context.Context, cm sqlutil.ConnectionManager) (tables.Events, error) {

	// Run migrations
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "syncapi_output_room_events_table_schema_001",
		Patch:       outputRoomEventsSchema,
		RevertPatch: outputRoomEventsSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	t := &outputRoomEventsTable{
		cm: cm,

		// Initialize SQL query fields
		insertEventSQL:                insertEventSQL,
		selectEventsSQL:               selectEventsSQL,
		selectEventsWithFilterSQL:     selectEventsWithFilterSQL,
		selectMaxEventIDSQL:           selectMaxEventIDSQL,
		selectRecentEventsSQL:         selectRecentEventsSQL,
		selectRecentEventsForSyncSQL:  selectRecentEventsForSyncSQL,
		selectStateInRangeFilteredSQL: selectStateInRangeFilteredSQL,
		selectStateInRangeSQL:         selectStateInRangeSQL,
		updateEventJSONSQL:            updateEventJSONSQL,
		deleteEventsForRoomSQL:        deleteEventsForRoomSQL,
		selectContextEventSQL:         selectContextEventSQL,
		selectContextBeforeEventSQL:   selectContextBeforeEventSQL,
		selectContextAfterEventSQL:    selectContextAfterEventSQL,
		purgeEventsSQL:                purgeEventsSQL,
		selectSearchSQL:               selectSearchSQL,
		excludeEventsFromIndexSQL:     excludeEventsFromIndexSQL,
		searchEventsSQL:               searchEventsSQL,
		searchEventsCountSQL:          searchEventsCountSQL,
	}

	return t, nil
}

// UpdateEventJSON updates the event JSON for a given event
func (t *outputRoomEventsTable) UpdateEventJSON(ctx context.Context, event *rstypes.HeaderedEvent) error {
	headeredJSON, err := json.Marshal(event)
	if err != nil {
		return err
	}

	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.updateEventJSONSQL, headeredJSON, event.EventID())
	return result.Error
}

// SelectStateInRange returns the state events between the two given PDU stream positions, exclusive of oldPos, inclusive of newPos.
// Results are bucketed based on the room ID. If the same state is overwritten multiple times between the
// two positions, only the most recent state is returned.
func (t *outputRoomEventsTable) SelectStateInRange(
	ctx context.Context, r types.Range,
	stateFilter *synctypes.StateFilter, roomIDs []string,
) (map[string]map[string]bool, map[string]types.StreamEvent, error) {
	db := t.cm.Connection(ctx, true)
	var rows *sql.Rows
	var err error
	if stateFilter != nil {
		senders, notSenders := getSendersStateFilterFilter(stateFilter)
		rows, err = db.Raw(t.selectStateInRangeFilteredSQL, r.Low(), r.High(), pq.StringArray(roomIDs),
			pq.StringArray(senders),
			pq.StringArray(notSenders),
			pq.StringArray(filterConvertTypeWildcardToSQL(stateFilter.Types)),
			pq.StringArray(filterConvertTypeWildcardToSQL(stateFilter.NotTypes)),
			stateFilter.ContainsURL,
		).Rows()
	} else {
		rows, err = db.Raw(t.selectStateInRangeSQL, r.Low(), r.High(), pq.StringArray(roomIDs)).Rows()
	}
	if err != nil {
		return nil, nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectStateInRange: rows.close() failed")
	// Fetch all the state change events for all rooms between the two positions then loop each event and:
	//  - Keep a cache of the event by ID (99% of state change events are for the event itself)
	//  - For each room ID, build up an array of event IDs which represents cumulative adds/removes
	// For each room, map cumulative event IDs to events and return. This may need to a batch SELECT based on event ID
	// if they aren't in the event ID cache. We don't handle state deletion yet.
	eventIDToEvent := make(map[string]types.StreamEvent)

	// RoomID => A set (map[string]bool) of state event IDs which are between the two positions
	stateNeeded := make(map[string]map[string]bool)

	for rows.Next() {
		var (
			eventID           string
			streamPos         types.StreamPosition
			eventBytes        []byte
			excludeFromSync   bool
			addIDs            pq.StringArray
			delIDs            pq.StringArray
			historyVisibility gomatrixserverlib.HistoryVisibility
		)
		if err := rows.Scan(&eventID, &streamPos, &eventBytes, &excludeFromSync, &addIDs, &delIDs, &historyVisibility); err != nil {
			return nil, nil, err
		}

		// TODO: Handle redacted events
		var ev rstypes.HeaderedEvent
		if err := json.Unmarshal(eventBytes, &ev); err != nil {
			return nil, nil, err
		}
		needSet := stateNeeded[ev.RoomID().String()]
		if needSet == nil { // make set if required
			needSet = make(map[string]bool)
		}
		for _, id := range delIDs {
			needSet[id] = false
		}
		for _, id := range addIDs {
			needSet[id] = true
		}
		stateNeeded[ev.RoomID().String()] = needSet
		ev.Visibility = historyVisibility

		eventIDToEvent[eventID] = types.StreamEvent{
			HeaderedEvent:   &ev,
			StreamPosition:  streamPos,
			ExcludeFromSync: excludeFromSync,
		}
	}

	return stateNeeded, eventIDToEvent, rows.Err()
}

// SelectMaxEventID returns the ID of the last inserted event in this table. 'txn' is optional. If it is not supplied,
// then this function should only ever be used at startup, as it will race with inserting events if it is
// done afterwards. If there are no inserted events, 0 is returned.
func (t *outputRoomEventsTable) SelectMaxEventID(
	ctx context.Context,
) (id int64, err error) {
	var nullableID sql.NullInt64
	db := t.cm.Connection(ctx, true)
	err = db.Raw(t.selectMaxEventIDSQL).Row().Scan(&nullableID)
	if err != nil {
		return 0, err
	}

	if nullableID.Valid {
		id = nullableID.Int64
	}

	return id, nil
}

// InsertEvent into the output_room_events table. addState and removeState are an optional list of state event IDs. Returns the position
// of the inserted event.
func (t *outputRoomEventsTable) InsertEvent(
	ctx context.Context,
	event *rstypes.HeaderedEvent, addState, removeState []string,
	transactionID *api.TransactionID, excludeFromSync bool, historyVisibility gomatrixserverlib.HistoryVisibility,
) (streamPos types.StreamPosition, err error) {
	var txnID *string
	var sessionID *int64
	if transactionID != nil {
		sessionID = &transactionID.SessionID
		txnID = &transactionID.TransactionID
	}

	// Parse content as JSON and search for an "url" key
	containsURL := false
	var content map[string]interface{}
	if json.Unmarshal(event.Content(), &content) == nil {
		// Set containsURL to true if url is present
		_, containsURL = content["url"]
	}

	var headeredJSON []byte
	headeredJSON, err = json.Marshal(event)
	if err != nil {
		return
	}

	db := t.cm.Connection(ctx, false)
	row := db.Raw(t.insertEventSQL,
		event.RoomID().String(),
		event.EventID(),
		headeredJSON,
		event.Type(),
		event.UserID.String(),
		containsURL,
		pq.StringArray(addState),
		pq.StringArray(removeState),
		sessionID,
		txnID,
		excludeFromSync,
		historyVisibility,
	).Row()
	err = row.Scan(&streamPos)
	return
}

// selectRecentEvents returns the most recent events in the given room, up to a maximum of 'limit'.
// If onlySyncEvents has a value of true, only returns the events that aren't marked as to exclude
// from sync.
func (t *outputRoomEventsTable) SelectRecentEvents(
	ctx context.Context,
	roomIDs []string, ra types.Range, eventFilter *synctypes.RoomEventFilter,
	chronologicalOrder bool, onlySyncEvents bool,
) (map[string]types.RecentEvents, error) {
	var queryToRun string
	if onlySyncEvents {
		queryToRun = t.selectRecentEventsForSyncSQL
	} else {
		queryToRun = t.selectRecentEventsSQL
	}
	senders, notSenders := getSendersRoomEventFilter(eventFilter)

	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(queryToRun, pq.StringArray(roomIDs), ra.Low(), ra.High(),
		pq.StringArray(senders),
		pq.StringArray(notSenders),
		pq.StringArray(filterConvertTypeWildcardToSQL(eventFilter.Types)),
		pq.StringArray(filterConvertTypeWildcardToSQL(eventFilter.NotTypes)),
		eventFilter.Limit+1,
	).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectRecentEvents: rows.close() failed")

	result := make(map[string]types.RecentEvents)

	for rows.Next() {
		var (
			roomID            string
			eventID           string
			streamPos         types.StreamPosition
			eventBytes        []byte
			excludeFromSync   bool
			sessionID         *int64
			txnID             *string
			transactionID     *api.TransactionID
			historyVisibility gomatrixserverlib.HistoryVisibility
		)
		if err := rows.Scan(&roomID, &eventID, &streamPos, &eventBytes, &sessionID, &excludeFromSync, &txnID, &historyVisibility); err != nil {
			return nil, err
		}
		// TODO: Handle redacted events
		var ev rstypes.HeaderedEvent
		if err := json.Unmarshal(eventBytes, &ev); err != nil {
			return nil, err
		}

		if sessionID != nil && txnID != nil {
			transactionID = &api.TransactionID{
				SessionID:     *sessionID,
				TransactionID: *txnID,
			}
		}

		r := result[roomID]

		ev.Visibility = historyVisibility
		r.Events = append(r.Events, types.StreamEvent{
			HeaderedEvent:   &ev,
			StreamPosition:  streamPos,
			TransactionID:   transactionID,
			ExcludeFromSync: excludeFromSync,
		})

		result[roomID] = r
	}

	if chronologicalOrder {
		for roomID, evs := range result {
			// The events need to be returned from oldest to latest, which isn't
			// necessary the way the SQL query returns them, so a sort is necessary to
			// ensure the events are in the right order in the slice.
			sort.SliceStable(evs.Events, func(i int, j int) bool {
				return evs.Events[i].StreamPosition < evs.Events[j].StreamPosition
			})

			if len(evs.Events) > eventFilter.Limit {
				evs.Limited = true
				evs.Events = evs.Events[1:]
			}

			result[roomID] = evs
		}
	} else {
		for roomID, evs := range result {
			if len(evs.Events) > eventFilter.Limit {
				evs.Limited = true
				evs.Events = evs.Events[:len(evs.Events)-1]
			}

			result[roomID] = evs
		}
	}
	return result, rows.Err()
}

// SelectEvents returns the events for the given event IDs
func (t *outputRoomEventsTable) SelectEvents(
	ctx context.Context, eventIDs []string, filter *synctypes.RoomEventFilter, preserveOrder bool,
) ([]types.StreamEvent, error) {
	db := t.cm.Connection(ctx, true)
	var rows *sql.Rows
	var err error

	if filter == nil {
		rows, err = db.Raw(t.selectEventsSQL, pq.Array(eventIDs)).Rows()
	} else {
		rows, err = db.Raw(
			t.selectEventsWithFilterSQL,
			pq.Array(eventIDs),
			pq.Array(filter.Senders),
			pq.Array(filter.NotSenders),
			pq.Array(filterConvertTypeWildcardToSQL(filter.Types)),
			pq.Array(filterConvertTypeWildcardToSQL(filter.NotTypes)),
			filter.ContainsURL,
			filter.Limit,
		).Rows()
	}
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectEvents: rows.close() failed")

	var streamEvents []types.StreamEvent
	for rows.Next() {
		var (
			streamPos         types.StreamPosition
			eventID           string
			eventBytes        []byte
			sessionID         *int64
			excludeFromSync   bool
			txnID             *string
			transactionID     *api.TransactionID
			historyVisibility int
		)
		if err := rows.Scan(&streamPos, &eventID, &eventBytes, &sessionID, &excludeFromSync, &txnID, &historyVisibility); err != nil {
			return nil, err
		}

		var ev rstypes.HeaderedEvent
		if err := json.Unmarshal(eventBytes, &ev); err != nil {
			return nil, err
		}

		if sessionID != nil && txnID != nil {
			transactionID = &api.TransactionID{
				SessionID:     *sessionID,
				TransactionID: *txnID,
			}
		}

		// Convert history visibility from int to enum
		hisVis := gomatrixserverlib.HistoryVisibilityShared
		switch historyVisibility {
		case 1:
			hisVis = gomatrixserverlib.HistoryVisibilityWorldReadable
		case 3:
			hisVis = gomatrixserverlib.HistoryVisibilityInvited
		case 4:
			hisVis = gomatrixserverlib.HistoryVisibilityJoined
		}
		ev.Visibility = hisVis

		streamEvents = append(streamEvents, types.StreamEvent{
			HeaderedEvent:   &ev,
			StreamPosition:  streamPos,
			TransactionID:   transactionID,
			ExcludeFromSync: excludeFromSync,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if preserveOrder {
		eventMap := make(map[string]types.StreamEvent)
		for _, ev := range streamEvents {
			eventMap[ev.EventID()] = ev
		}
		var returnEvents []types.StreamEvent
		for _, eventID := range eventIDs {
			ev, ok := eventMap[eventID]
			if ok {
				returnEvents = append(returnEvents, ev)
			}
		}
		return returnEvents, nil
	}

	return streamEvents, nil
}

// DeleteEventsForRoom deletes all events for a room
func (t *outputRoomEventsTable) DeleteEventsForRoom(ctx context.Context, roomID string) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteEventsForRoomSQL, roomID)
	return result.Error
}

// SelectContextEvent returns a single event
func (t *outputRoomEventsTable) SelectContextEvent(ctx context.Context, roomID, eventID string) (id int, evt rstypes.HeaderedEvent, err error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectContextEventSQL, roomID, eventID).Row()

	var eventAsString string
	var historyVisibility gomatrixserverlib.HistoryVisibility
	if err = row.Scan(&id, &eventAsString, &historyVisibility); err != nil {
		return 0, evt, err
	}

	if err = json.Unmarshal([]byte(eventAsString), &evt); err != nil {
		return 0, evt, err
	}
	evt.Visibility = historyVisibility
	return id, evt, nil
}

// SelectContextBeforeEvent returns events before a given event
func (t *outputRoomEventsTable) SelectContextBeforeEvent(
	ctx context.Context, id int, roomID string, filter *synctypes.RoomEventFilter,
) (evts []*rstypes.HeaderedEvent, err error) {
	senders, notSenders := getSendersRoomEventFilter(filter)

	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectContextBeforeEventSQL, roomID, id, filter.Limit,
		pq.StringArray(senders),
		pq.StringArray(notSenders),
		pq.StringArray(filterConvertTypeWildcardToSQL(filter.Types)),
		pq.StringArray(filterConvertTypeWildcardToSQL(filter.NotTypes)),
	).Rows()
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "rows.close() failed")

	for rows.Next() {
		var (
			eventBytes        []byte
			evt               *rstypes.HeaderedEvent
			historyVisibility gomatrixserverlib.HistoryVisibility
		)
		if err = rows.Scan(&eventBytes, &historyVisibility); err != nil {
			return evts, err
		}
		if err = json.Unmarshal(eventBytes, &evt); err != nil {
			return evts, err
		}
		evt.Visibility = historyVisibility
		evts = append(evts, evt)
	}

	return evts, rows.Err()
}

// SelectContextAfterEvent returns events after a given event
func (t *outputRoomEventsTable) SelectContextAfterEvent(
	ctx context.Context, id int, roomID string, filter *synctypes.RoomEventFilter,
) (lastID int, evts []*rstypes.HeaderedEvent, err error) {
	senders, notSenders := getSendersRoomEventFilter(filter)

	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectContextAfterEventSQL, roomID, id, filter.Limit,
		pq.StringArray(senders),
		pq.StringArray(notSenders),
		pq.StringArray(filterConvertTypeWildcardToSQL(filter.Types)),
		pq.StringArray(filterConvertTypeWildcardToSQL(filter.NotTypes)),
	).Rows()
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "rows.close() failed")

	for rows.Next() {
		var (
			eventBytes        []byte
			evt               *rstypes.HeaderedEvent
			historyVisibility gomatrixserverlib.HistoryVisibility
		)
		if err = rows.Scan(&lastID, &eventBytes, &historyVisibility); err != nil {
			return 0, evts, err
		}
		if err = json.Unmarshal(eventBytes, &evt); err != nil {
			return 0, evts, err
		}
		evt.Visibility = historyVisibility
		evts = append(evts, evt)
	}

	return lastID, evts, rows.Err()
}

// PurgeEvents removes all events for a room ID
func (t *outputRoomEventsTable) PurgeEvents(ctx context.Context, roomID string) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.purgeEventsSQL, roomID)
	return result.Error
}

// ReIndex retrieves events for reindexing
func (t *outputRoomEventsTable) ReIndex(ctx context.Context, limit, afterID int64, eventTypes []string) (map[int64]rstypes.HeaderedEvent, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectSearchSQL, afterID, pq.Array(eventTypes), limit).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "rows.close() failed")

	var eventID string
	var id int64
	result := make(map[int64]rstypes.HeaderedEvent)
	for rows.Next() {
		var ev rstypes.HeaderedEvent
		var eventBytes []byte
		if err = rows.Scan(&id, &eventID, &eventBytes); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(eventBytes, &ev); err != nil {
			return nil, err
		}
		result[id] = ev
	}
	return result, rows.Err()
}

// escapeSpecialCharacters escapes the special characters in a query term
func (t *outputRoomEventsTable) escapeSpecialCharacters(input string) string {
	specialChars := "+,^`,:{},\"[],()<>,~!\\* "
	escaped := strings.Builder{}

	for _, char := range input {
		if strings.ContainsRune(specialChars, char) {
			escaped.WriteRune('\\')
		}
		escaped.WriteRune(char)
	}

	return escaped.String()
}

// SearchEvents performs a search for events matching the given criteria
func (t *outputRoomEventsTable) SearchEvents(
	ctx context.Context, searchTerm string, roomIDs []string,
	keys []string, limit, offset int,
) (*types.SearchResult, error) {
	var (
		rows            *sql.Rows
		err             error
		whereKeyStrings []string
		whereParams     []any
		totalCount      int
	)

	escapedRoomIDs := make([]string, len(roomIDs))
	for i, roomID := range roomIDs {
		escapedRoomIDs[i] = t.escapeSpecialCharacters(roomID)
	}
	whereParams = append(whereParams, strings.Join(escapedRoomIDs, " "))

	whereRoomIDQueryStr := ""
	if len(roomIDs) > 0 {
		whereRoomIDQueryStr = " room_id @@@ $1  AND "
	}

	whereExclusionQueryStr := " exclude_from_search @@@ 'false' "

	if len(keys) == 0 {
		keys = append(keys, "content.body", "content.name", "content.topic")
	}

	k := len(whereParams) + 1
	for i, key := range keys {
		j := i + k
		whereParams = append(whereParams, searchTerm)
		whereKeyString := fmt.Sprintf(" paradedb.match( field => 'headered_event_json.%s', value => $%d, distance => 0) ", key, j)
		whereKeyStrings = append(whereKeyStrings, whereKeyString)
	}

	whereShouldQueryStr := fmt.Sprintf(" should => ARRAY[ %s ]", strings.Join(whereKeyStrings, ", "))

	finalWhereStr := fmt.Sprintf("%s %s AND event_id @@@ paradedb.boolean(  %s )", whereRoomIDQueryStr, whereExclusionQueryStr, whereShouldQueryStr)

	db := t.cm.Connection(ctx, true)

	// Get total count
	totalCountQuery := fmt.Sprintf(t.searchEventsCountSQL, finalWhereStr)
	err = db.Raw(totalCountQuery, whereParams...).Row().Scan(&totalCount)
	if err != nil {
		return nil, err
	}

	// Get search results
	parameterCount := len(whereParams)
	finalQuery := fmt.Sprintf(t.searchEventsSQL, finalWhereStr, parameterCount+1, parameterCount+2)
	whereParams = append(whereParams, limit, offset)

	rows, err = db.Raw(finalQuery, whereParams...).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SearchEvents: rows.close() failed")
	resultHits, err := rowsToSearchResult(rows)
	if err != nil {
		return nil, err
	}

	return &types.SearchResult{Results: resultHits, Total: totalCount}, nil
}

func rowsToStreamEvents(rows *sql.Rows) ([]types.StreamEvent, error) {
	var result []types.StreamEvent
	for rows.Next() {
		var (
			eventID           string
			streamPos         types.StreamPosition
			eventBytes        []byte
			excludeFromSync   bool
			sessionID         *int64
			txnID             *string
			transactionID     *api.TransactionID
			historyVisibility gomatrixserverlib.HistoryVisibility
		)
		if err := rows.Scan(&streamPos, &eventID, &eventBytes, &sessionID, &excludeFromSync, &txnID, &historyVisibility); err != nil {
			return nil, err
		}
		// TODO: Handle redacted events
		var ev rstypes.HeaderedEvent
		if err := json.Unmarshal(eventBytes, &ev); err != nil {
			return nil, err
		}

		if sessionID != nil && txnID != nil {
			transactionID = &api.TransactionID{
				SessionID:     *sessionID,
				TransactionID: *txnID,
			}
		}
		ev.Visibility = historyVisibility
		result = append(result, types.StreamEvent{
			HeaderedEvent:   &ev,
			StreamPosition:  streamPos,
			TransactionID:   transactionID,
			ExcludeFromSync: excludeFromSync,
		})
	}
	return result, rows.Err()
}

func rowsToSearchResult(rows *sql.Rows) ([]*types.SearchResultHit, error) {
	var result []*types.SearchResultHit

	for rows.Next() {
		var (
			eventID           string
			highlight         *string
			streamPos         types.StreamPosition
			eventBytes        []byte
			score             *float64
			historyVisibility gomatrixserverlib.HistoryVisibility
		)
		if err := rows.Scan(&streamPos, &eventID, &eventBytes, &historyVisibility, &highlight, &score); err != nil {
			return nil, err
		}
		// TODO: Handle redacted events
		var ev rstypes.HeaderedEvent
		if err := json.Unmarshal(eventBytes, &ev); err != nil {
			return nil, err
		}

		highlightFinal := ""
		if highlight != nil {
			highlightFinal = *highlight
		}

		ev.Visibility = historyVisibility
		result = append(result, &types.SearchResultHit{
			Event:          &ev,
			StreamPosition: streamPos,
			Highlight:      highlightFinal,
			Score:          score,
		})
	}
	return result, rows.Err()
}

func (t *outputRoomEventsTable) ExcludeEventsFromSearchIndex(ctx context.Context, eventIDs []string) error {
	db := t.cm.Connection(ctx, true)
	err := db.Exec(t.excludeEventsFromIndexSQL, pq.StringArray(eventIDs)).Error
	return err
}
