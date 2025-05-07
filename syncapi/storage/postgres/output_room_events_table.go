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
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/api"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/syncapi/storage/postgres/deltas"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/lib/pq"
)

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

// SQL query constants for output room events
const (
	// Insert a new event.
	insertEventSQL = `
		INSERT INTO syncapi_output_room_events (
			room_id, event_id, headered_event_json, type, sender, 
			contains_url, add_state_ids, remove_state_ids, 
			session_id, transaction_id, exclude_from_sync, history_visibility
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT ON CONSTRAINT syncapi_output_room_event_id_idx 
		DO UPDATE SET exclude_from_sync = (excluded.exclude_from_sync AND $11)
		RETURNING id
	`

	// Select events by their event IDs.
	selectEventsSQL = `
		SELECT id, event_id, headered_event_json, session_id, exclude_from_sync, transaction_id, history_visibility 
		FROM syncapi_output_room_events 
		WHERE event_id = ANY($1)
	`

	// Select events with a filter.
	selectEventsWithFilterSQL = `
		SELECT id, event_id, headered_event_json, session_id, exclude_from_sync, transaction_id, history_visibility 
		FROM syncapi_output_room_events 
		WHERE event_id = ANY($1)
		AND ( $2::text[] IS NULL OR     sender  = ANY($2)  )
		AND ( $3::text[] IS NULL OR NOT(sender  = ANY($3)) )
		AND ( $4::text[] IS NULL OR     type LIKE ANY($4)  )
		AND ( $5::text[] IS NULL OR NOT(type LIKE ANY($5)) )
		AND ( $6::bool   IS NULL OR     contains_url = $6 )
		LIMIT $7
	`

	// Select recent events in a room and apply a filter.
	selectRecentEventsSQL = `
		SELECT event_id, id, headered_event_json, session_id, exclude_from_sync, transaction_id, history_visibility 
		FROM syncapi_output_room_events
		WHERE room_id = $1 AND id > $2 AND id <= $3
		AND ( $4::text[] IS NULL OR     sender  = ANY($4)  )
		AND ( $5::text[] IS NULL OR NOT(sender  = ANY($5)) )
		AND ( $6::text[] IS NULL OR     type LIKE ANY($6)  )
		AND ( $7::text[] IS NULL OR NOT(type LIKE ANY($7)) )
		ORDER BY id DESC LIMIT $8
	`

	// Select recent events for sync with optimization using LATERAL JOIN.
	selectRecentEventsForSyncSQL = `
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

	// Get the maximum ID.
	selectMaxEventIDSQL = `
		SELECT MAX(id) FROM syncapi_output_room_events
	`

	// Update event JSON.
	updateEventJSONSQL = `
		UPDATE syncapi_output_room_events SET headered_event_json=$1 WHERE event_id=$2
	`

	// Select state in range with a filter.
	selectStateInRangeFilteredSQL = `
		SELECT event_id, id, headered_event_json, exclude_from_sync, add_state_ids, remove_state_ids, history_visibility
		FROM syncapi_output_room_events
		WHERE (id > $1 AND id <= $2) AND (add_state_ids IS NOT NULL OR remove_state_ids IS NOT NULL)
		AND room_id = ANY($3)
		AND ( $4::text[] IS NULL OR     sender  = ANY($4)  )
		AND ( $5::text[] IS NULL OR NOT(sender  = ANY($5)) )
		AND ( $6::text[] IS NULL OR     type LIKE ANY($6)  )
		AND ( $7::text[] IS NULL OR NOT(type LIKE ANY($7)) )
		AND ( $8::bool IS NULL   OR     contains_url = $8  )
		ORDER BY id ASC
	`

	// Select state in range.
	selectStateInRangeSQL = `
		SELECT event_id, id, headered_event_json, exclude_from_sync, add_state_ids, remove_state_ids, history_visibility
		FROM syncapi_output_room_events
		WHERE (id > $1 AND id <= $2) AND (add_state_ids IS NOT NULL OR remove_state_ids IS NOT NULL)
		AND room_id = ANY($3)
		ORDER BY id ASC
	`

	// Delete events for a room.
	deleteEventsForRoomSQL = `
		DELETE FROM syncapi_output_room_events WHERE room_id = $1
	`

	// Select a context event.
	selectContextEventSQL = `
		SELECT id, headered_event_json, history_visibility 
		FROM syncapi_output_room_events 
		WHERE room_id = $1 AND event_id = $2
	`

	// Select context before an event.
	selectContextBeforeEventSQL = `
		SELECT headered_event_json, history_visibility 
		FROM syncapi_output_room_events 
		WHERE room_id = $1 AND id < $2
		AND ( $3::text[] IS NULL OR     sender  = ANY($3)  )
		AND ( $4::text[] IS NULL OR NOT(sender  = ANY($4)) )
		AND ( $5::text[] IS NULL OR     type LIKE ANY($5)  )
		AND ( $6::text[] IS NULL OR NOT(type LIKE ANY($6)) )
		ORDER BY id DESC LIMIT $7
	`

	// Select context after an event.
	selectContextAfterEventSQL = `
		SELECT id, headered_event_json, history_visibility 
		FROM syncapi_output_room_events 
		WHERE room_id = $1 AND id > $2
		AND ( $3::text[] IS NULL OR     sender  = ANY($3)  )
		AND ( $4::text[] IS NULL OR NOT(sender  = ANY($4)) )
		AND ( $5::text[] IS NULL OR     type LIKE ANY($5)  )
		AND ( $6::text[] IS NULL OR NOT(type LIKE ANY($6)) )
		ORDER BY id ASC LIMIT $7
	`

	// Purge events.
	purgeEventsSQL = `
		DELETE FROM syncapi_output_room_events WHERE room_id = $1
	`

	// Select events for search.
	selectSearchSQL = `
		SELECT id, event_id, headered_event_json 
		FROM syncapi_output_room_events 
		WHERE id > $1 AND type = ANY($2) 
		ORDER BY id ASC LIMIT $3
	`

	// Exclude events from search index.
	excludeEventsFromIndexSQL = `
		UPDATE syncapi_output_room_events SET exclude_from_search = TRUE WHERE event_id = ANY($1)
	`
)

type outputRoomEventsTable struct {
	cm *sqlutil.Connections

	// SQL query strings
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
}

func NewPostgresEventsTable(ctx context.Context, cm *sqlutil.Connections) (tables.Events, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(outputRoomEventsSchema).Error; err != nil {
		return nil, err
	}

	db2, err := cm.Writer.DB.DB()
	if err != nil {
		return nil, err
	}

	migrationName := "syncapi: rename dupe index (output_room_events)"

	var cName string
	err = db2.QueryRowContext(ctx, "select constraint_name from information_schema.table_constraints where table_name = 'syncapi_output_room_events' AND constraint_name = 'syncapi_event_id_idx'").Scan(&cName)
	switch {
	case errors.Is(err, sql.ErrNoRows): // migration was already executed, as the index was renamed
		if err = sqlutil.InsertMigration(ctx, db2, migrationName); err != nil {
			return nil, fmt.Errorf("unable to manually insert migration '%s': %w", migrationName, err)
		}
	case err == nil:
	default:
		return nil, err
	}

	m := sqlutil.NewMigrator(db2)
	m.AddMigrations(
		sqlutil.Migration{
			Version: "syncapi: add history visibility column (output_room_events)",
			Up:      deltas.UpAddHistoryVisibilityColumnOutputRoomEvents,
		},
		sqlutil.Migration{
			Version: migrationName,
			Up:      deltas.UpRenameOutputRoomEventsIndex,
		},
	)
	err = m.Up(ctx)
	if err != nil {
		return nil, err
	}

	// Initialize the table with SQL statements
	s := &outputRoomEventsTable{
		cm:                            cm,
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
	}

	return s, nil
}

func (s *outputRoomEventsTable) UpdateEventJSON(ctx context.Context, event *rstypes.HeaderedEvent) error {
	headeredJSON, err := json.Marshal(event)
	if err != nil {
		return err
	}
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.updateEventJSONSQL, headeredJSON, event.EventID()).Error
}

// selectStateInRange returns the state events between the two given PDU stream positions, exclusive of oldPos, inclusive of newPos.
// Results are bucketed based on the room ID. If the same state is overwritten multiple times between the
// two positions, only the most recent state is returned.
func (s *outputRoomEventsTable) SelectStateInRange(ctx context.Context, r types.Range, stateFilter *synctypes.StateFilter, roomIDs []string) (map[string]map[string]bool, map[string]types.StreamEvent, error) {
	var rows *sql.Rows
	var err error
	if stateFilter != nil {
		db := s.cm.Connection(ctx, false)
		senders, notSenders := getSendersStateFilterFilter(stateFilter)
		rows, err = db.Raw(
			s.selectStateInRangeFilteredSQL, r.Low(), r.High(), pq.StringArray(roomIDs),
			pq.StringArray(senders),
			pq.StringArray(notSenders),
			pq.StringArray(filterConvertTypeWildcardToSQL(stateFilter.Types)),
			pq.StringArray(filterConvertTypeWildcardToSQL(stateFilter.NotTypes)),
			stateFilter.ContainsURL,
		).Rows()
	} else {
		db := s.cm.Connection(ctx, false)
		rows, err = db.Raw(
			s.selectStateInRangeSQL, r.Low(), r.High(), pq.StringArray(roomIDs),
		).Rows()
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
func (s *outputRoomEventsTable) SelectMaxEventID(ctx context.Context) (id int64, err error) {
	var nullableID sql.NullInt64
	db := s.cm.Connection(ctx, false)
	err = db.Raw(s.selectMaxEventIDSQL).Row().Scan(&nullableID)
	if nullableID.Valid {
		id = nullableID.Int64
	}
	return
}

// InsertEvent into the output_room_events table. addState and removeState are an optional list of state event IDs. Returns the position
// of the inserted event.
func (s *outputRoomEventsTable) InsertEvent(ctx context.Context, event *rstypes.HeaderedEvent, addState, removeState []string, transactionID *api.TransactionID, excludeFromSync bool, historyVisibility gomatrixserverlib.HistoryVisibility) (streamPos types.StreamPosition, err error) {
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

	db := s.cm.Connection(ctx, false)
	err = db.Raw(
		s.insertEventSQL,
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
	).Row().Scan(&streamPos)
	return
}

// selectRecentEvents returns the most recent events in the given room, up to a maximum of 'limit'.
// If onlySyncEvents has a value of true, only returns the events that aren't marked as to exclude
// from sync.
func (s *outputRoomEventsTable) SelectRecentEvents(ctx context.Context, roomIDs []string, ra types.Range, eventFilter *synctypes.RoomEventFilter, chronologicalOrder bool, onlySyncEvents bool) (map[string]types.RecentEvents, error) {
	var stmt string
	if onlySyncEvents {
		stmt = s.selectRecentEventsForSyncSQL
	} else {
		stmt = s.selectRecentEventsSQL
	}
	db := s.cm.Connection(ctx, false)
	senders, notSenders := getSendersRoomEventFilter(eventFilter)

	rows, err := db.Raw(
		stmt, pq.StringArray(roomIDs), ra.Low(), ra.High(),
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

// SelectEvents returns the events for the given event IDs. If an event is
// missing from the database, it will be omitted.
func (s *outputRoomEventsTable) SelectEvents(ctx context.Context, eventIDs []string, filter *synctypes.RoomEventFilter, preserveOrder bool) ([]types.StreamEvent, error) {
	var rows *sql.Rows
	var err error
	db := s.cm.Connection(ctx, false)

	if filter == nil {
		rows, err = db.Raw(s.selectEventsSQL, pq.StringArray(eventIDs)).Rows()
	} else {
		senders, notSenders := getSendersRoomEventFilter(filter)
		rows, err = db.Raw(
			s.selectEventsWithFilterSQL,
			pq.StringArray(eventIDs),
			pq.StringArray(senders),
			pq.StringArray(notSenders),
			pq.StringArray(filterConvertTypeWildcardToSQL(filter.Types)),
			pq.StringArray(filterConvertTypeWildcardToSQL(filter.NotTypes)),
			filter.ContainsURL,
			filter.Limit,
		).Rows()
	}

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectEvents: rows.close() failed")

	streamEvents, eventIDMap, err := rowsToStreamEvents(rows)
	if err != nil {
		return nil, err
	}

	if preserveOrder {
		// The events need to be returned in the same order as eventIDs.
		var result []types.StreamEvent
		for _, eventID := range eventIDs {
			if event, ok := eventIDMap[eventID]; ok {
				result = append(result, event)
			}
		}
		return result, nil
	}

	return streamEvents, nil
}

func (s *outputRoomEventsTable) DeleteEventsForRoom(ctx context.Context, roomID string) (err error) {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.deleteEventsForRoomSQL, roomID).Error
}

func (s *outputRoomEventsTable) SelectContextEvent(ctx context.Context, roomID, eventID string) (id int, evt rstypes.HeaderedEvent, err error) {
	db := s.cm.Connection(ctx, true)
	row := db.Raw(s.selectContextEventSQL, eventID, roomID).Row()

	var eventBytes []byte
	var historyVisibility gomatrixserverlib.HistoryVisibility
	err = row.Scan(&id, &eventBytes, &historyVisibility)
	if err != nil {
		return 0, rstypes.HeaderedEvent{}, fmt.Errorf("failed to scan row: %w", err)
	}
	err = json.Unmarshal(eventBytes, &evt)
	if err != nil {
		return 0, rstypes.HeaderedEvent{}, fmt.Errorf("failed to unmarshal event: %w", err)
	}
	evt.Visibility = historyVisibility
	return id, evt, nil
}

func (s *outputRoomEventsTable) SelectContextBeforeEvent(ctx context.Context, id int, roomID string, filter *synctypes.RoomEventFilter) (evts []*rstypes.HeaderedEvent, err error) {
	db := s.cm.Connection(ctx, true)

	senders, notSenders := getSendersRoomEventFilter(filter)
	rows, err := db.Raw(
		s.selectContextBeforeEventSQL, roomID, id,
		pq.StringArray(senders),
		pq.StringArray(notSenders),
		pq.StringArray(filterConvertTypeWildcardToSQL(filter.Types)),
		pq.StringArray(filterConvertTypeWildcardToSQL(filter.NotTypes)),
		filter.Limit,
	).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectContextBeforeEvent: rows.close() failed")

	for rows.Next() {
		var eventBytes []byte
		var historyVisibility gomatrixserverlib.HistoryVisibility
		if err = rows.Scan(&eventBytes, &historyVisibility); err != nil {
			return nil, err
		}
		var event rstypes.HeaderedEvent
		if err = json.Unmarshal(eventBytes, &event); err != nil {
			return nil, err
		}
		event.Visibility = historyVisibility
		evts = append(evts, &event)
	}
	return evts, rows.Err()
}

func (s *outputRoomEventsTable) SelectContextAfterEvent(ctx context.Context, id int, roomID string, filter *synctypes.RoomEventFilter) (lastID int, evts []*rstypes.HeaderedEvent, err error) {
	db := s.cm.Connection(ctx, true)

	senders, notSenders := getSendersRoomEventFilter(filter)
	rows, err := db.Raw(
		s.selectContextAfterEventSQL, roomID, id,
		pq.StringArray(senders),
		pq.StringArray(notSenders),
		pq.StringArray(filterConvertTypeWildcardToSQL(filter.Types)),
		pq.StringArray(filterConvertTypeWildcardToSQL(filter.NotTypes)),
		filter.Limit,
	).Rows()
	if err != nil {
		return 0, nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectContextAfterEvent: rows.close() failed")

	for rows.Next() {
		var eventBytes []byte
		var historyVisibility gomatrixserverlib.HistoryVisibility
		if err = rows.Scan(&lastID, &eventBytes, &historyVisibility); err != nil {
			return 0, nil, err
		}
		var event rstypes.HeaderedEvent
		if err = json.Unmarshal(eventBytes, &event); err != nil {
			return 0, nil, err
		}
		event.Visibility = historyVisibility
		evts = append(evts, &event)
	}
	return lastID, evts, rows.Err()
}

func (s *outputRoomEventsTable) PurgeEvents(ctx context.Context, roomID string) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.purgeEventsSQL, roomID).Error
}

func (s *outputRoomEventsTable) ReIndex(ctx context.Context, limit, afterID int64, types []string) (map[int64]rstypes.HeaderedEvent, error) {
	db := s.cm.Connection(ctx, false)
	rows, err := db.Raw(s.selectSearchSQL, afterID, pq.StringArray(types), limit).Rows()
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

// escapeSpecialCharacters escapes the special characters in a query term.
func (s *outputRoomEventsTable) escapeSpecialCharacters(input string) string {
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

func (s *outputRoomEventsTable) SearchEvents(ctx context.Context, searchTerm string, roomIDs []string, keys []string, limit, offset int) (*types.SearchResult, error) {
	var err error
	var totalCount int64
	var rows *sql.Rows

	const (
		searchEventsSQL = `SELECT id, event_id, headered_event_json, history_visibility, '' AS highlight, paradedb.score(event_id) AS score  
		FROM syncapi_output_room_events WHERE %s ORDER BY score LIMIT $%d OFFSET $%d`

		searchEventsCountSQL = `SELECT COUNT(*) FROM syncapi_output_room_events WHERE %s`
	)

	var whereShouldTerms []string
	escapedSearchTerm := "(" + s.escapeSpecialCharacters(searchTerm) + ")"
	whereParams := make([]interface{}, 0)

	// Add the terms we're searching over
	var whereRoomIDQuery string
	if len(roomIDs) > 0 {
		whereRoomIDQuery = fmt.Sprintf("room_id = ANY($%d)", len(whereParams)+1)
		whereParams = append(whereParams, pq.StringArray(roomIDs))
	} else {
		whereRoomIDQuery = "1=1"
	}
	whereRoomIDQueryStr := "(" + whereRoomIDQuery + ")"

	// Exclude events from being searched
	whereExclusionQueryStr := "( exclude_from_search = FALSE )"

	// Find content in keys to search in
	// Start with sensible default
	if len(keys) == 0 {
		keys = []string{"content.body"}
	}

	// Build up boolean search terms
	for _, key := range keys {
		shouldQueryStr := fmt.Sprintf("%s:Lexeme:%s", key, escapedSearchTerm)
		whereShouldTerms = append(whereShouldTerms, shouldQueryStr)
	}
	whereShouldQueryStr := strings.Join(whereShouldTerms, " | ")

	// Build the final query
	finalWhereStr := fmt.Sprintf("%s %s AND event_id @@@ paradedb.boolean(%s)", whereRoomIDQueryStr, whereExclusionQueryStr, whereShouldQueryStr)

	// Get total count
	totalCountQuery := fmt.Sprintf(searchEventsCountSQL, finalWhereStr)
	db := s.cm.Connection(ctx, false)
	totalsRow := db.Raw(totalCountQuery, whereParams...).Row()
	err = totalsRow.Scan(&totalCount)
	if err != nil {
		return nil, err
	}

	// Get search results
	finalQuery := fmt.Sprintf(searchEventsSQL, finalWhereStr, len(whereParams)+1, len(whereParams)+2)
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

	return &types.SearchResult{Results: resultHits, Total: int(totalCount)}, nil
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

func rowsToStreamEvents(rows *sql.Rows) ([]types.StreamEvent, map[string]types.StreamEvent, error) {
	var result []types.StreamEvent
	eventIDMap := make(map[string]types.StreamEvent)
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
			return nil, nil, err
		}
		// TODO: Handle redacted events
		var ev rstypes.HeaderedEvent
		if err := json.Unmarshal(eventBytes, &ev); err != nil {
			return nil, nil, err
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
		eventIDMap[eventID] = result[len(result)-1]
	}

	return result, eventIDMap, rows.Err()
}

func (s *outputRoomEventsTable) ExcludeEventsFromSearchIndex(ctx context.Context, eventIDs []string) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.excludeEventsFromIndexSQL, pq.StringArray(eventIDs)).Error
}
