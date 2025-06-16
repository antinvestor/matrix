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

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/util"
)

// SQL schema for the state snapshot table
const stateSnapshotSchema = `
-- The state of a room before an event.
-- Stored as a list of state_block entries stored in a separate table.
-- The actual state is constructed by combining all the state_block entries
-- referenced by state_block_nids together. If the same state key tuple appears
-- multiple times then the entry from the later state_block clobbers the earlier
-- entries.
-- This encoding format allows us to implement a delta encoding which is useful
-- because room state tends to accumulate small changes over time. Although if
-- the list of deltas becomes too long it becomes more efficient to encode
-- the full state under single state_block_nid.
CREATE SEQUENCE IF NOT EXISTS roomserver_state_snapshot_nid_seq;
CREATE TABLE IF NOT EXISTS roomserver_state_snapshots (
	-- The state snapshot NID that identifies this snapshot.
	state_snapshot_nid bigint PRIMARY KEY DEFAULT nextval('roomserver_state_snapshot_nid_seq'),
	-- The hash of the state snapshot, which is used to enforce uniqueness. The hash is
	-- generated in Dendrite and passed through to the database, as a btree index over 
	-- this column is cheap and fits within the maximum index size.
	state_snapshot_hash BYTEA UNIQUE,
	-- The room NID that the snapshot belongs to.
	room_nid bigint NOT NULL,
	-- The state blocks contained within this snapshot.
	state_block_nids bigint[] NOT NULL
);
`

// SQL query to revert the state snapshot table schema
const stateSnapshotSchemaRevert = `
DROP TABLE IF EXISTS roomserver_state_snapshots;
DROP SEQUENCE IF EXISTS roomserver_state_snapshot_nid_seq;
`

// SQL queries for the state snapshot table

// Insert a new state snapshot. If we conflict on the hash column then
// we must perform an update so that the RETURNING statement returns the
// ID of the row that we conflicted with, so that we can then refer to
// the original snapshot.
const insertStateSQL = "" +
	"INSERT INTO roomserver_state_snapshots (state_snapshot_hash, room_nid, state_block_nids)" +
	" VALUES ($1, $2, $3)" +
	" ON CONFLICT (state_snapshot_hash) DO UPDATE SET room_nid=$2" +
	" RETURNING state_snapshot_nid"

// Bulk state data NID lookup.
// Sorting by state_snapshot_nid means we can use binary search over the result
// to lookup the state data NIDs for a state snapshot NID.
const bulkSelectStateBlockNIDsSQL = "" +
	"SELECT state_snapshot_nid, state_block_nids FROM roomserver_state_snapshots" +
	" WHERE state_snapshot_nid = ANY($1) ORDER BY state_snapshot_nid ASC"

// Looks up both the history visibility event and relevant membership events from
// a given domain name from a given state snapshot. This is used to optimise the
// helpers.CheckServerAllowedToSeeEvent function.
// TODO: There's a sequence scan here because of the hash join strategy, which is
// probably O(n) on state key entries, so there must be a way to avoid that somehow.
// Event type NIDs are:
// - 5: m.room.member as per https://github.com/antinvestor/matrix/blob/c7f7aec4d07d59120d37d5b16a900f6d608a75c4/roomserver/storage/postgres/event_types_table.go#L40
// - 7: m.room.history_visibility as per https://github.com/antinvestor/matrix/blob/c7f7aec4d07d59120d37d5b16a900f6d608a75c4/roomserver/storage/postgres/event_types_table.go#L42
const bulkSelectStateForHistoryVisibilitySQL = "" +
	"SELECT event_nid FROM (" +
	"  SELECT event_nid, event_type_nid, event_state_key_nid FROM roomserver_events" +
	"  WHERE (event_type_nid = 5 OR event_type_nid = 7)" +
	"  AND event_nid = ANY(" +
	"    SELECT UNNEST(event_nids) FROM roomserver_state_block" +
	"    WHERE state_block_nid = ANY(" +
	"      SELECT UNNEST(state_block_nids) FROM roomserver_state_snapshots" +
	"      WHERE state_snapshot_nid = $1" +
	"    )" +
	"  )" +
	"  ORDER BY depth ASC" +
	") AS roomserver_events" +
	" INNER JOIN roomserver_event_state_keys" +
	"  ON roomserver_events.event_state_key_nid = roomserver_event_state_keys.event_state_key_nid" +
	"  AND (event_type_nid = 7 OR event_state_key LIKE '%:' || $2)"

// bulkSelectMembershipForHistoryVisibilitySQL is an optimization to get membership events for a specific user for defined set of events.
// Returns the event_id of the event we want the membership event for, the event_id of the membership event and the membership event JSON.
const bulkSelectMembershipForHistoryVisibilitySQL = "" +
	"SELECT re.event_id, re2.event_id, rej.event_json" +
	" FROM roomserver_events re" +
	" LEFT JOIN roomserver_state_snapshots rss on re.state_snapshot_nid = rss.state_snapshot_nid" +
	" CROSS JOIN unnest(rss.state_block_nids) AS blocks(block_nid)" +
	" LEFT JOIN roomserver_state_block rsb ON rsb.state_block_nid = blocks.block_nid" +
	" CROSS JOIN unnest(rsb.event_nids) AS rsb2(event_nid)" +
	" JOIN roomserver_events re2 ON re2.room_nid = $3 AND re2.event_type_nid = 5 AND re2.event_nid = rsb2.event_nid AND re2.event_state_key_nid = $1" +
	" LEFT JOIN roomserver_event_json rej ON rej.event_nid = re2.event_nid" +
	" WHERE re.event_id = ANY($2)"

// StateSnapshot represents a state snapshot in the database
type StateSnapshot struct {
	StateSnapshotNID  int64   `gorm:"column:state_snapshot_nid;primaryKey"`
	StateSnapshotHash []byte  `gorm:"column:state_snapshot_hash;unique"`
	RoomNID           int64   `gorm:"column:room_nid;not null"`
	StateBlockNIDs    []int64 `gorm:"column:state_block_nids;type:bigint[]"`
}

// stateSnapshotStatements holds prepared SQL statements for the state snapshot table.
type stateSnapshotStatements struct {
	cm sqlutil.ConnectionManager

	// SQL query string fields
	insertStateSQL                              string
	bulkSelectStateBlockNIDsSQL                 string
	bulkSelectStateForHistoryVisibilitySQL      string
	bulkSelectMembershipForHistoryVisibilitySQL string
}

// NewPostgresStateSnapshotTable creates a new instance of the state snapshot table.
// If the table does not exist, it will be created.
func NewPostgresStateSnapshotTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.StateSnapshot, error) {
	s := &stateSnapshotStatements{
		cm: cm,

		insertStateSQL:                              insertStateSQL,
		bulkSelectStateBlockNIDsSQL:                 bulkSelectStateBlockNIDsSQL,
		bulkSelectStateForHistoryVisibilitySQL:      bulkSelectStateForHistoryVisibilitySQL,
		bulkSelectMembershipForHistoryVisibilitySQL: bulkSelectMembershipForHistoryVisibilitySQL,
	}

	// Create the table if it doesn't exist
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "roomserver_state_snapshots_schema_001",
		Patch:       stateSnapshotSchema,
		RevertPatch: stateSnapshotSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertState inserts a new state snapshot and returns a new state snapshot NID.
func (s *stateSnapshotStatements) InsertState(
	ctx context.Context, roomNID types.RoomNID, nids types.StateBlockNIDs,
) (stateNID types.StateSnapshotNID, err error) {
	// Sort and deduplicate state block NIDs
	nids = nids[:util.SortAndUnique(nids)]

	// Convert state block NIDs to int64 array
	blockNIDs := make([]int64, len(nids))
	for i := range nids {
		blockNIDs[i] = int64(nids[i])
	}

	// Get a connection
	db := s.cm.Connection(ctx, false)

	// Insert the state snapshot
	err = db.Raw(s.insertStateSQL, nids.Hash(), int64(roomNID), pq.Array(blockNIDs)).Row().Scan(&stateNID)
	return
}

// BulkSelectStateBlockNIDs returns the state block NIDs for the given state snapshot NIDs.
func (s *stateSnapshotStatements) BulkSelectStateBlockNIDs(
	ctx context.Context, stateNIDs []types.StateSnapshotNID,
) ([]types.StateBlockNIDList, error) {
	// Convert state snapshot NIDs to int64 array
	nids := make([]int64, len(stateNIDs))
	for i := range stateNIDs {
		nids[i] = int64(stateNIDs[i])
	}

	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	rows, err := db.Raw(s.bulkSelectStateBlockNIDsSQL, pq.Array(nids)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectStateBlockNIDs: rows.close() failed")

	// Process the results
	results := make([]types.StateBlockNIDList, len(stateNIDs))
	i := 0
	var stateBlockNIDs pq.Int64Array
	for ; rows.Next(); i++ {
		result := &results[i]
		if err = rows.Scan(&result.StateSnapshotNID, &stateBlockNIDs); err != nil {
			return nil, err
		}
		result.StateBlockNIDs = make([]types.StateBlockNID, len(stateBlockNIDs))
		for k := range stateBlockNIDs {
			result.StateBlockNIDs[k] = types.StateBlockNID(stateBlockNIDs[k])
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if i != len(stateNIDs) {
		return nil, types.MissingStateError(fmt.Sprintf("storage: state NIDs missing from the database (%d != %d)", i, len(stateNIDs)))
	}
	return results, nil
}

// BulkSelectStateForHistoryVisibility returns the event NIDs for history visibility events and members of a domain.
func (s *stateSnapshotStatements) BulkSelectStateForHistoryVisibility(
	ctx context.Context, stateSnapshotNID types.StateSnapshotNID, domain string,
) ([]types.EventNID, error) {
	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	rows, err := db.Raw(s.bulkSelectStateForHistoryVisibilitySQL, stateSnapshotNID, domain).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectStateForHistoryVisibility: rows.close() failed")

	// Process the results
	results := make([]types.EventNID, 0, 16)
	for rows.Next() {
		var eventNID types.EventNID
		if err = rows.Scan(&eventNID); err != nil {
			return nil, err
		}
		results = append(results, eventNID)
	}
	return results, rows.Err()
}

// BulkSelectMembershipForHistoryVisibility returns the membership events for a user in specific events.
func (s *stateSnapshotStatements) BulkSelectMembershipForHistoryVisibility(
	ctx context.Context, userNID types.EventStateKeyNID, roomInfo *types.RoomInfo, eventIDs ...string,
) (map[string]*types.HeaderedEvent, error) {
	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	rows, err := db.Raw(s.bulkSelectMembershipForHistoryVisibilitySQL, userNID, pq.Array(eventIDs), roomInfo.RoomNID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectMembershipForHistoryVisibility: rows.close() failed")

	result := make(map[string]*types.HeaderedEvent, len(eventIDs))
	var evJson []byte
	var eventID string
	var membershipEventID string

	knownEvents := make(map[string]*types.HeaderedEvent, len(eventIDs))
	verImpl, err := gomatrixserverlib.GetRoomVersion(roomInfo.RoomVersion)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		if err = rows.Scan(&eventID, &membershipEventID, &evJson); err != nil {
			return nil, err
		}
		if len(evJson) == 0 {
			result[eventID] = &types.HeaderedEvent{}
			continue
		}
		// If we already know this event, don't try to marshal the json again
		if ev, ok := knownEvents[membershipEventID]; ok {
			result[eventID] = ev
			continue
		}
		event, err := verImpl.NewEventFromTrustedJSON(evJson, false)
		if err != nil {
			result[eventID] = &types.HeaderedEvent{}
			// not fatal
			continue
		}
		he := &types.HeaderedEvent{PDU: event}
		result[eventID] = he
		knownEvents[membershipEventID] = he
	}
	return result, rows.Err()
}
