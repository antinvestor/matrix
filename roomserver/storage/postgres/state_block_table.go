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
	"fmt"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/util"
)

// SQL schema for the state block table
const stateBlockSchema = `
-- The state data map.
-- Designed to give enough information to run the state resolution algorithm
-- without hitting the database in the internal case.
-- TODO: Is it worth replacing the unique btree index with a covering index so
-- that postgres could lookup the state using an index-only scan?
-- The type and state_key are included in the index to make it easier to
-- lookup a specific (type, state_key) pair for an event. It also makes it easy
-- to read the state for a given state_block_nid ordered by (type, state_key)
-- which in turn makes it easier to merge state data blocks.
CREATE SEQUENCE IF NOT EXISTS roomserver_state_block_nid_seq;
CREATE TABLE IF NOT EXISTS roomserver_state_block (
	-- The state snapshot NID that identifies this snapshot.
	state_block_nid bigint PRIMARY KEY DEFAULT nextval('roomserver_state_block_nid_seq'),
	-- The hash of the state block, which is used to enforce uniqueness. The hash is
	-- generated in Dendrite and passed through to the database, as a btree index over 
	-- this column is cheap and fits within the maximum index size.
	state_block_hash BYTEA UNIQUE,
	-- The event NIDs contained within the state block.
	event_nids bigint[] NOT NULL
);
`

// SQL query to revert the state block table schema
const stateBlockSchemaRevert = `
DROP TABLE IF EXISTS roomserver_state_block;
DROP SEQUENCE IF EXISTS roomserver_state_block_nid_seq;
`

// SQL queries for the state block table

// Insert a new state block. If we conflict on the hash column then
// we must perform an update so that the RETURNING statement returns the
// ID of the row that we conflicted with, so that we can then refer to
// the original block.
const insertStateDataSQL = "" +
	"INSERT INTO roomserver_state_block (state_block_hash, event_nids)" +
	" VALUES ($1, $2)" +
	" ON CONFLICT (state_block_hash) DO UPDATE SET event_nids=$2" +
	" RETURNING state_block_nid"

// Bulk select state block entries by their state block NIDs.
// Sorting by state_block_nid means we can use binary search over the result
// to lookup the event NIDs for a state block NID.
const bulkSelectStateBlockEntriesSQL = "" +
	"SELECT state_block_nid, event_nids" +
	" FROM roomserver_state_block" +
	" WHERE state_block_nid = ANY($1)" +
	" ORDER BY state_block_nid ASC"

// stateBlockStatements holds prepared SQL statements for the state block table.
type stateBlockStatements struct {
	cm sqlutil.ConnectionManager

	// SQL query string fields
	insertStateDataSQL             string
	bulkSelectStateBlockEntriesSQL string
}

// NewPostgresStateBlockTable creates a new instance of the state block table.
// If the table does not exist, it will be created.
func NewPostgresStateBlockTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.StateBlock, error) {
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "roomserver_state_block_schema_001",
		Patch:       stateBlockSchema,
		RevertPatch: stateBlockSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	s := &stateBlockStatements{
		cm: cm,

		insertStateDataSQL:             insertStateDataSQL,
		bulkSelectStateBlockEntriesSQL: bulkSelectStateBlockEntriesSQL,
	}

	return s, nil
}

// BulkInsertStateData inserts a block of state data. Returns a state block ID.
func (s *stateBlockStatements) BulkInsertStateData(
	ctx context.Context,
	entries types.StateEntries,
) (id types.StateBlockNID, err error) {
	// Sort and deduplicate the entries
	entries = entries[:util.SortAndUnique(entries)]

	// Convert state entries to event NIDs
	nids := make(types.EventNIDs, entries.Len())
	for i := range entries {
		nids[i] = entries[i].EventNID
	}

	// Get a connection
	db := s.cm.Connection(ctx, false)

	// Insert the state data
	err = db.Raw(s.insertStateDataSQL, nids.Hash(), pq.Array(nids)).Row().Scan(&id)
	return
}

// BulkSelectStateBlockEntries retrieves multiple state block entries by their state block NIDs.
func (s *stateBlockStatements) BulkSelectStateBlockEntries(
	ctx context.Context, stateBlockNIDs types.StateBlockNIDs,
) ([][]types.EventNID, error) {
	// Convert state block NIDs to int64 array
	nids := make([]int64, len(stateBlockNIDs))
	for i := range stateBlockNIDs {
		nids[i] = int64(stateBlockNIDs[i])
	}

	// Get a connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	rows, err := db.Raw(s.bulkSelectStateBlockEntriesSQL, pq.Array(nids)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectStateBlockEntries: rows.close() failed")

	// Process the results
	results := make([][]types.EventNID, len(stateBlockNIDs))
	i := 0
	var stateBlockNID types.StateBlockNID
	var result pq.Int64Array
	for ; rows.Next(); i++ {
		if err = rows.Scan(&stateBlockNID, &result); err != nil {
			return nil, err
		}
		r := make([]types.EventNID, len(result))
		for x := range result {
			r[x] = types.EventNID(result[x])
		}
		results[i] = r
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if i != len(stateBlockNIDs) {
		return nil, fmt.Errorf("storage: state data NIDs missing from the database (%d != %d)", i, len(stateBlockNIDs))
	}
	return results, err
}
