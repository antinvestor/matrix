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
	"fmt"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/lib/pq"
	"github.com/pitabwire/util"
)

const stateDataSchema = `
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

// SQL query constants for state block operations
const (
	// insertStateDataSQL inserts or updates a state block
	insertStateDataSQL = "" +
		"INSERT INTO roomserver_state_block (state_block_hash, event_nids)" +
		" VALUES ($1, $2)" +
		" ON CONFLICT (state_block_hash) DO UPDATE SET event_nids=$2" +
		" RETURNING state_block_nid"

	// bulkSelectStateBlockEntriesSQL selects state block entries for a list of state block NIDs
	bulkSelectStateBlockEntriesSQL = "" +
		"SELECT state_block_nid, event_nids" +
		" FROM roomserver_state_block WHERE state_block_nid = ANY($1) ORDER BY state_block_nid ASC"
)

// stateBlockTable implements tables.StateBlock using PostgreSQL
type stateBlockTable struct {
	cm *sqlutil.Connections
}

// NewPostgresStateBlockTable creates a new PostgreSQL state block table and prepares all statements
func NewPostgresStateBlockTable(ctx context.Context, cm *sqlutil.Connections) (tables.StateBlock, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(stateDataSchema).Error; err != nil {
		return nil, err
	}
	
	// Initialize the table
	s := &stateBlockTable{
		cm: cm,
	}
	
	return s, nil
}

func (s *stateBlockTable) BulkInsertStateData(
	ctx context.Context, txn *sql.Tx,
	entries types.StateEntries,
) (id types.StateBlockNID, err error) {
	// Sort and deduplicate the entries
	entries = entries[:util.SortAndUnique(entries)]
	nids := make(types.EventNIDs, entries.Len())
	for i := range entries {
		nids[i] = entries[i].EventNID
	}
	
	// Get database connection
	db := s.cm.Connection(ctx, false)
	if txn != nil {
		db = db.WithContext(ctx)
	}
	
	// Execute query
	err = db.Raw(
		insertStateDataSQL,
		nids.Hash(),
		eventNIDsAsArray(nids),
	).Scan(&id).Error
	
	return
}

func (s *stateBlockTable) BulkSelectStateBlockEntries(
	ctx context.Context, txn *sql.Tx,
	stateBlockNIDs types.StateBlockNIDs,
) ([][]types.EventNID, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	if txn != nil {
		db = db.WithContext(ctx)
	}
	
	// Execute query
	rows, err := db.Raw(
		bulkSelectStateBlockEntriesSQL,
		stateBlockNIDsAsArray(stateBlockNIDs),
	).Rows()
	
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectStateBlockEntries: rows.close() failed")

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

func stateBlockNIDsAsArray(stateBlockNIDs []types.StateBlockNID) pq.Int64Array {
	nids := make([]int64, len(stateBlockNIDs))
	for i := range stateBlockNIDs {
		nids[i] = int64(stateBlockNIDs[i])
	}
	return nids
}
