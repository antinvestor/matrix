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
	"database/sql"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
)

// Schema defines the table schema for creating the account data tables
const accountDataSchema = `
-- This sequence is shared between all the tables generated from kafka logs.
CREATE SEQUENCE IF NOT EXISTS syncapi_stream_id;

-- Stores the types of account data that a user set has globally and in each room
-- and the stream ID when that type was last updated.
CREATE TABLE IF NOT EXISTS syncapi_account_data_type (
    -- An incrementing ID which denotes the position in the log that this event resides at.
    id BIGINT PRIMARY KEY DEFAULT nextval('syncapi_stream_id'),
    -- ID of the user the data belongs to
    user_id TEXT NOT NULL,
    -- ID of the room the data is related to (empty string if not related to a specific room)
    room_id TEXT NOT NULL,
    -- Type of the data
    type TEXT NOT NULL,

    -- We don't want two entries of the same type for the same user
    CONSTRAINT syncapi_account_data_unique UNIQUE (user_id, room_id, type)
);

CREATE UNIQUE INDEX IF NOT EXISTS syncapi_account_data_id_idx ON syncapi_account_data_type(id, type);
`

// Revert schema to be used for migration rollback
const accountDataSchemaRevert = `
DROP INDEX IF EXISTS syncapi_account_data_id_idx;
DROP TABLE IF EXISTS syncapi_account_data_type;
-- Don't drop the sequence as it might be used by other tables
`

// SQL query to insert account data
const insertAccountDataSQL = `
INSERT INTO syncapi_account_data_type (user_id, room_id, type) VALUES ($1, $2, $3)
ON CONFLICT ON CONSTRAINT syncapi_account_data_unique
DO UPDATE SET id = nextval('syncapi_stream_id')
RETURNING id
`

// SQL query to select account data in a range
const selectAccountDataInRangeSQL = `
SELECT id, room_id, type FROM syncapi_account_data_type
WHERE user_id = $1 AND id > $2 AND id <= $3
AND ( $4::text[] IS NULL OR     type LIKE ANY($4)  )
AND ( $5::text[] IS NULL OR NOT(type LIKE ANY($5)) )
ORDER BY id ASC LIMIT $6
`

// SQL query to get max account data ID
const selectMaxAccountDataIDSQL = `
SELECT MAX(id) FROM syncapi_account_data_type
`

// accountDataTable represents a table for storing account data
type accountDataTable struct {
	cm                          sqlutil.ConnectionManager
	insertAccountDataSQL        string
	selectAccountDataInRangeSQL string
	selectMaxAccountDataIDSQL   string
}

// NewPostgresAccountDataTable creates a new postgres account data table
func NewPostgresAccountDataTable(_ context.Context, cm sqlutil.ConnectionManager) (tables.AccountData, error) {
	// Perform the migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "syncapi_account_data_table_schema_001",
		Patch:       accountDataSchema,
		RevertPatch: accountDataSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	t := &accountDataTable{
		cm:                          cm,
		insertAccountDataSQL:        insertAccountDataSQL,
		selectAccountDataInRangeSQL: selectAccountDataInRangeSQL,
		selectMaxAccountDataIDSQL:   selectMaxAccountDataIDSQL,
	}

	return t, nil
}

func (t *accountDataTable) InsertAccountData(
	ctx context.Context, userID, roomID, dataType string,
) (pos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, false)

	row := db.Raw(t.insertAccountDataSQL, userID, roomID, dataType).Row()
	err = row.Scan(&pos)
	return
}

func (t *accountDataTable) SelectAccountDataInRange(
	ctx context.Context, userID string, r types.Range,
	accountDataEventFilter *synctypes.EventFilter,
) (data map[string][]string, pos types.StreamPosition, err error) {
	data = make(map[string][]string)
	db := t.cm.Connection(ctx, true)

	rows, err := db.Raw(
		t.selectAccountDataInRangeSQL,
		userID, r.Low(), r.High(),
		pq.StringArray(filterConvertTypeWildcardToSQL(accountDataEventFilter.Types)),
		pq.StringArray(filterConvertTypeWildcardToSQL(accountDataEventFilter.NotTypes)),
		accountDataEventFilter.Limit,
	).Rows()
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectAccountDataInRange: rows.close() failed")

	var dataType string
	var roomID string
	var id types.StreamPosition

	for rows.Next() {
		if err = rows.Scan(&id, &roomID, &dataType); err != nil {
			return
		}

		if len(data[roomID]) > 0 {
			data[roomID] = append(data[roomID], dataType)
		} else {
			data[roomID] = []string{dataType}
		}
		if id > pos {
			pos = id
		}
	}
	if pos == 0 {
		pos = r.High()
	}
	return data, pos, rows.Err()
}

func (t *accountDataTable) SelectMaxAccountDataID(
	ctx context.Context,
) (id int64, err error) {
	var nullableID sql.NullInt64
	db := t.cm.Connection(ctx, true)

	row := db.Raw(t.selectMaxAccountDataIDSQL).Row()
	err = row.Scan(&nullableID)
	if nullableID.Valid {
		id = nullableID.Int64
	}
	return
}
