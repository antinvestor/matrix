// Copyright 2017 Jan Christian Grünhage
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

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/pitabwire/frame"
)

// Schema for filter table creation
const filterSchema = `
-- Stores data about filters
CREATE TABLE IF NOT EXISTS syncapi_filter (
	-- The filter
	filter TEXT NOT NULL,
	-- The ID
	id SERIAL UNIQUE,
	-- The localpart of the Matrix user ID associated to this filter
	localpart TEXT NOT NULL,

	PRIMARY KEY(id, localpart)
);

CREATE INDEX IF NOT EXISTS syncapi_filter_localpart ON syncapi_filter(localpart);
`

// Revert schema for filter table
const filterSchemaRevert = `
DROP INDEX IF EXISTS syncapi_filter_localpart;
DROP TABLE IF EXISTS syncapi_filter;
`

// SQL query to select a filter
const selectFilterSQL = `
SELECT filter FROM syncapi_filter WHERE localpart = $1 AND id = $2
`

// SQL query to select a filter ID by content
const selectFilterIDByContentSQL = `
SELECT id FROM syncapi_filter WHERE localpart = $1 AND filter = $2
`

// SQL query to insert a filter
const insertFilterSQL = `
INSERT INTO syncapi_filter (filter, id, localpart) VALUES ($1, DEFAULT, $2) RETURNING id
`

// filterTable implements the tables.Filter interface
type filterTable struct {
	cm                         *sqlutil.Connections
	selectFilterSQL            string
	selectFilterIDByContentSQL string
	insertFilterSQL            string
}

// NewPostgresFilterTable creates a new filter table
func NewPostgresFilterTable(ctx context.Context, cm *sqlutil.Connections) (tables.Filter, error) {
	t := &filterTable{
		cm:                         cm,
		selectFilterSQL:            selectFilterSQL,
		selectFilterIDByContentSQL: selectFilterIDByContentSQL,
		insertFilterSQL:            insertFilterSQL,
	}

	// Perform the migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "syncapi_filter_table_schema_001",
		Patch:       filterSchema,
		RevertPatch: filterSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// SelectFilter fetches a filter by its ID and localpart
func (t *filterTable) SelectFilter(
	ctx context.Context, target *synctypes.Filter, localpart string, filterID string,
) error {
	// Retrieve filter from database (stored as canonical JSON)
	var filterData []byte
	db := t.cm.Connection(ctx, true)

	row := db.Raw(t.selectFilterSQL, localpart, filterID).Row()
	err := row.Scan(&filterData)
	if err != nil {
		return err
	}

	// Unmarshal JSON into Filter struct
	if err = json.Unmarshal(filterData, &target); err != nil {
		return err
	}
	return nil
}

// InsertFilter stores a new filter in the database
func (t *filterTable) InsertFilter(
	ctx context.Context, filter *synctypes.Filter, localpart string,
) (filterID string, err error) {
	var existingFilterID string

	// Serialise json
	filterJSON, err := json.Marshal(filter)
	if err != nil {
		return "", err
	}
	// Remove whitespaces and sort JSON data
	// needed to prevent from inserting the same filter multiple times
	filterJSON, err = gomatrixserverlib.CanonicalJSON(filterJSON)
	if err != nil {
		return "", err
	}

	db := t.cm.Connection(ctx, false)

	// Check if filter already exists in the database using its localpart and content
	//
	// This can result in a race condition when two clients try to insert the
	// same filter and localpart at the same time, however this is not a
	// problem as both calls will result in the same filterID
	row := db.Raw(t.selectFilterIDByContentSQL, localpart, filterJSON).Row()
	err = row.Scan(&existingFilterID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	// If it does, return the existing ID
	if existingFilterID != "" {
		return existingFilterID, nil
	}

	// Otherwise insert the filter and return the new ID
	row = db.Raw(t.insertFilterSQL, filterJSON, localpart).Row()
	err = row.Scan(&filterID)
	return
}
