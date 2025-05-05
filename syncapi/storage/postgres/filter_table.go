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
)

const filterSchema = `
-- Stores data about filters
CREATE TABLE IF NOT EXISTS syncapi_filter (
	-- The filter
	filter TEXT NOT NULL,
	-- The ID
	id SERIAL UNIQUE,
	-- The localpart of the Global user ID associated to this filter
	localpart TEXT NOT NULL,

	PRIMARY KEY(id, localpart)
);

CREATE INDEX IF NOT EXISTS syncapi_filter_localpart ON syncapi_filter(localpart);
`

const filterSchemaRevert = `DROP TABLE IF EXISTS syncapi_filter; DROP INDEX IF EXISTS syncapi_filter_localpart;`

const selectFilterSQL = "" +
	"SELECT filter FROM syncapi_filter WHERE localpart = $1 AND id = $2"

const selectFilterIDByContentSQL = "" +
	"SELECT id FROM syncapi_filter WHERE localpart = $1 AND filter = $2"

const insertFilterSQL = "" +
	"INSERT INTO syncapi_filter (filter, id, localpart) VALUES ($1, DEFAULT, $2) RETURNING id"

// filterTable implements tables.Filter using a connection manager and SQL constants.
// This table stores user filters and provides methods for inserting and retrieving filters by user and content.
type filterTable struct {
	cm                         *sqlutil.Connections
	selectFilterSQL            string
	selectFilterIDByContentSQL string
	insertFilterSQL            string
}

// NewPostgresFilterTable creates a new Filter table using a connection manager.
func NewPostgresFilterTable(cm *sqlutil.Connections) tables.Filter {
	return &filterTable{
		cm:                         cm,
		selectFilterSQL:            selectFilterSQL,
		selectFilterIDByContentSQL: selectFilterIDByContentSQL,
		insertFilterSQL:            insertFilterSQL,
	}
}

// SelectFilter retrieves a filter by localpart and filter ID, and unmarshals it into target.
func (t *filterTable) SelectFilter(
	ctx context.Context, target *synctypes.Filter, localpart string, filterID string,
) error {
	// Retrieve filter from database (stored as canonical JSON)
	db := t.cm.Connection(ctx, true)
	var filterData []byte
	err := db.Raw(t.selectFilterSQL, localpart, filterID).Row().Scan(&filterData)
	if err != nil {
		return err
	}
	// Unmarshal JSON into Filter struct
	if err = json.Unmarshal(filterData, &target); err != nil {
		return err
	}
	return nil
}

// InsertFilter inserts a filter for a user, ensuring deduplication by canonical JSON.
func (t *filterTable) InsertFilter(
	ctx context.Context, filter *synctypes.Filter, localpart string,
) (filterID string, err error) {
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
	// Check if filter already exists in the database using its localpart and content
	//
	// This can result in a race condition when two clients try to insert the
	// same filter and localpart at the same time, however this is not a
	// problem as both calls will result in the same filterID
	db := t.cm.Connection(ctx, false)
	err = db.Raw(t.selectFilterIDByContentSQL, localpart, string(filterJSON)).Row().Scan(&filterID)
	if err == nil {
		return filterID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	// Insert new filter
	err = db.Raw(t.insertFilterSQL, string(filterJSON), localpart).Row().Scan(&filterID)
	return filterID, err
}
