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
	"encoding/json"
	"errors"
	"github.com/pitabwire/frame"

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
	-- The localpart of the Matrix user ID associated to this filter
	localpart TEXT NOT NULL,

	PRIMARY KEY(id, localpart)
);

CREATE INDEX IF NOT EXISTS syncapi_filter_localpart ON syncapi_filter(localpart);
`

const selectFilterSQL = "" +
	"SELECT filter FROM syncapi_filter WHERE localpart = $1 AND id = $2"

const selectFilterIDByContentSQL = "" +
	"SELECT id FROM syncapi_filter WHERE localpart = $1 AND filter = $2"

const insertFilterSQL = "" +
	"INSERT INTO syncapi_filter (filter, id, localpart) VALUES ($1, DEFAULT, $2) RETURNING id"

type filterTable struct {
	cm                         *sqlutil.Connections
	selectFilterSQL            string
	selectFilterIDByContentSQL string
	insertFilterSQL            string
}

func NewPostgresFilterTable(ctx context.Context, cm *sqlutil.Connections) (tables.Filter, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(filterSchema).Error; err != nil {
		return nil, err
	}
	
	// Initialize the table with SQL statements
	s := &filterTable{
		cm:                         cm,
		selectFilterSQL:            selectFilterSQL,
		selectFilterIDByContentSQL: selectFilterIDByContentSQL,
		insertFilterSQL:            insertFilterSQL,
	}
	
	return s, nil
}

func (s *filterTable) SelectFilter(
	ctx context.Context, target *synctypes.Filter, localpart string, filterID string,
) error {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	// Retrieve filter from database (stored as canonical JSON)
	var filterData []byte
	err := db.Raw(s.selectFilterSQL, localpart, filterID).Scan(&filterData).Error
	if err != nil {
		return err
	}

	// Unmarshal JSON into Filter struct
	if err = json.Unmarshal(filterData, &target); err != nil {
		return err
	}
	return nil
}

func (s *filterTable) InsertFilter(
	ctx context.Context, filter *synctypes.Filter, localpart string,
) (filterID string, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
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

	// Check if filter already exists in the database using its localpart and content
	//
	// This can result in a race condition when two clients try to insert the
	// same filter and localpart at the same time, however this is not a
	// problem as both calls will result in the same filterID
	err = db.Raw(s.selectFilterIDByContentSQL, localpart, filterJSON).Scan(&existingFilterID).Error
	if err != nil && !frame.DBErrorIsRecordNotFound(err) {
		return "", err
	}
	// If it does, return the existing ID
	if existingFilterID != "" {
		return existingFilterID, nil
	}

	// Otherwise insert the filter and return the new ID
	err = db.Raw(s.insertFilterSQL, filterJSON, localpart).Scan(&filterID).Error
	return
}
