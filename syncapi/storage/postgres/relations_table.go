// Copyright 2022 The Matrix.org Foundation C.I.C.
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

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
)

const relationsSchema = `
CREATE SEQUENCE IF NOT EXISTS syncapi_relation_id;

CREATE TABLE IF NOT EXISTS syncapi_relations (
	id BIGINT PRIMARY KEY DEFAULT nextval('syncapi_relation_id'),
	room_id TEXT NOT NULL,
	event_id TEXT NOT NULL,
	child_event_id TEXT NOT NULL,
	child_event_type TEXT NOT NULL,
	rel_type TEXT NOT NULL,
	CONSTRAINT syncapi_relations_unique UNIQUE (room_id, event_id, child_event_id, rel_type)
);
`

const insertRelationSQL = "" +
	"INSERT INTO syncapi_relations (" +
	"  room_id, event_id, child_event_id, child_event_type, rel_type" +
	") VALUES ($1, $2, $3, $4, $5) " +
	" ON CONFLICT DO NOTHING"

const deleteRelationSQL = "" +
	"DELETE FROM syncapi_relations WHERE room_id = $1 AND child_event_id = $2"

const selectRelationsInRangeAscSQL = "" +
	"SELECT id, child_event_id, rel_type FROM syncapi_relations" +
	" WHERE room_id = $1 AND event_id = $2" +
	" AND ( $3 = '' OR rel_type = $3 )" +
	" AND ( $4 = '' OR child_event_type = $4 )" +
	" AND id > $5 AND id <= $6" +
	" ORDER BY id ASC LIMIT $7"

const selectRelationsInRangeDescSQL = "" +
	"SELECT id, child_event_id, rel_type FROM syncapi_relations" +
	" WHERE room_id = $1 AND event_id = $2" +
	" AND ( $3 = '' OR rel_type = $3 )" +
	" AND ( $4 = '' OR child_event_type = $4 )" +
	" AND id >= $5 AND id < $6" +
	" ORDER BY id DESC LIMIT $7"

const selectMaxRelationIDSQL = "" +
	"SELECT COALESCE(MAX(id), 0) FROM syncapi_relations"

type relationsTable struct {
	cm                            *sqlutil.Connections
	insertRelationSQL             string
	selectRelationsInRangeAscSQL  string
	selectRelationsInRangeDescSQL string
	deleteRelationSQL             string
	selectMaxRelationIDSQL        string
}

func NewPostgresRelationsTable(ctx context.Context, cm *sqlutil.Connections) (tables.Relations, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(relationsSchema).Error; err != nil {
		return nil, err
	}
	
	// Initialize the table with SQL statements
	s := &relationsTable{
		cm:                            cm,
		insertRelationSQL:             insertRelationSQL,
		selectRelationsInRangeAscSQL:  selectRelationsInRangeAscSQL,
		selectRelationsInRangeDescSQL: selectRelationsInRangeDescSQL,
		deleteRelationSQL:             deleteRelationSQL,
		selectMaxRelationIDSQL:        selectMaxRelationIDSQL,
	}
	return s, nil
}

func (s *relationsTable) InsertRelation(
	ctx context.Context, roomID, eventID, childEventID, childEventType, relType string,
) (err error) {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	err = db.Exec(s.insertRelationSQL, roomID, eventID, childEventID, childEventType, relType).Error
	return
}

func (s *relationsTable) DeleteRelation(
	ctx context.Context, roomID, childEventID string,
) error {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	return db.Exec(s.deleteRelationSQL, roomID, childEventID).Error
}

// SelectRelationsInRange returns a map rel_type -> []child_event_id
func (s *relationsTable) SelectRelationsInRange(
	ctx context.Context, roomID, eventID, relType, eventType string,
	r types.Range, limit int,
) (map[string][]types.RelationEntry, types.StreamPosition, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	var lastPos types.StreamPosition
	var query string
	if r.Backwards {
		query = s.selectRelationsInRangeDescSQL
	} else {
		query = s.selectRelationsInRangeAscSQL
	}
	
	rows, err := db.Raw(query, roomID, eventID, relType, eventType, r.Low(), r.High(), limit).Rows()
	if err != nil {
		return nil, lastPos, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectRelationsInRange: rows.close() failed")
	
	result := map[string][]types.RelationEntry{}
	var (
		id           types.StreamPosition
		childEventID string
		relationType string
	)
	for rows.Next() {
		if err = rows.Scan(&id, &childEventID, &relationType); err != nil {
			return nil, lastPos, err
		}
		if id > lastPos {
			lastPos = id
		}
		result[relationType] = append(result[relationType], types.RelationEntry{
			Position: id,
			EventID:  childEventID,
		})
	}
	if lastPos == 0 {
		lastPos = r.To
	}
	return result, lastPos, rows.Err()
}

func (s *relationsTable) SelectMaxRelationID(
	ctx context.Context,
) (id int64, err error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	err = db.Raw(s.selectMaxRelationIDSQL).Scan(&id).Error
	return
}
