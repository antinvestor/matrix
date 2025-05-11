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
	"github.com/pitabwire/frame"
)

// Schema for relations table
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

// Revert schema for relations table
const relationsSchemaRevert = `
DROP TABLE IF EXISTS syncapi_relations;
DROP SEQUENCE IF EXISTS syncapi_relation_id;
`

// SQL query to insert a relation
const insertRelationSQL = `
INSERT INTO syncapi_relations (
  room_id, event_id, child_event_id, child_event_type, rel_type
) VALUES ($1, $2, $3, $4, $5) 
 ON CONFLICT DO NOTHING
`

// SQL query to delete a relation
const deleteRelationSQL = `
DELETE FROM syncapi_relations WHERE room_id = $1 AND child_event_id = $2
`

// SQL query to select relations in ascending order
const selectRelationsInRangeAscSQL = `
SELECT id, child_event_id, rel_type FROM syncapi_relations
 WHERE room_id = $1 AND event_id = $2
 AND ( $3 = '' OR rel_type = $3 )
 AND ( $4 = '' OR child_event_type = $4 )
 AND id > $5 AND id <= $6
 ORDER BY id ASC LIMIT $7
`

// SQL query to select relations in descending order
const selectRelationsInRangeDescSQL = `
SELECT id, child_event_id, rel_type FROM syncapi_relations
 WHERE room_id = $1 AND event_id = $2
 AND ( $3 = '' OR rel_type = $3 )
 AND ( $4 = '' OR child_event_type = $4 )
 AND id >= $5 AND id < $6
 ORDER BY id DESC LIMIT $7
`

// SQL query to select max relation ID
const selectMaxRelationIDSQL = `
SELECT COALESCE(MAX(id), 0) FROM syncapi_relations
`

// relationsTable implements tables.Relations
type relationsTable struct {
	cm                            sqlutil.ConnectionManager
	insertRelationSQL             string
	selectRelationsInRangeAscSQL  string
	selectRelationsInRangeDescSQL string
	deleteRelationSQL             string
	selectMaxRelationIDSQL        string
}

// NewPostgresRelationsTable creates a new relations table
func NewPostgresRelationsTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.Relations, error) {
	t := &relationsTable{
		cm:                            cm,
		insertRelationSQL:             insertRelationSQL,
		selectRelationsInRangeAscSQL:  selectRelationsInRangeAscSQL,
		selectRelationsInRangeDescSQL: selectRelationsInRangeDescSQL,
		deleteRelationSQL:             deleteRelationSQL,
		selectMaxRelationIDSQL:        selectMaxRelationIDSQL,
	}

	// Perform the migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "syncapi_relations_table_schema_001",
		Patch:       relationsSchema,
		RevertPatch: relationsSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// InsertRelation adds a new relation
func (t *relationsTable) InsertRelation(
	ctx context.Context, roomID, eventID, childEventID, childEventType, relType string,
) (err error) {
	db := t.cm.Connection(ctx, false)
	err = db.Exec(t.insertRelationSQL, roomID, eventID, childEventID, childEventType, relType).Error
	return
}

// DeleteRelation removes a relation
func (t *relationsTable) DeleteRelation(
	ctx context.Context, roomID, childEventID string,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteRelationSQL, roomID, childEventID).Error
}

// SelectRelationsInRange returns a map rel_type -> []child_event_id
func (t *relationsTable) SelectRelationsInRange(
	ctx context.Context, roomID, eventID, relType, eventType string,
	r types.Range, limit int,
) (map[string][]types.RelationEntry, types.StreamPosition, error) {
	var lastPos types.StreamPosition
	var sql string

	if r.Backwards {
		sql = t.selectRelationsInRangeDescSQL
	} else {
		sql = t.selectRelationsInRangeAscSQL
	}

	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(sql, roomID, eventID, relType, eventType, r.Low(), r.High(), limit).Rows()
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

// SelectMaxRelationID returns the maximum relation ID
func (t *relationsTable) SelectMaxRelationID(ctx context.Context) (id int64, err error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectMaxRelationIDSQL).Row()
	err = row.Scan(&id)
	return
}
