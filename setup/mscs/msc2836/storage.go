package msc2836

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"

	"github.com/antinvestor/matrix/internal"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/pitabwire/frame"
)

// Schema for edges and nodes tables
const msc2836Schema = `
CREATE TABLE IF NOT EXISTS msc2836_edges (
	parent_event_id TEXT NOT NULL,
	child_event_id TEXT NOT NULL,
	rel_type TEXT NOT NULL,
	parent_room_id TEXT NOT NULL,
	parent_servers TEXT NOT NULL,
	CONSTRAINT msc2836_edges_uniq UNIQUE (parent_event_id, child_event_id, rel_type)
);

CREATE TABLE IF NOT EXISTS msc2836_nodes (
	event_id TEXT PRIMARY KEY NOT NULL,
	origin_server_ts BIGINT NOT NULL,
	room_id TEXT NOT NULL,
	unsigned_children_count BIGINT NOT NULL,
	unsigned_children_hash TEXT NOT NULL,
	explored SMALLINT NOT NULL
);
`

// Schema revert for edges and nodes tables
const msc2836SchemaRevert = `
DROP TABLE IF EXISTS msc2836_nodes;
DROP TABLE IF EXISTS msc2836_edges;
`

// SQL for inserting an edge into the msc2836_edges table
const insertEdgeSQL = `
	INSERT INTO msc2836_edges(parent_event_id, child_event_id, rel_type, parent_room_id, parent_servers)
	VALUES($1, $2, $3, $4, $5)
	ON CONFLICT DO NOTHING
`

// SQL for inserting a node into the msc2836_nodes table
const insertNodeSQL = `
	INSERT INTO msc2836_nodes(event_id, origin_server_ts, room_id, unsigned_children_count, unsigned_children_hash, explored)
	VALUES($1, $2, $3, $4, $5, $6)
	ON CONFLICT DO NOTHING
`

// SQL for selecting children for a parent, sorted by origin_server_ts
const selectChildrenForParentOldestFirstSQL = `
	SELECT child_event_id, origin_server_ts, room_id FROM msc2836_edges
	LEFT JOIN msc2836_nodes ON msc2836_edges.child_event_id = msc2836_nodes.event_id
	WHERE parent_event_id = $1 AND rel_type = $2
	ORDER BY origin_server_ts ASC
`

// SQL for selecting children for a parent, sorted by origin_server_ts in descending order
const selectChildrenForParentRecentFirstSQL = `
	SELECT child_event_id, origin_server_ts, room_id FROM msc2836_edges
	LEFT JOIN msc2836_nodes ON msc2836_edges.child_event_id = msc2836_nodes.event_id
	WHERE parent_event_id = $1 AND rel_type = $2
	ORDER BY origin_server_ts DESC
`

// SQL for selecting a parent for a child
const selectParentForChildSQL = `
	SELECT parent_event_id, parent_room_id FROM msc2836_edges
	WHERE child_event_id = $1 AND rel_type = $2
`

// SQL for updating child metadata
const updateChildMetadataSQL = `
	UPDATE msc2836_nodes SET unsigned_children_count=$1, unsigned_children_hash=$2, explored=$3 WHERE event_id=$4
`

// SQL for selecting child metadata
const selectChildMetadataSQL = `
	SELECT unsigned_children_count, unsigned_children_hash, explored FROM msc2836_nodes WHERE event_id=$1
`

// SQL for updating explored flag for child metadata
const updateChildMetadataExploredSQL = `
	UPDATE msc2836_nodes SET explored=$1 WHERE event_id=$2
`

type eventInfo struct {
	EventID        string
	OriginServerTS spec.Timestamp
	RoomID         string
}

type Database interface {
	// StoreRelation stores the parent->child and child->parent relationship for later querying.
	// Also stores the event metadata e.g timestamp
	StoreRelation(ctx context.Context, ev *types.HeaderedEvent) error
	// ChildrenForParent returns the events who have the given `eventID` as an m.relationship with the
	// provided `relType`. The returned slice is sorted by origin_server_ts according to whether
	// `recentFirst` is true or false.
	ChildrenForParent(ctx context.Context, eventID, relType string, recentFirst bool) ([]eventInfo, error)
	// ParentForChild returns the parent event for the given child `eventID`. The eventInfo should be nil if
	// there is no parent for this child event, with no error. The parent eventInfo can be missing the
	// timestamp if the event is not known to the server.
	ParentForChild(ctx context.Context, eventID, relType string) (*eventInfo, error)
	// UpdateChildMetadata persists the children_count and children_hash from this event if and only if
	// the count is greater than what was previously there. If the count is updated, the event will be
	// updated to be unexplored.
	UpdateChildMetadata(ctx context.Context, ev *types.HeaderedEvent) error
	// ChildMetadata returns the children_count and children_hash for the event ID in question.
	// Also returns the `explored` flag, which is set to true when MarkChildrenExplored is called and is set
	// back to `false` when a larger count is inserted via UpdateChildMetadata.
	// Returns nil error if the event ID does not exist.
	ChildMetadata(ctx context.Context, eventID string) (count int, hash []byte, explored bool, err error)
	// MarkChildrenExplored sets the 'explored' flag on this event to `true`.
	MarkChildrenExplored(ctx context.Context, eventID string) error
}

// postgresDB implements the Database interface using Postgres
type postgresDB struct {
	cm sqlutil.ConnectionManager
	// SQL query string fields, initialise at construction
	insertEdgeSQL                         string
	insertNodeSQL                         string
	selectChildrenForParentOldestFirstSQL string
	selectChildrenForParentRecentFirstSQL string
	selectParentForChildSQL               string
	updateChildMetadataSQL                string
	selectChildMetadataSQL                string
	updateChildMetadataExploredSQL        string
}

// NewDatabase loads the database for msc2836
func NewDatabase(ctx context.Context, cm sqlutil.ConnectionManager) (Database, error) {
	db, err := newPostgresDatabase(ctx, cm)
	if err != nil {
		return nil, err
	}

	err = cm.Migrate(ctx)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func newPostgresDatabase(_ context.Context, cm sqlutil.ConnectionManager) (Database, error) {
	// Create migration patch
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "msc2836_tables_schema_001",
		Patch:       msc2836Schema,
		RevertPatch: msc2836SchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Create table implementation with SQL queries
	p := &postgresDB{
		cm:                                    cm,
		insertEdgeSQL:                         insertEdgeSQL,
		insertNodeSQL:                         insertNodeSQL,
		selectChildrenForParentOldestFirstSQL: selectChildrenForParentOldestFirstSQL,
		selectChildrenForParentRecentFirstSQL: selectChildrenForParentRecentFirstSQL,
		selectParentForChildSQL:               selectParentForChildSQL,
		updateChildMetadataSQL:                updateChildMetadataSQL,
		selectChildMetadataSQL:                selectChildMetadataSQL,
		updateChildMetadataExploredSQL:        updateChildMetadataExploredSQL,
	}

	return p, nil
}

func (p *postgresDB) StoreRelation(ctx context.Context, ev *types.HeaderedEvent) error {
	parent, child, relType := parentChildEventIDs(ev)
	if parent == "" || child == "" {
		return nil
	}
	relationRoomID, relationServers := roomIDAndServers(ev)
	relationServersJSON, err := json.Marshal(relationServers)
	if err != nil {
		return err
	}
	count, hash := extractChildMetadata(ev)

	return p.cm.Do(ctx, func(ctx context.Context) error {

		db := p.cm.Connection(ctx, false)

		// Insert edge
		result := db.Exec(p.insertEdgeSQL, parent, child, relType, relationRoomID, string(relationServersJSON))
		if result.Error != nil {
			return result.Error
		}

		frame.Log(ctx).
			WithField("child", child).
			WithField("parent", parent).
			WithField("rel_type", relType).Info("StoreRelation")

		// Insert node
		result = db.Exec(p.insertNodeSQL, ev.EventID(), ev.OriginServerTS(), ev.RoomID().String(), count, base64.RawStdEncoding.EncodeToString(hash), 0)
		if result.Error != nil {
			return result.Error
		}
		return nil
	})
}

func (p *postgresDB) UpdateChildMetadata(ctx context.Context, ev *types.HeaderedEvent) error {
	eventCount, eventHash := extractChildMetadata(ev)
	if eventCount == 0 {
		return nil // nothing to update with
	}

	// extract current children count/hash, if they are less than the current event then update the columns and set to unexplored
	count, hash, _, err := p.ChildMetadata(ctx, ev.EventID())
	if err != nil {
		return err
	}
	if eventCount > count || (eventCount == count && !bytes.Equal(hash, eventHash)) {
		db := p.cm.Connection(ctx, false)
		result := db.Exec(p.updateChildMetadataSQL, eventCount, base64.RawStdEncoding.EncodeToString(eventHash), 0, ev.EventID())
		return result.Error
	}
	return nil
}

func (p *postgresDB) ChildMetadata(ctx context.Context, eventID string) (count int, hash []byte, explored bool, err error) {
	var b64hash string
	var exploredInt int

	db := p.cm.Connection(ctx, true)
	row := db.Raw(p.selectChildMetadataSQL, eventID).Row()
	err = row.Scan(&count, &b64hash, &exploredInt)
	if err != nil {
		if sqlutil.ErrorIsNoRows(err) {
			err = nil
		}
		return
	}
	hash, err = base64.RawStdEncoding.DecodeString(b64hash)
	explored = exploredInt > 0
	return
}

func (p *postgresDB) MarkChildrenExplored(ctx context.Context, eventID string) error {
	db := p.cm.Connection(ctx, false)
	result := db.Exec(p.updateChildMetadataExploredSQL, 1, eventID)
	return result.Error
}

func (p *postgresDB) ChildrenForParent(ctx context.Context, eventID, relType string, recentFirst bool) ([]eventInfo, error) {
	db := p.cm.Connection(ctx, true)

	var query string
	if recentFirst {
		query = p.selectChildrenForParentRecentFirstSQL
	} else {
		query = p.selectChildrenForParentOldestFirstSQL
	}

	rows, err := db.Raw(query, eventID, relType).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "ChildrenForParent: rows.close() failed")

	var children []eventInfo
	for rows.Next() {
		var evInfo eventInfo
		if err := rows.Scan(&evInfo.EventID, &evInfo.OriginServerTS, &evInfo.RoomID); err != nil {
			return nil, err
		}
		children = append(children, evInfo)
	}
	return children, rows.Err()
}

func (p *postgresDB) ParentForChild(ctx context.Context, eventID, relType string) (*eventInfo, error) {
	var ei eventInfo

	db := p.cm.Connection(ctx, true)
	row := db.Raw(p.selectParentForChildSQL, eventID, relType).Row()
	err := row.Scan(&ei.EventID, &ei.RoomID)
	if sqlutil.ErrorIsNoRows(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &ei, nil
}

func parentChildEventIDs(ev *types.HeaderedEvent) (parent, child, relType string) {
	if ev == nil {
		return
	}
	body := struct {
		Relationship struct {
			RelType string `json:"rel_type"`
			EventID string `json:"event_id"`
		} `json:"m.relationship"`
	}{}
	if err := json.Unmarshal(ev.Content(), &body); err != nil {
		return
	}
	if body.Relationship.EventID == "" || body.Relationship.RelType == "" {
		return
	}
	return body.Relationship.EventID, ev.EventID(), body.Relationship.RelType
}

func roomIDAndServers(ev *types.HeaderedEvent) (roomID string, servers []string) {
	servers = []string{}
	if ev == nil {
		return
	}
	body := struct {
		RoomID  string   `json:"relationship_room_id"`
		Servers []string `json:"relationship_servers"`
	}{}
	if err := json.Unmarshal(ev.Unsigned(), &body); err != nil {
		return
	}
	return body.RoomID, body.Servers
}

func extractChildMetadata(ev *types.HeaderedEvent) (count int, hash []byte) {
	unsigned := struct {
		Counts map[string]int   `json:"children"`
		Hash   spec.Base64Bytes `json:"children_hash"`
	}{}
	if err := json.Unmarshal(ev.Unsigned(), &unsigned); err != nil {
		// expected if there is no unsigned field at all
		return
	}
	for _, c := range unsigned.Counts {
		count += c
	}
	hash = unsigned.Hash
	return
}
