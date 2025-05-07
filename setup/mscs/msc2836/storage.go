package msc2836

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
)

// Schema definitions
const (
	postgresSchema = `
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
)

// SQL query constants
const (
	// Insert edge represents a relationship between parent and child
	insertEdgeSQL = `
		INSERT INTO msc2836_edges(parent_event_id, child_event_id, rel_type, parent_room_id, parent_servers)
		VALUES($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING
	`

	// Insert node with event metadata
	insertNodeSQL = `
		INSERT INTO msc2836_nodes(event_id, origin_server_ts, room_id, unsigned_children_count, unsigned_children_hash, explored)
		VALUES($1, $2, $3, $4, $5, 0)
		ON CONFLICT (event_id) DO UPDATE SET
			origin_server_ts = EXCLUDED.origin_server_ts,
			room_id = EXCLUDED.room_id
	`

	// Select children for a parent, ordered by origin_server_ts ascending
	selectChildrenForParentOldestFirstSQL = `
		SELECT c.event_id, c.origin_server_ts, c.room_id
		FROM msc2836_edges AS r
		JOIN msc2836_nodes AS c ON r.child_event_id = c.event_id
		WHERE r.parent_event_id = $1 AND r.rel_type = $2
		ORDER BY c.origin_server_ts ASC
	`

	// Select children for a parent, ordered by origin_server_ts descending
	selectChildrenForParentRecentFirstSQL = `
		SELECT c.event_id, c.origin_server_ts, c.room_id
		FROM msc2836_edges AS r
		JOIN msc2836_nodes AS c ON r.child_event_id = c.event_id
		WHERE r.parent_event_id = $1 AND r.rel_type = $2
		ORDER BY c.origin_server_ts DESC
	`

	// Select parent for a child
	selectParentForChildSQL = `
		SELECT p.event_id, p.origin_server_ts, p.room_id
		FROM msc2836_edges AS r
		JOIN msc2836_nodes AS p ON r.parent_event_id = p.event_id
		WHERE r.child_event_id = $1 AND r.rel_type = $2
	`

	// Update child metadata
	updateChildMetadataSQL = `
		INSERT INTO msc2836_nodes (event_id, origin_server_ts, room_id, unsigned_children_count, unsigned_children_hash, explored)
		VALUES ($1, $2, $3, $4, $5, 0)
		ON CONFLICT (event_id) DO UPDATE SET
			unsigned_children_count = CASE WHEN msc2836_nodes.unsigned_children_count < EXCLUDED.unsigned_children_count THEN EXCLUDED.unsigned_children_count ELSE msc2836_nodes.unsigned_children_count END,
			unsigned_children_hash = CASE WHEN msc2836_nodes.unsigned_children_count < EXCLUDED.unsigned_children_count THEN EXCLUDED.unsigned_children_hash ELSE msc2836_nodes.unsigned_children_hash END,
			explored = CASE WHEN msc2836_nodes.unsigned_children_count < EXCLUDED.unsigned_children_count THEN 0 ELSE msc2836_nodes.explored END
	`

	// Select child metadata
	selectChildMetadataSQL = `
		SELECT unsigned_children_count, unsigned_children_hash, explored FROM msc2836_nodes WHERE event_id = $1
	`

	// Update child metadata explored status
	updateChildMetadataExploredSQL = `
		UPDATE msc2836_nodes SET explored = 1 WHERE event_id = $1
	`
)

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
	cm *sqlutil.Connections

	// SQL query strings
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
func NewDatabase(ctx context.Context, cm *sqlutil.Connections, dbOpts *config.DatabaseOptions) (Database, error) {
	if !conMan.DS().IsPostgres() {
		return nil, fmt.Errorf("unexpected database type")
	}
	return newPostgresDatabase(ctx, cm)

}

func newPostgresDatabase(ctx context.Context, cm *sqlutil.Connections) (Database, error) {

	// Create tables if they don't exist
	if err := conMan.Connection(ctx, false).Exec(postgresSchema).Error; err != nil {
		return nil, err
	}

	// Initialize the database
	d := &postgresDB{
		cm:                                    conMan,
		insertEdgeSQL:                         insertEdgeSQL,
		insertNodeSQL:                         insertNodeSQL,
		selectChildrenForParentOldestFirstSQL: selectChildrenForParentOldestFirstSQL,
		selectChildrenForParentRecentFirstSQL: selectChildrenForParentRecentFirstSQL,
		selectParentForChildSQL:               selectParentForChildSQL,
		updateChildMetadataSQL:                updateChildMetadataSQL,
		selectChildMetadataSQL:                selectChildMetadataSQL,
		updateChildMetadataExploredSQL:        updateChildMetadataExploredSQL,
	}

	return d, nil
}

// PostgreSQL implementation
func (d *postgresDB) StoreRelation(ctx context.Context, ev *types.HeaderedEvent) error {
	parent, child, relType := parentChildEventIDs(ev)
	if parent == "" || child == "" || relType == "" {
		return nil
	}
	roomID, servers := roomIDAndServers(ev)
	serversJSON, err := json.Marshal(servers)
	if err != nil {
		return err
	}

	db := d.cm.Connection(ctx, false)

	// Store the edge (relationship)
	if err := db.Exec(d.insertEdgeSQL, parent, child, relType, roomID, serversJSON).Error; err != nil {
		return err
	}

	// Store the node (event) with metadata
	originServerTS := ev.OriginServerTS()
	if err := db.Exec(d.insertNodeSQL, child, originServerTS, ev.RoomID().String(), 0, "", 0).Error; err != nil {
		return err
	}

	// Update child metadata if needed
	return d.UpdateChildMetadata(ctx, ev)
}

func (d *postgresDB) UpdateChildMetadata(ctx context.Context, ev *types.HeaderedEvent) error {
	count, hash := extractChildMetadata(ev)
	if count == 0 || hash == nil {
		return nil
	}

	db := d.cm.Connection(ctx, false)
	hashStr := base64.StdEncoding.EncodeToString(hash)
	eventID := ev.EventID()
	return db.Exec(
		d.updateChildMetadataSQL,
		eventID,
		ev.OriginServerTS(),
		ev.RoomID().String(),
		count,
		hashStr,
	).Error
}

func (d *postgresDB) ChildMetadata(ctx context.Context, eventID string) (count int, hash []byte, explored bool, err error) {
	var hashB64 string
	var exploredInt int

	db := d.cm.Connection(ctx, true)
	row := db.Raw(d.selectChildMetadataSQL, eventID).Row()
	err = row.Scan(&count, &hashB64, &exploredInt)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil, false, nil
		}
		return 0, nil, false, err
	}

	hash, err = base64.StdEncoding.DecodeString(hashB64)
	return count, hash, exploredInt == 1, err
}

func (d *postgresDB) MarkChildrenExplored(ctx context.Context, eventID string) error {
	db := d.cm.Connection(ctx, false)
	return db.Exec(d.updateChildMetadataExploredSQL, eventID).Error
}

func (d *postgresDB) ChildrenForParent(ctx context.Context, eventID, relType string, recentFirst bool) ([]eventInfo, error) {
	var rows *sql.Rows
	var err error

	db := d.cm.Connection(ctx, true)

	// Select the appropriate query based on sort order
	queryStr := d.selectChildrenForParentOldestFirstSQL
	if recentFirst {
		queryStr = d.selectChildrenForParentRecentFirstSQL
	}

	rows, err = db.Raw(queryStr, eventID, relType).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close() // nolint: errcheck

	var result []eventInfo
	for rows.Next() {
		var ei eventInfo
		if err = rows.Scan(&ei.EventID, &ei.OriginServerTS, &ei.RoomID); err != nil {
			return nil, err
		}
		result = append(result, ei)
	}

	return result, rows.Err()
}

func (d *postgresDB) ParentForChild(ctx context.Context, eventID, relType string) (*eventInfo, error) {
	db := d.cm.Connection(ctx, true)

	row := db.Raw(d.selectParentForChildSQL, eventID, relType).Row()

	var ei eventInfo
	err := row.Scan(&ei.EventID, &ei.OriginServerTS, &ei.RoomID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
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
