package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/mscs/msc2836/shared"
	"github.com/pitabwire/util"
)

const msc2836EdgesSchema = `
CREATE TABLE IF NOT EXISTS msc2836_edges (
	parent_event_id TEXT NOT NULL,
	child_event_id TEXT NOT NULL,
	rel_type TEXT NOT NULL,
	parent_room_id TEXT NOT NULL,
	parent_servers TEXT NOT NULL,
	CONSTRAINT msc2836_edges_uniq UNIQUE (parent_event_id, child_event_id, rel_type)
);
`
const msc2836EdgesSchemaRevert = `DROP TABLE IF EXISTS msc2836_edges;`

const insertEdgeSQL = `
INSERT INTO msc2836_edges(parent_event_id, child_event_id, rel_type, parent_room_id, parent_servers)
VALUES($1, $2, $3, $4, $5)
ON CONFLICT DO NOTHING`

const selectChildrenSQL = `SELECT child_event_id, origin_server_ts, room_id FROM msc2836_edges
LEFT JOIN msc2836_nodes ON msc2836_edges.child_event_id = msc2836_nodes.event_id
WHERE parent_event_id = $1 AND rel_type = $2
ORDER BY origin_server_ts`

const selectParentSQL = `SELECT parent_event_id, parent_room_id FROM msc2836_edges WHERE child_event_id = $1 AND rel_type = $2`
const updateChildMetadataSQL = `UPDATE msc2836_nodes SET unsigned_children_count=$1, unsigned_children_hash=$2, explored=$3 WHERE event_id=$4`
const selectChildMetadataSQL = `SELECT unsigned_children_count, unsigned_children_hash, explored FROM msc2836_nodes WHERE event_id=$1`
const updateChildMetadataExploredSQL = `UPDATE msc2836_nodes SET explored=$1 WHERE event_id=$2`

// msc2836EdgeTable implements the shared.Database interface for MSC2836 edges
// All DB access uses the connection manager and GORM-style API
// All SQL is referenced via struct fields
// No *sql.Stmt, *sql.Tx, or direct transaction logic
// Struct is unexported, only constructor is exported

type msc2836EdgeTable struct {
	*msc2836NodeTable

	cm                             *sqlutil.Connections
	insertEdgeSQL                  string
	selectChildrenSQL              string
	selectParentSQL                string
	updateChildMetadataSQL         string
	selectChildMetadataSQL         string
	updateChildMetadataExploredSQL string
}

// NewPostgresMSC2836EdgeTable returns a new shared.Database implementation for MSC2836 edges
func NewPostgresMSC2836EdgeTable(cm *sqlutil.Connections) shared.Database {
	nodeTable := NewPostgresMSC2836NodeTable(cm)

	return &msc2836EdgeTable{
		msc2836NodeTable:               nodeTable,
		cm:                             cm,
		insertEdgeSQL:                  insertEdgeSQL,
		selectChildrenSQL:              selectChildrenSQL,
		selectParentSQL:                selectParentSQL,
		updateChildMetadataSQL:         updateChildMetadataSQL,
		selectChildMetadataSQL:         selectChildMetadataSQL,
		updateChildMetadataExploredSQL: updateChildMetadataExploredSQL,
	}
}

func (t *msc2836EdgeTable) StoreRelation(ctx context.Context, ev *types.HeaderedEvent) error {
	parent, child, relType := shared.ParentChildEventIDs(ev)
	if parent == "" || child == "" {
		return nil
	}
	relationRoomID, relationServers := roomIDAndServers(ev)
	relationServersJSON, err := json.Marshal(relationServers)
	if err != nil {
		return err
	}
	count, hash := extractChildMetadata(ev)
	db := t.cm.Connection(ctx, false)
	err = db.Exec(t.insertEdgeSQL, parent, child, relType, relationRoomID, string(relationServersJSON)).Error
	if err != nil {
		return err
	}
	util.GetLogger(ctx).Infof("StoreRelation child=%s parent=%s rel_type=%s", child, parent, relType)
	return db.Exec(insertNodeSQL, ev.EventID(), ev.OriginServerTS(), ev.RoomID().String(), count, base64.RawStdEncoding.EncodeToString(hash), 0).Error
}

func (t *msc2836EdgeTable) UpdateChildMetadata(ctx context.Context, ev *types.HeaderedEvent) error {
	eventCount, eventHash := extractChildMetadata(ev)
	if eventCount == 0 {
		return nil // nothing to update with
	}

	// extract current children count/hash, if they are less than the current event then update the columns and set to unexplored
	count, hash, _, err := t.ChildMetadata(ctx, ev.EventID())
	if err != nil {
		return err
	}
	if eventCount > count || (eventCount == count && !bytes.Equal(hash, eventHash)) {
		db := t.cm.Connection(ctx, false)
		return db.Exec(t.updateChildMetadataSQL, eventCount, base64.RawStdEncoding.EncodeToString(eventHash), 0, ev.EventID()).Error
	}
	return nil
}

func (t *msc2836EdgeTable) ChildMetadata(ctx context.Context, eventID string) (count int, hash []byte, explored bool, err error) {
	var b64hash string
	var exploredInt int
	db := t.cm.Connection(ctx, false)
	err = db.Raw(t.selectChildMetadataSQL, eventID).Row().Scan(&count, &b64hash, &exploredInt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
		}
		return
	}
	hash, err = base64.RawStdEncoding.DecodeString(b64hash)
	explored = exploredInt > 0
	return
}

func (t *msc2836EdgeTable) MarkChildrenExplored(ctx context.Context, eventID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.updateChildMetadataExploredSQL, 1, eventID).Error
}

func (t *msc2836EdgeTable) ChildrenForParent(ctx context.Context, eventID, relType string, recentFirst bool) ([]shared.EventInfo, error) {
	var rows *sql.Rows
	var err error
	db := t.cm.Connection(ctx, false)
	if recentFirst {
		rows, err = db.Raw(t.selectChildrenSQL+" DESC", eventID, relType).Rows()
	} else {
		rows, err = db.Raw(t.selectChildrenSQL+" ASC", eventID, relType).Rows()
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close() // nolint: errcheck
	var children []shared.EventInfo
	for rows.Next() {
		var evInfo shared.EventInfo
		if err := rows.Scan(&evInfo.EventID, &evInfo.OriginServerTS, &evInfo.RoomID); err != nil {
			return nil, err
		}
		children = append(children, evInfo)
	}
	return children, rows.Err()
}

func (t *msc2836EdgeTable) ParentForChild(ctx context.Context, eventID, relType string) (*shared.EventInfo, error) {
	var ei shared.EventInfo
	db := t.cm.Connection(ctx, false)
	err := db.Raw(t.selectParentSQL, eventID, relType).Row().Scan(&ei.EventID, &ei.RoomID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &ei, nil
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
