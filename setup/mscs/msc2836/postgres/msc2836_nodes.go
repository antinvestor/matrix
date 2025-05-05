package postgres

import (
	"context"
	"github.com/antinvestor/matrix/internal/sqlutil"
)

const msc2836NodesSchema = `
CREATE TABLE IF NOT EXISTS msc2836_nodes (
	event_id TEXT PRIMARY KEY NOT NULL,
	origin_server_ts BIGINT NOT NULL,
	room_id TEXT NOT NULL,
	unsigned_children_count BIGINT NOT NULL,
	unsigned_children_hash TEXT NOT NULL,
	explored SMALLINT NOT NULL
);
`

const msc2836NodesSchemaRevert = `DROP TABLE IF EXISTS msc2836_nodes;`

const insertNodeSQL = `INSERT INTO msc2836_nodes(event_id, origin_server_ts, room_id, unsigned_children_count, unsigned_children_hash, explored)
VALUES($1, $2, $3, $4, $5, $6)
ON CONFLICT DO NOTHING`

// All SQL queries as constants with comments

// msc2836NodeTable implements the node operations for MSC2836
// All DB access uses the connection manager and GORM-style API
// All SQL is referenced via struct fields
// No *sql.Stmt, *sql.Tx, or direct transaction logic
// Struct is unexported, only constructor is exported

type msc2836NodeTable struct {
	cm            *sqlutil.Connections
	insertNodeSQL string
}

// NewPostgresMSC2836NodeTable returns a new node table implementation for MSC2836
func NewPostgresMSC2836NodeTable(cm *sqlutil.Connections) *msc2836NodeTable {
	return &msc2836NodeTable{
		cm:            cm,
		insertNodeSQL: insertNodeSQL,
	}
}

// InsertNode inserts a node row using the connection manager
func (t *msc2836NodeTable) InsertNode(ctx context.Context, eventID string, originServerTS int64, roomID string, unsignedChildrenCount int64, unsignedChildrenHash string, explored int) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.insertNodeSQL, eventID, originServerTS, roomID, unsignedChildrenCount, unsignedChildrenHash, explored).Error
}
