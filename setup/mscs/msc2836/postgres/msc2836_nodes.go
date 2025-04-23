package postgres

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
