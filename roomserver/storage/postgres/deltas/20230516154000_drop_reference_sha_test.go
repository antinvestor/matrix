package deltas

import (
	"testing"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestUpDropEventReferenceSHAPrevEvents(t *testing.T) {

	ctx := testrig.NewContext(t)
	cfg, closeRig := testrig.CreateConfig(ctx, t, test.DependancyOption{})
	defer closeRig()

	db, err := sqlutil.Open(&cfg.Global.DatabaseOptions, sqlutil.NewDummyWriter())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	// create the table in the old layout
	_, err = db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS roomserver_previous_events (
    previous_event_id TEXT NOT NULL,
    event_nids BIGINT[] NOT NULL,
    previous_reference_sha256 BYTEA NOT NULL,
    CONSTRAINT roomserver_previous_event_id_unique UNIQUE (previous_event_id, previous_reference_sha256)
);`)
	assert.Nil(t, err)

	// create the events table as well, slimmed down with one eventNID
	_, err = db.ExecContext(ctx, `
CREATE SEQUENCE IF NOT EXISTS roomserver_event_nid_seq;
CREATE TABLE IF NOT EXISTS roomserver_events (
    event_nid BIGINT PRIMARY KEY DEFAULT nextval('roomserver_event_nid_seq'),
    room_nid BIGINT NOT NULL
);

INSERT INTO roomserver_events (event_nid, room_nid) VALUES (1, 1)
`)
	assert.Nil(t, err)

	// insert duplicate prev events with different event_nids
	stmt, err := db.PrepareContext(ctx, `INSERT INTO roomserver_previous_events (previous_event_id, event_nids, previous_reference_sha256) VALUES ($1, $2, $3)`)
	assert.Nil(t, err)
	assert.NotNil(t, stmt)
	_, err = stmt.ExecContext(ctx, "1", pq.Array([]int64{1, 2}), "a")
	assert.Nil(t, err)
	_, err = stmt.ExecContext(ctx, "1", pq.Array([]int64{1, 2, 3}), "b")
	assert.Nil(t, err)
	// execute the migration
	txn, err := db.Begin()
	assert.Nil(t, err)
	assert.NotNil(t, txn)
	defer txn.Rollback()
	err = UpDropEventReferenceSHAPrevEvents(ctx, txn)
	assert.NoError(t, err)
}
