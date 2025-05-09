// Copyright 2020 The Matrix.org Foundation C.I.C.
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
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/pitabwire/frame"
)

// Schema for the redactions table
const redactionsSchema = `
-- Stores information about the redacted state of events.
-- We need to track redactions rather than blindly updating the event JSON table on receipt of a redaction
-- because we might receive the redaction BEFORE we receive the event which it redacts (think backfill).
CREATE TABLE IF NOT EXISTS roomserver_redactions (
    redaction_event_id TEXT PRIMARY KEY,
	redacts_event_id TEXT NOT NULL,
	-- Initially FALSE, set to TRUE when the redaction has been validated according to rooms v3+ spec
	-- https://matrix.org/docs/spec/rooms/v3#authorization-rules-for-events
	validated BOOLEAN NOT NULL
);
CREATE INDEX IF NOT EXISTS roomserver_redactions_redacts_event_id ON roomserver_redactions(redacts_event_id);
`

// Schema revert script for migration purposes
const redactionsSchemaRevert = `DROP TABLE IF EXISTS roomserver_redactions;`

// SQL to insert a new redaction record
const insertRedactionSQL = "" +
	"INSERT INTO roomserver_redactions (redaction_event_id, redacts_event_id, validated)" +
	" VALUES ($1, $2, $3)" +
	" ON CONFLICT DO NOTHING"

// SQL to select redaction info by redaction event ID
const selectRedactionInfoByRedactionEventIDSQL = "" +
	"SELECT redaction_event_id, redacts_event_id, validated FROM roomserver_redactions" +
	" WHERE redaction_event_id = $1"

// SQL to select redaction info by the event being redacted
const selectRedactionInfoByEventBeingRedactedSQL = "" +
	"SELECT redaction_event_id, redacts_event_id, validated FROM roomserver_redactions" +
	" WHERE redacts_event_id = $1"

// SQL to mark a redaction as validated or not
const markRedactionValidatedSQL = "" +
	" UPDATE roomserver_redactions SET validated = $2 WHERE redaction_event_id = $1"

// redactionsTable implements the tables.Redactions interface using GORM
type redactionsTable struct {
	cm *sqlutil.Connections

	// SQL query strings loaded from constants
	insertRedactionSQL                         string
	selectRedactionInfoByRedactionEventIDSQL   string
	selectRedactionInfoByEventBeingRedactedSQL string
	markRedactionValidatedSQL                  string
}

// NewPostgresRedactionsTable creates a new redactions table
func NewPostgresRedactionsTable(ctx context.Context, cm *sqlutil.Connections) (tables.Redactions, error) {
	// Create the table if it doesn't exist using migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "roomserver_redactions_table_schema_001",
		Patch:       redactionsSchema,
		RevertPatch: redactionsSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Initialize the table struct with just the connection manager
	t := &redactionsTable{
		cm: cm,

		// Initialize SQL query strings from constants
		insertRedactionSQL:                         insertRedactionSQL,
		selectRedactionInfoByRedactionEventIDSQL:   selectRedactionInfoByRedactionEventIDSQL,
		selectRedactionInfoByEventBeingRedactedSQL: selectRedactionInfoByEventBeingRedactedSQL,
		markRedactionValidatedSQL:                  markRedactionValidatedSQL,
	}

	return t, nil
}

func (t *redactionsTable) InsertRedaction(
	ctx context.Context, info tables.RedactionInfo,
) error {
	db := t.cm.Connection(ctx, false)

	result := db.Exec(t.insertRedactionSQL, info.RedactionEventID, info.RedactsEventID, info.Validated)
	return result.Error
}

func (t *redactionsTable) SelectRedactionInfoByRedactionEventID(
	ctx context.Context, redactionEventID string,
) (info *tables.RedactionInfo, err error) {
	db := t.cm.Connection(ctx, true)

	info = &tables.RedactionInfo{}
	row := db.Raw(t.selectRedactionInfoByRedactionEventIDSQL, redactionEventID).Row()
	err = row.Scan(&info.RedactionEventID, &info.RedactsEventID, &info.Validated)
	if sqlutil.ErrorIsNoRows(err) {
		info = nil
		err = nil
	}
	return
}

func (t *redactionsTable) SelectRedactionInfoByEventBeingRedacted(
	ctx context.Context, eventID string,
) (info *tables.RedactionInfo, err error) {
	db := t.cm.Connection(ctx, true)

	info = &tables.RedactionInfo{}
	row := db.Raw(t.selectRedactionInfoByEventBeingRedactedSQL, eventID).Row()
	err = row.Scan(&info.RedactionEventID, &info.RedactsEventID, &info.Validated)
	if sqlutil.ErrorIsNoRows(err) {
		info = nil
		err = nil
	}
	return
}

func (t *redactionsTable) MarkRedactionValidated(
	ctx context.Context, redactionEventID string, validated bool,
) error {
	db := t.cm.Connection(ctx, false)

	result := db.Exec(t.markRedactionValidatedSQL, redactionEventID, validated)
	return result.Error
}
