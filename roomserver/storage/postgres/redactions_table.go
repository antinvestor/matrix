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
	"github.com/pitabwire/frame"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
)

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

// SQL query constants for redactions table
const (
	// insertRedactionSQL inserts a new redaction event into the redactions table
	insertRedactionSQL = "" +
		"INSERT INTO roomserver_redactions (redaction_event_id, redacts_event_id, validated)" +
		" VALUES ($1, $2, $3)" +
		" ON CONFLICT DO NOTHING"

	// selectRedactionInfoByRedactionEventIDSQL gets redaction info by the redaction event ID
	selectRedactionInfoByRedactionEventIDSQL = "" +
		"SELECT redaction_event_id, redacts_event_id, validated" +
		" FROM roomserver_redactions" +
		" WHERE redaction_event_id = $1"

	// selectRedactionInfoByEventBeingRedactedSQL gets redaction info by the event being redacted
	selectRedactionInfoByEventBeingRedactedSQL = "" +
		"SELECT redaction_event_id, redacts_event_id, validated" +
		" FROM roomserver_redactions" +
		" WHERE redacts_event_id = $1"

	// markRedactionValidatedSQL updates the validated status of a redaction
	markRedactionValidatedSQL = "" +
		"UPDATE roomserver_redactions" +
		" SET validated = $2" +
		" WHERE redaction_event_id = $1"
)

type redactionsStatements struct {
	cm *sqlutil.Connections
	
	// SQL statements stored as struct fields
	insertRedactionStmt                        string
	selectRedactionInfoByRedactionEventIDStmt  string
	selectRedactionInfoByEventBeingRedactedStmt string
	markRedactionValidatedStmt                 string
}

// NewPostgresRedactionsTable creates a new PostgreSQL redactions table and initializes it
func NewPostgresRedactionsTable(ctx context.Context, cm *sqlutil.Connections) (tables.Redactions, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(redactionsSchema).Error; err != nil {
		return nil, err
	}

	// Initialize the table
	s := &redactionsStatements{
		cm: cm,
		
		// Initialize SQL statement fields with the constants
		insertRedactionStmt:                        insertRedactionSQL,
		selectRedactionInfoByRedactionEventIDStmt:  selectRedactionInfoByRedactionEventIDSQL,
		selectRedactionInfoByEventBeingRedactedStmt: selectRedactionInfoByEventBeingRedactedSQL,
		markRedactionValidatedStmt:                 markRedactionValidatedSQL,
	}

	return s, nil
}

// InsertRedaction adds a new redaction event to the table
func (s *redactionsStatements) InsertRedaction(
	ctx context.Context, info tables.RedactionInfo,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(
		s.insertRedactionStmt,
		info.RedactionEventID,
		info.RedactsEventID,
		info.Validated,
	).Error
}

// SelectRedactionInfoByRedactionEventID gets redaction info by the redaction event ID
func (s *redactionsStatements) SelectRedactionInfoByRedactionEventID(
	ctx context.Context, redactionEventID string,
) (info *tables.RedactionInfo, err error) {
	db := s.cm.Connection(ctx, true)
	info = &tables.RedactionInfo{}

	raw := db.Raw(
		s.selectRedactionInfoByRedactionEventIDStmt,
		redactionEventID,
	).Row()
	err = raw.Scan(
		&info.RedactionEventID,
		&info.RedactsEventID,
		&info.Validated,
	)

	if frame.DBErrorIsRecordNotFound(err) {
		return nil, nil
	}

	return
}

// SelectRedactionInfoByEventBeingRedacted gets redaction info by the event being redacted
func (s *redactionsStatements) SelectRedactionInfoByEventBeingRedacted(
	ctx context.Context, eventID string,
) (info *tables.RedactionInfo, err error) {
	db := s.cm.Connection(ctx, true)
	info = &tables.RedactionInfo{}

	row := db.Raw(
		s.selectRedactionInfoByEventBeingRedactedStmt,
		eventID,
	).Row()

	err = row.Scan(
		&info.RedactionEventID,
		&info.RedactsEventID,
		&info.Validated,
	)

	if frame.DBErrorIsRecordNotFound(err) {
		return nil, nil
	}

	return
}

// MarkRedactionValidated updates the validated status of a redaction
func (s *redactionsStatements) MarkRedactionValidated(
	ctx context.Context, redactionEventID string, validated bool,
) error {
	db := s.cm.Connection(ctx, false)
	return db.Exec(s.markRedactionValidatedStmt, redactionEventID, validated).Error
}
