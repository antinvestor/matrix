// Copyright 2020 The Global.org Foundation C.I.C.
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
	"errors"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"gorm.io/gorm"
)

// --- SQL statements as constants ---
const (
	redactionsSchema = `
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

	redactionsSchemaRevert = `DROP TABLE IF EXISTS roomserver_redactions;`

	insertRedactionSQL                         = "INSERT INTO roomserver_redactions (redaction_event_id, redacts_event_id, validated) VALUES (?, ?, ?) ON CONFLICT DO NOTHING"
	selectRedactionInfoByRedactionEventIDSQL   = "SELECT redaction_event_id, redacts_event_id, validated FROM roomserver_redactions WHERE redaction_event_id = ?"
	selectRedactionInfoByEventBeingRedactedSQL = "SELECT redaction_event_id, redacts_event_id, validated FROM roomserver_redactions WHERE redacts_event_id = ?"
	markRedactionValidatedSQL                  = "UPDATE roomserver_redactions SET validated = ? WHERE redaction_event_id = ?"
)

// --- Table struct with SQL fields ---
type redactionsTable struct {
	cm                                         *sqlutil.Connections
	insertRedactionSQL                         string
	selectRedactionInfoByRedactionEventIDSQL   string
	selectRedactionInfoByEventBeingRedactedSQL string
	markRedactionValidatedSQL                  string
}

// --- Constructor ---
func NewPostgresRedactionsTable(cm *sqlutil.Connections) tables.Redactions {
	return &redactionsTable{
		cm:                                       cm,
		insertRedactionSQL:                       insertRedactionSQL,
		selectRedactionInfoByRedactionEventIDSQL: selectRedactionInfoByRedactionEventIDSQL,
		selectRedactionInfoByEventBeingRedactedSQL: selectRedactionInfoByEventBeingRedactedSQL,
		markRedactionValidatedSQL:                  markRedactionValidatedSQL,
	}
}

// --- Methods referencing struct fields and using GORM ---
func (t *redactionsTable) InsertRedaction(ctx context.Context, info tables.RedactionInfo) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.insertRedactionSQL, info.RedactionEventID, info.RedactsEventID, info.Validated).Error
}

func (t *redactionsTable) SelectRedactionInfoByRedactionEventID(ctx context.Context, redactionEventID string) (*tables.RedactionInfo, error) {
	db := t.cm.Connection(ctx, true)
	var info tables.RedactionInfo
	result := db.Raw(t.selectRedactionInfoByRedactionEventIDSQL, redactionEventID).Scan(&info)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if result.Error != nil {
		return nil, result.Error
	}
	return &info, nil
}

func (t *redactionsTable) SelectRedactionInfoByEventBeingRedacted(ctx context.Context, eventID string) (*tables.RedactionInfo, error) {
	db := t.cm.Connection(ctx, true)
	var info tables.RedactionInfo
	result := db.Raw(t.selectRedactionInfoByEventBeingRedactedSQL, eventID).Scan(&info)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if result.Error != nil {
		return nil, result.Error
	}
	return &info, nil
}

func (t *redactionsTable) MarkRedactionValidated(ctx context.Context, redactionEventID string, validated bool) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.markRedactionValidatedSQL, validated, redactionEventID).Error
}
