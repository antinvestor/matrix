// Copyright 2023 The Matrix.org Foundation C.I.C.
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
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/pitabwire/frame"
)

// SQL queries for reported events table
const (
	reportedEventsSchema = `
CREATE SEQUENCE IF NOT EXISTS roomserver_reported_events_id_seq;
CREATE TABLE IF NOT EXISTS roomserver_reported_events
(
	id 					BIGINT PRIMARY KEY DEFAULT nextval('roomserver_reported_events_id_seq'),
    room_nid 			BIGINT NOT NULL,
	event_nid 			BIGINT NOT NULL,
    reporting_user_nid	BIGINT NOT NULL, -- the user reporting the event
    event_sender_nid	BIGINT NOT NULL, -- the user who sent the reported event
    reason      		TEXT,
    score       		INTEGER,
    received_ts 		BIGINT NOT NULL
);`

	reportedEventsSchemaRevert = "DROP TABLE IF EXISTS roomserver_reported_events; DROP SEQUENCE IF EXISTS roomserver_reported_events_id_seq;"

	insertReportedEventSQL = `
	INSERT INTO roomserver_reported_events (room_nid, event_nid, reporting_user_nid, event_sender_nid, reason, score, received_ts) 
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	RETURNING id
`

	selectReportedEventsDescSQL = `
WITH countReports AS (
    SELECT count(*) as report_count
    FROM roomserver_reported_events
    WHERE ($1::BIGINT IS NULL OR room_nid = $1::BIGINT) AND ($2::TEXT IS NULL OR reporting_user_nid = $2::BIGINT)
)
SELECT report_count, id, room_nid, event_nid, reporting_user_nid, event_sender_nid, reason, score, received_ts
FROM roomserver_reported_events, countReports
WHERE ($1::BIGINT IS NULL OR room_nid = $1::BIGINT) AND ($2::TEXT IS NULL OR reporting_user_nid = $2::BIGINT)
ORDER BY received_ts DESC
OFFSET $3
LIMIT $4
`

	selectReportedEventsAscSQL = `
WITH countReports AS (
    SELECT count(*) as report_count
    FROM roomserver_reported_events
    WHERE ($1::BIGINT IS NULL OR room_nid = $1::BIGINT) AND ($2::TEXT IS NULL OR reporting_user_nid = $2::BIGINT)
)
SELECT report_count, id, room_nid, event_nid, reporting_user_nid, event_sender_nid, reason, score, received_ts
FROM roomserver_reported_events, countReports
WHERE ($1::BIGINT IS NULL OR room_nid = $1::BIGINT) AND ($2::TEXT IS NULL OR reporting_user_nid = $2::BIGINT)
ORDER BY received_ts ASC
OFFSET $3
LIMIT $4
`

	selectReportedEventSQL = `
SELECT id, room_nid, event_nid, reporting_user_nid, event_sender_nid, reason, score, received_ts
FROM roomserver_reported_events
WHERE id = $1
`

	deleteReportedEventSQL = `DELETE FROM roomserver_reported_events WHERE id = $1`
)

// reportedEventsStatements holds the SQL queries for reported events
type reportedEventsStatements struct {
	cm sqlutil.ConnectionManager

	// SQL query string fields
	insertReportedEventSQL      string
	selectReportedEventsDescSQL string
	selectReportedEventsAscSQL  string
	selectReportedEventSQL      string
	deleteReportedEventSQL      string
}

// NewPostgresReportedEventsTable creates a new instance of the reported events table.
// It creates the table if it doesn't exist and applies any necessary migrations.
func NewPostgresReportedEventsTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.ReportedEvents, error) {
	s := &reportedEventsStatements{
		cm: cm,

		insertReportedEventSQL:      insertReportedEventSQL,
		selectReportedEventsDescSQL: selectReportedEventsDescSQL,
		selectReportedEventsAscSQL:  selectReportedEventsAscSQL,
		selectReportedEventSQL:      selectReportedEventSQL,
		deleteReportedEventSQL:      deleteReportedEventSQL,
	}

	// Create the table if it doesn't exist using migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "roomserver_reported_events_schema_001",
		Patch:       reportedEventsSchema,
		RevertPatch: reportedEventsSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertReportedEvent inserts a new reported event into the database.
func (r *reportedEventsStatements) InsertReportedEvent(
	ctx context.Context,
	roomNID types.RoomNID,
	eventNID types.EventNID,
	reportingUserID types.EventStateKeyNID,
	eventSenderID types.EventStateKeyNID,
	reason string,
	score int64,
) (int64, error) {
	db := r.cm.Connection(ctx, false)

	var reportID int64
	row := db.Raw(r.insertReportedEventSQL,
		roomNID,
		eventNID,
		reportingUserID,
		eventSenderID,
		reason,
		score,
		spec.AsTimestamp(time.Now()),
	).Row()
	err := row.Scan(&reportID)
	return reportID, err
}

// SelectReportedEvents retrieves reported events with pagination.
func (r *reportedEventsStatements) SelectReportedEvents(
	ctx context.Context,
	from, limit uint64,
	backwards bool,
	reportingUserID types.EventStateKeyNID,
	roomNID types.RoomNID,
) ([]api.QueryAdminEventReportsResponse, int64, error) {
	db := r.cm.Connection(ctx, true)

	var querySQL string
	if backwards {
		querySQL = r.selectReportedEventsDescSQL
	} else {
		querySQL = r.selectReportedEventsAscSQL
	}

	var qryRoomNID *types.RoomNID
	if roomNID > 0 {
		qryRoomNID = &roomNID
	}
	var qryReportingUser *types.EventStateKeyNID
	if reportingUserID > 0 {
		qryReportingUser = &reportingUserID
	}

	rows, err := db.Raw(querySQL,
		qryRoomNID,
		qryReportingUser,
		from,
		limit,
	).Rows()
	if err != nil {
		return nil, 0, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectReportedEvents: failed to close rows")

	var result []api.QueryAdminEventReportsResponse
	var row api.QueryAdminEventReportsResponse
	var count int64
	for rows.Next() {
		if err = rows.Scan(
			&count,
			&row.ID,
			&row.RoomNID,
			&row.EventNID,
			&row.ReportingUserNID,
			&row.SenderNID,
			&row.Reason,
			&row.Score,
			&row.ReceivedTS,
		); err != nil {
			return nil, 0, err
		}
		result = append(result, row)
	}

	return result, count, rows.Err()
}

// SelectReportedEvent retrieves a single reported event by ID.
func (r *reportedEventsStatements) SelectReportedEvent(
	ctx context.Context,
	reportID uint64,
) (api.QueryAdminEventReportResponse, error) {
	db := r.cm.Connection(ctx, true)

	var row api.QueryAdminEventReportResponse
	rowResult := db.Raw(r.selectReportedEventSQL, reportID).Row()
	if err := rowResult.Scan(
		&row.ID,
		&row.RoomNID,
		&row.EventNID,
		&row.ReportingUserNID,
		&row.SenderNID,
		&row.Reason,
		&row.Score,
		&row.ReceivedTS,
	); err != nil {
		return api.QueryAdminEventReportResponse{}, err
	}
	return row, nil
}

// DeleteReportedEvent deletes a reported event by ID.
func (r *reportedEventsStatements) DeleteReportedEvent(ctx context.Context, reportID uint64) error {
	db := r.cm.Connection(ctx, false)
	return db.Exec(r.deleteReportedEventSQL, reportID).Error
}
