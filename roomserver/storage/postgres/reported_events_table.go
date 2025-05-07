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
)

const reportedEventsScheme = `
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

// SQL query constants for reported events operations
const (
	// insertReportedEventSQL inserts a new reported event
	insertReportedEventSQL = `
		INSERT INTO roomserver_reported_events (room_nid, event_nid, reporting_user_nid, event_sender_nid, reason, score, received_ts) 
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id
	`

	// selectReportedEventsDescSQL selects reported events in descending order
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

	// selectReportedEventsAscSQL selects reported events in ascending order
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

	// selectReportedEventSQL selects a single reported event by ID
	selectReportedEventSQL = `
	SELECT id, room_nid, event_nid, reporting_user_nid, event_sender_nid, reason, score, received_ts
	FROM roomserver_reported_events
	WHERE id = $1
	`

	// deleteReportedEventSQL deletes a reported event by ID
	deleteReportedEventSQL = `DELETE FROM roomserver_reported_events WHERE id = $1`
)

type reportedEventsStatements struct {
	cm *sqlutil.Connections
	
	// SQL statements stored as struct fields
	insertReportedEventStmt       string
	selectReportedEventsDescStmt  string
	selectReportedEventsAscStmt   string
	selectReportedEventStmt       string
	deleteReportedEventStmt       string
}

func NewPostgresReportedEventsTable(ctx context.Context, cm *sqlutil.Connections) (tables.ReportedEvents, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(reportedEventsScheme).Error; err != nil {
		return nil, err
	}
	
	// Initialize the table
	s := &reportedEventsStatements{
		cm: cm,
		
		// Initialize SQL statement fields with the constants
		insertReportedEventStmt:      insertReportedEventSQL,
		selectReportedEventsDescStmt: selectReportedEventsDescSQL,
		selectReportedEventsAscStmt:  selectReportedEventsAscSQL,
		selectReportedEventStmt:      selectReportedEventSQL,
		deleteReportedEventStmt:      deleteReportedEventSQL,
	}
	
	return s, nil
}

func (r *reportedEventsStatements) InsertReportedEvent(
	ctx context.Context,
	roomNID types.RoomNID,
	eventNID types.EventNID,
	reportingUserID types.EventStateKeyNID,
	eventSenderID types.EventStateKeyNID,
	reason string,
	score int64,
) (int64, error) {
	// Get database connection
	db := r.cm.Connection(ctx, false)
	
	var reportID int64
	err := db.Raw(
		r.insertReportedEventStmt,
		roomNID,
		eventNID,
		reportingUserID,
		eventSenderID,
		reason,
		score,
		spec.AsTimestamp(time.Now()),
	).Scan(&reportID).Error
	
	return reportID, err
}

func (r *reportedEventsStatements) SelectReportedEvents(
	ctx context.Context,
	from, limit uint64,
	backwards bool,
	reportingUserID types.EventStateKeyNID,
	roomNID types.RoomNID,
) ([]api.QueryAdminEventReportsResponse, int64, error) {
	// Get database connection
	db := r.cm.Connection(ctx, true)
	
	var sqlQuery string
	if backwards {
		sqlQuery = r.selectReportedEventsDescStmt
	} else {
		sqlQuery = r.selectReportedEventsAscStmt
	}

	var qryRoomNID *types.RoomNID
	if roomNID > 0 {
		qryRoomNID = &roomNID
	}
	var qryReportingUser *types.EventStateKeyNID
	if reportingUserID > 0 {
		qryReportingUser = &reportingUserID
	}

	rows, err := db.Raw(
		sqlQuery,
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

func (r *reportedEventsStatements) SelectReportedEvent(
	ctx context.Context,
	reportID uint64,
) (api.QueryAdminEventReportResponse, error) {
	// Get database connection
	db := r.cm.Connection(ctx, true)
	
	var row api.QueryAdminEventReportResponse
	err := db.Raw(
		r.selectReportedEventStmt,
		reportID,
	).Scan(
		&row.ID,
		&row.RoomNID,
		&row.EventNID,
		&row.ReportingUserNID,
		&row.SenderNID,
		&row.Reason,
		&row.Score,
		&row.ReceivedTS,
	).Error
	
	return row, err
}

func (r *reportedEventsStatements) DeleteReportedEvent(ctx context.Context, reportID uint64) error {
	// Get database connection
	db := r.cm.Connection(ctx, false)
	
	return db.Exec(r.deleteReportedEventStmt, reportID).Error
}
