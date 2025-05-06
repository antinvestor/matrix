// Copyright 2023 The Global.org Foundation C.I.C.
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
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
)

const reportedEventsSchema = `
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

const reportedEventsSchemaRevert = `DROP TABLE IF EXISTS roomserver_reported_events;`

// Insert a new reported event.
const insertReportedEventSQL = `
	INSERT INTO roomserver_reported_events (room_nid, event_nid, reporting_user_nid, event_sender_nid, reason, score, received_ts) 
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	RETURNING id
`

// Select reported events in descending order of received timestamp.
const selectReportedEventsDescSQL = `
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

// Select reported events in ascending order of received timestamp.
const selectReportedEventsAscSQL = `
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

// Select a single reported event by ID.
const selectReportedEventSQL = `
SELECT id, room_nid, event_nid, reporting_user_nid, event_sender_nid, reason, score, received_ts
FROM roomserver_reported_events
WHERE id = $1
`

// Delete a reported event by ID.
const deleteReportedEventSQL = `DELETE FROM roomserver_reported_events WHERE id = $1`

// Refactored struct for GORM usage
// All SQL strings are struct fields, set at initialization

type reportedEventsTable struct {
	cm                          *sqlutil.Connections
	insertReportedEventSQL      string
	selectReportedEventsDescSQL string
	selectReportedEventsAscSQL  string
	selectReportedEventSQL      string
	deleteReportedEventSQL      string
}

func NewPostgresReportedEventsTable(cm *sqlutil.Connections) tables.ReportedEvents {
	return &reportedEventsTable{
		cm:                          cm,
		insertReportedEventSQL:      insertReportedEventSQL,
		selectReportedEventsDescSQL: selectReportedEventsDescSQL,
		selectReportedEventsAscSQL:  selectReportedEventsAscSQL,
		selectReportedEventSQL:      selectReportedEventSQL,
		deleteReportedEventSQL:      deleteReportedEventSQL,
	}
}

// InsertReportedEvent inserts a new reported event and returns its ID.
func (t *reportedEventsTable) InsertReportedEvent(
	ctx context.Context,
	roomNID types.RoomNID,
	eventNID types.EventNID,
	reportingUserID types.EventStateKeyNID,
	eventSenderID types.EventStateKeyNID,
	reason string,
	score int64,
	receivedTS int64,
) (int64, error) {
	// Get a database connection for the given context.
	db := t.cm.Connection(ctx, false)
	var id int64
	// Execute the insert query and scan the result into the id variable.
	err := db.Raw(
		t.insertReportedEventSQL,
		roomNID, eventNID, reportingUserID, eventSenderID, reason, score, receivedTS,
	).Row().Scan(&id)
	return id, err
}

// SelectReportedEvents returns a slice of reported events and the total report count.
func (t *reportedEventsTable) SelectReportedEvents(
	ctx context.Context,
	from, limit uint64,
	backwards bool,
	reportingUserID types.EventStateKeyNID,
	roomNID types.RoomNID,
) ([]api.QueryAdminEventReportsResponse, int64, error) {
	// Get a read-only database connection for the given context.
	db := t.cm.Connection(ctx, true)
	var sqlStr string
	if backwards {
		// If backwards is true, use the descending order SQL query.
		sqlStr = t.selectReportedEventsDescSQL
	} else {
		// Otherwise, use the ascending order SQL query.
		sqlStr = t.selectReportedEventsAscSQL
	}
	// Execute the query and get the rows.
	rows, err := db.Raw(sqlStr, roomNID, reportingUserID, from, limit).Rows()
	if err != nil {
		return nil, 0, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "failed to close rows")
	// Initialize variables to store the report count and the slice of reports.
	var reports []api.QueryAdminEventReportsResponse
	var reportCount int64
	// Iterate over the rows and scan each row into a report.
	for rows.Next() {
		var r api.QueryAdminEventReportsResponse
		if err := rows.Scan(&reportCount, &r.ID, &r.RoomNID, &r.EventNID, &r.ReportingUserNID, &r.SenderNID, &r.Reason, &r.Score, &r.ReceivedTS); err != nil {
			return nil, 0, err
		}
		reports = append(reports, r)
	}
	// Return the slice of reports and the report count.
	return reports, reportCount, rows.Err()
}

// SelectReportedEvent fetches a single reported event by its ID.
func (t *reportedEventsTable) SelectReportedEvent(
	ctx context.Context,
	reportID uint64,
) (api.QueryAdminEventReportResponse, error) {
	// Get a read-only database connection for the given context.
	db := t.cm.Connection(ctx, true)
	var r api.QueryAdminEventReportResponse
	// Execute the query and scan the result into the report variable.
	row := db.Raw(t.selectReportedEventSQL, reportID).Row()
	err := row.Scan(&r.ID, &r.RoomNID, &r.EventNID, &r.ReportingUserNID, &r.SenderNID, &r.Reason, &r.Score, &r.ReceivedTS)
	return r, err
}

// DeleteReportedEvent deletes a reported event by its ID.
func (t *reportedEventsTable) DeleteReportedEvent(
	ctx context.Context,
	reportID uint64,
) error {
	// Get a database connection for the given context.
	db := t.cm.Connection(ctx, false)
	// Execute the delete query.
	return db.Exec(t.deleteReportedEventSQL, reportID).Error
}
