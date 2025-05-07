// Copyright 2021 Dan Peleg <dan@globekeeper.com>
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
	"database/sql"
	"encoding/json"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

const notificationSchema = `
CREATE TABLE IF NOT EXISTS userapi_notifications (
    id BIGSERIAL PRIMARY KEY,
	localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
	room_id TEXT NOT NULL,
	event_id TEXT NOT NULL,
	stream_pos BIGINT NOT NULL,
    ts_ms BIGINT NOT NULL,
    highlight BOOLEAN NOT NULL,
    notification_json TEXT NOT NULL,
    read BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS userapi_notification_localpart_room_id_event_id_idx ON userapi_notifications(localpart, server_name, room_id, event_id);
CREATE INDEX IF NOT EXISTS userapi_notification_localpart_room_id_id_idx ON userapi_notifications(localpart, server_name, room_id, id);
CREATE INDEX IF NOT EXISTS userapi_notification_localpart_id_idx ON userapi_notifications(localpart, server_name, id);
`

// SQL query constants for notification operations
const (
	// insertNotificationSQL inserts a notification
	insertNotificationSQL = "INSERT INTO userapi_notifications (localpart, server_name, room_id, event_id, stream_pos, ts_ms, highlight, notification_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

	// deleteNotificationsUpToSQL deletes notifications up to a specified stream position
	deleteNotificationsUpToSQL = "DELETE FROM userapi_notifications WHERE localpart = $1 AND server_name = $2 AND room_id = $3 AND stream_pos <= $4"

	// updateNotificationReadSQL updates the read status of notifications
	updateNotificationReadSQL = "UPDATE userapi_notifications SET read = $1 WHERE localpart = $2 AND server_name = $3 AND room_id = $4 AND stream_pos <= $5 AND read <> $1"

	// selectNotificationSQL selects notifications with filtering
	selectNotificationSQL = "SELECT id, room_id, ts_ms, read, notification_json FROM userapi_notifications WHERE localpart = $1 AND server_name = $2 AND id > $3 AND (" +
		"(($4 & 1) <> 0 AND highlight) OR (($4 & 2) <> 0 AND NOT highlight)" +
		") AND NOT read ORDER BY localpart, id LIMIT $5"

	// selectNotificationCountSQL counts notifications with filtering
	selectNotificationCountSQL = "SELECT COUNT(*) FROM userapi_notifications WHERE localpart = $1 AND server_name = $2 AND (" +
		"(($3 & 1) <> 0 AND highlight) OR (($3 & 2) <> 0 AND NOT highlight)" +
		") AND NOT read"

	// selectRoomNotificationCountsSQL counts total and highlight notifications for a room
	selectRoomNotificationCountsSQL = "SELECT COUNT(*), COUNT(*) FILTER (WHERE highlight) FROM userapi_notifications " +
		"WHERE localpart = $1 AND server_name = $2 AND room_id = $3 AND NOT read"

	// cleanNotificationsSQL removes old notifications based on timestamp
	cleanNotificationsSQL = "DELETE FROM userapi_notifications WHERE" +
		" (highlight = FALSE AND ts_ms < $1) OR (highlight = TRUE AND ts_ms < $2)"
)

type notificationsTable struct {
	cm *sqlutil.Connections
	
	// SQL queries stored as fields for better maintainability
	insertStmt             string
	deleteUpToStmt         string
	updateReadStmt         string
	selectStmt             string
	selectCountStmt        string
	selectRoomCountsStmt   string
	cleanNotificationsStmt string
}

func NewPostgresNotificationTable(ctx context.Context, cm *sqlutil.Connections) (tables.NotificationTable, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(notificationSchema).Error; err != nil {
		return nil, err
	}
	
	// Initialize table with SQL statements
	t := &notificationsTable{
		cm: cm,
		insertStmt:            insertNotificationSQL,
		deleteUpToStmt:        deleteNotificationsUpToSQL,
		updateReadStmt:        updateNotificationReadSQL,
		selectStmt:            selectNotificationSQL,
		selectCountStmt:       selectNotificationCountSQL,
		selectRoomCountsStmt:  selectRoomNotificationCountsSQL,
		cleanNotificationsStmt: cleanNotificationsSQL,
	}
	
	return t, nil
}

func (t *notificationsTable) Clean(ctx context.Context) error {
	db := t.cm.Connection(ctx, false)
	
	return db.Exec(
		t.cleanNotificationsStmt,
		time.Now().AddDate(0, 0, -1).UnixNano()/int64(time.Millisecond), // keep non-highlights for a day
		time.Now().AddDate(0, -1, 0).UnixNano()/int64(time.Millisecond), // keep highlights for a month
	).Error
}

// Insert inserts a notification into the database.
func (t *notificationsTable) Insert(ctx context.Context, localpart string, serverName spec.ServerName, eventID string, pos uint64, highlight bool, n *api.Notification) error {
	db := t.cm.Connection(ctx, false)
	
	roomID, tsMS := n.RoomID, n.TS
	nn := *n
	// Clears out fields that have their own columns to (1) shrink the
	// data and (2) avoid difficult-to-debug inconsistency bugs.
	nn.RoomID = ""
	nn.TS, nn.Read = 0, false
	bs, err := json.Marshal(nn)
	if err != nil {
		return err
	}
	
	return db.Exec(t.insertStmt, localpart, serverName, roomID, eventID, pos, tsMS, highlight, string(bs)).Error
}

// DeleteUpTo deletes all previous notifications, up to and including the event.
func (t *notificationsTable) DeleteUpTo(ctx context.Context, localpart string, serverName spec.ServerName, roomID string, pos uint64) (affected bool, _ error) {
	db := t.cm.Connection(ctx, false)
	
	result := db.Exec(t.deleteUpToStmt, localpart, serverName, roomID, pos)
	if err := result.Error; err != nil {
		return false, err
	}
	
	nrows := result.RowsAffected
	log.WithFields(log.Fields{"localpart": localpart, "room_id": roomID, "stream_pos": pos}).Tracef("DeleteUpTo: %d rows affected", nrows)
	return nrows > 0, nil
}

// UpdateRead updates the "read" value for an event.
func (t *notificationsTable) UpdateRead(ctx context.Context, localpart string, serverName spec.ServerName, roomID string, pos uint64, v bool) (affected bool, _ error) {
	db := t.cm.Connection(ctx, false)
	
	result := db.Exec(t.updateReadStmt, v, localpart, serverName, roomID, pos)
	if err := result.Error; err != nil {
		return false, err
	}
	
	nrows := result.RowsAffected
	log.WithFields(log.Fields{"localpart": localpart, "room_id": roomID, "stream_pos": pos}).Tracef("UpdateRead: %d rows affected", nrows)
	return nrows > 0, nil
}

func (t *notificationsTable) Select(ctx context.Context, localpart string, serverName spec.ServerName, fromID int64, limit int, filter tables.NotificationFilter) ([]*api.Notification, int64, error) {
	db := t.cm.Connection(ctx, true)
	
	rows, err := db.Raw(t.selectStmt, localpart, serverName, fromID, uint32(filter), limit).Rows()
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	
	lastID := fromID
	notifs := []*api.Notification{}
	for rows.Next() {
		var id int64
		var roomID string
		var tsMS int64
		var read bool
		var jsonStr string
		if err = rows.Scan(&id, &roomID, &tsMS, &read, &jsonStr); err != nil {
			return nil, 0, err
		}
		
		var n api.Notification
		if err = json.Unmarshal([]byte(jsonStr), &n); err != nil {
			log.WithFields(log.Fields{
				log.ErrorKey: err,
				"localpart":  localpart,
				"id":         id,
				"json":       jsonStr,
			}).Error("unable to decode notification")
			continue
		}
		
		// Overwrite values decoded from JSON with the values from dedicated columns
		n.RoomID, n.TS, n.Read = roomID, tsMS, read
		notifs = append(notifs, &n)
		lastID = id
	}
	
	return notifs, lastID, nil
}

func (t *notificationsTable) SelectCount(ctx context.Context, localpart string, serverName spec.ServerName, filter tables.NotificationFilter) (count int64, err error) {
	db := t.cm.Connection(ctx, true)
	
	row := db.Raw(t.selectCountStmt, localpart, serverName, uint32(filter)).Row()
	err = row.Scan(&count)
	return
}

func (t *notificationsTable) SelectRoomCounts(ctx context.Context, localpart string, serverName spec.ServerName, roomID string) (total int64, highlight int64, err error) {
	db := t.cm.Connection(ctx, true)
	
	row := db.Raw(t.selectRoomCountsStmt, localpart, serverName, roomID).Row()
	err = row.Scan(&total, &highlight)
	return
}
