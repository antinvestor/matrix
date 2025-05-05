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
	"encoding/json"
	"github.com/antinvestor/matrix/internal"
	"time"

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

const notificationsSchemaRevert = "DROP TABLE IF EXISTS userapi_notifications CASCADE;"

const insertNotificationSQL = "" +
	"INSERT INTO userapi_notifications (localpart, server_name, room_id, event_id, stream_pos, ts_ms, highlight, notification_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

const deleteNotificationsUpToSQL = "" +
	"DELETE FROM userapi_notifications WHERE localpart = $1 AND server_name = $2 AND room_id = $3 AND stream_pos <= $4"

const updateNotificationReadSQL = "" +
	"UPDATE userapi_notifications SET read = $1 WHERE localpart = $2 AND server_name = $3 AND room_id = $4 AND stream_pos <= $5 AND read <> $1"

const selectNotificationSQL = "" +
	"SELECT id, room_id, ts_ms, read, notification_json FROM userapi_notifications WHERE localpart = $1 AND server_name = $2 AND id > $3 AND (" +
	"(($4 & 1) <> 0 AND highlight) OR (($4 & 2) <> 0 AND NOT highlight)" +
	") AND NOT read ORDER BY localpart, id LIMIT $5"

const selectNotificationCountSQL = "" +
	"SELECT COUNT(*) FROM userapi_notifications WHERE localpart = $1 AND server_name = $2 AND (" +
	"(($3 & 1) <> 0 AND highlight) OR (($3 & 2) <> 0 AND NOT highlight)" +
	") AND NOT read"

const selectRoomNotificationCountsSQL = "" +
	"SELECT COUNT(*), COUNT(*) FILTER (WHERE highlight) FROM userapi_notifications " +
	"WHERE localpart = $1 AND server_name = $2 AND room_id = $3 AND NOT read"

const cleanNotificationsSQL = "" +
	"DELETE FROM userapi_notifications WHERE" +
	" (highlight = FALSE AND ts_ms < $1) OR (highlight = TRUE AND ts_ms < $2)"

// notificationsTable implements tables.NotificationsTable using GORM and a connection manager.
type notificationsTable struct {
	cm *sqlutil.Connections

	insertNotificationSQL           string
	deleteNotificationsUpToSQL      string
	updateNotificationReadSQL       string
	selectNotificationSQL           string
	selectNotificationCountSQL      string
	selectRoomNotificationCountsSQL string
	cleanNotificationsSQL           string
}

// NewPostgresNotificationsTable returns a new NotificationsTable using the provided connection manager.
func NewPostgresNotificationsTable(cm *sqlutil.Connections) tables.NotificationTable {
	return &notificationsTable{
		cm:                              cm,
		insertNotificationSQL:           insertNotificationSQL,
		deleteNotificationsUpToSQL:      deleteNotificationsUpToSQL,
		updateNotificationReadSQL:       updateNotificationReadSQL,
		selectNotificationSQL:           selectNotificationSQL,
		selectNotificationCountSQL:      selectNotificationCountSQL,
		selectRoomNotificationCountsSQL: selectRoomNotificationCountsSQL,
		cleanNotificationsSQL:           cleanNotificationsSQL,
	}
}

// Insert inserts a notification into the database.
func (t *notificationsTable) Insert(ctx context.Context, localpart string, serverName spec.ServerName, eventID string, pos uint64, highlight bool, n *api.Notification) error {
	db := t.cm.Connection(ctx, false)
	roomID, tsMS := n.RoomID, n.TS
	nn := *n
	// Clears out fields that have their own columns to shrink the data and avoid inconsistency bugs.
	nn.RoomID = ""
	nn.TS, nn.Read = 0, false
	bs, err := json.Marshal(nn)
	if err != nil {
		return err
	}
	result := db.Exec(t.insertNotificationSQL, localpart, serverName, roomID, eventID, pos, tsMS, highlight, string(bs))
	return result.Error
}

// DeleteUpTo deletes all previous notifications, up to and including the event.
// It returns true if any rows were affected, false otherwise.
func (t *notificationsTable) DeleteUpTo(ctx context.Context, localpart string, serverName spec.ServerName, roomID string, pos uint64) (bool, error) {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteNotificationsUpToSQL, localpart, serverName, roomID, pos)
	// Return true if any rows were affected, false otherwise.
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// UpdateRead updates the "read" value for notifications up to a given stream position.
// It returns true if any rows were affected, false otherwise.
func (t *notificationsTable) UpdateRead(ctx context.Context, localpart string, serverName spec.ServerName, roomID string, pos uint64, v bool) (bool, error) {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.updateNotificationReadSQL, v, localpart, serverName, roomID, pos)
	// Return true if any rows were affected, false otherwise.
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// Select retrieves notifications for a user after a given notification ID.
// It returns a slice of notifications and the highest notification ID found.
func (t *notificationsTable) Select(ctx context.Context, localpart string, serverName spec.ServerName, fromID int64, limit int, filter tables.NotificationFilter) ([]*api.Notification, int64, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectNotificationSQL, localpart, serverName, fromID, filter, limit).Rows()
	if err != nil {
		return nil, 0, err
	}
	// Use internal.CloseAndLogIfError to ensure rows are closed and errors are logged.
	defer internal.CloseAndLogIfError(ctx, rows, "notifications.Select: rows.Close() failed")

	var maxID int64 = -1
	var notifs []*api.Notification
	for rows.Next() {
		var id int64
		var roomID string
		var ts spec.Timestamp
		var read bool
		var jsonStr string
		// Scan the row values.
		err = rows.Scan(&id, &roomID, &ts, &read, &jsonStr)
		if err != nil {
			return nil, 0, err
		}

		var n api.Notification
		// Unmarshal the notification JSON into the struct.
		err := json.Unmarshal([]byte(jsonStr), &n)
		if err != nil {
			return nil, 0, err
		}
		n.RoomID = roomID
		n.TS = ts
		n.Read = read
		notifs = append(notifs, &n)

		if maxID < id {
			maxID = id
		}
	}
	return notifs, maxID, rows.Err()
}

// SelectCount retrieves the count of unread notifications for a user.
func (t *notificationsTable) SelectCount(ctx context.Context, localpart string, serverName spec.ServerName, filter tables.NotificationFilter) (int64, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectNotificationCountSQL, localpart, serverName, filter).Row()
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// SelectRoomCounts retrieves notification counts for a user and room.
func (t *notificationsTable) SelectRoomCounts(ctx context.Context, localpart string, serverName spec.ServerName, roomID string) (int64, int64, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectRoomNotificationCountsSQL, localpart, serverName, roomID).Row()
	var total, highlights int64
	if err := row.Scan(&total, &highlights); err != nil {
		return 0, 0, err
	}
	return total, highlights, nil
}

// Clean removes old notifications based on highlight and timestamp.
func (t *notificationsTable) Clean(ctx context.Context) error {
	normalCutoff := time.Now().AddDate(0, 0, -1).UnixNano() / int64(time.Millisecond)
	highlightCutoff := time.Now().AddDate(0, -1, 0).UnixNano() / int64(time.Millisecond)
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.cleanNotificationsSQL, normalCutoff, highlightCutoff)
	return result.Error
}
