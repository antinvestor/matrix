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
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/util"
)

// notificationSchema defines the schema for notifications storage
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

// notificationSchemaRevert defines the revert operation for the notification schema
const notificationSchemaRevert = `
DROP INDEX IF EXISTS userapi_notification_localpart_id_idx;
DROP INDEX IF EXISTS userapi_notification_localpart_room_id_id_idx;
DROP INDEX IF EXISTS userapi_notification_localpart_room_id_event_id_idx;
DROP TABLE IF EXISTS userapi_notifications;
`

// SQL query constants
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

// notificationsTable represents a notifications table for user data
type notificationsTable struct {
	cm                           sqlutil.ConnectionManager
	insertNotification           string
	deleteNotificationsUpTo      string
	updateNotificationRead       string
	selectNotifications          string
	selectNotificationCount      string
	selectRoomNotificationCounts string
	cleanNotifications           string
}

// NewPostgresNotificationTable creates a new postgres notification table
func NewPostgresNotificationTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.NotificationTable, error) {
	// Perform schema migration first
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "userapi_notifications_table_schema_001",
		Patch:       notificationSchema,
		RevertPatch: notificationSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Create the table implementation after migration is successful
	s := &notificationsTable{
		cm:                           cm,
		insertNotification:           insertNotificationSQL,
		deleteNotificationsUpTo:      deleteNotificationsUpToSQL,
		updateNotificationRead:       updateNotificationReadSQL,
		selectNotifications:          selectNotificationSQL,
		selectNotificationCount:      selectNotificationCountSQL,
		selectRoomNotificationCounts: selectRoomNotificationCountsSQL,
		cleanNotifications:           cleanNotificationsSQL,
	}

	return s, nil
}

// Clean removes old notifications based on configurable timeframes
func (s *notificationsTable) Clean(ctx context.Context) error {
	db := s.cm.Connection(ctx, false)
	res := db.Exec(
		s.cleanNotifications,
		time.Now().AddDate(0, 0, -1).UnixNano()/int64(time.Millisecond), // keep non-highlights for a day
		time.Now().AddDate(0, -1, 0).UnixNano()/int64(time.Millisecond), // keep highlights for a month
	)
	return res.Error
}

// Insert inserts a notification into the database.
func (s *notificationsTable) Insert(ctx context.Context, localpart string, serverName spec.ServerName, eventID string, pos uint64, highlight bool, n *api.Notification) error {
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

	db := s.cm.Connection(ctx, false)
	res := db.Exec(s.insertNotification, localpart, serverName, roomID, eventID, pos, tsMS, highlight, string(bs))
	return res.Error
}

// DeleteUpTo deletes all previous notifications, up to and including the event.
func (s *notificationsTable) DeleteUpTo(ctx context.Context, localpart string, serverName spec.ServerName, roomID string, pos uint64) (affected bool, _ error) {
	db := s.cm.Connection(ctx, false)
	result := db.Exec(s.deleteNotificationsUpTo, localpart, serverName, roomID, pos)
	if result.Error != nil {
		return false, result.Error
	}

	util.Log(ctx).WithField("localpart", localpart).
		WithField("room_id", roomID).
		WithField("stream_pos", pos).
		Debug("DeleteUpTo: %d rows affected", result.RowsAffected)
	return result.RowsAffected > 0, nil
}

// UpdateRead updates the "read" value for an event.
func (s *notificationsTable) UpdateRead(ctx context.Context, localpart string, serverName spec.ServerName, roomID string, pos uint64, v bool) (affected bool, _ error) {
	db := s.cm.Connection(ctx, false)
	res := db.Exec(s.updateNotificationRead, v, localpart, serverName, roomID, pos)
	if res.Error != nil {
		return false, res.Error
	}

	util.Log(ctx).WithField("localpart", localpart).
		WithField("room_id", roomID).
		WithField("stream_pos", pos).
		Debug("UpdateRead: %d rows affected", res.RowsAffected)
	return res.RowsAffected > 0, nil
}

// Select retrieves notifications for a given user
func (s *notificationsTable) Select(ctx context.Context, localpart string, serverName spec.ServerName, fromID int64, limit int, filter tables.NotificationFilter) ([]*api.Notification, int64, error) {
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectNotifications, localpart, serverName, fromID, uint32(filter), limit).Rows()

	if err != nil {
		return nil, 0, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "notifications.Select: rows.Close() failed")

	var maxID int64 = -1
	var notifs []*api.Notification
	for rows.Next() {
		var id int64
		var roomID string
		var ts spec.Timestamp
		var read bool
		var jsonStr string
		err = rows.Scan(
			&id,
			&roomID,
			&ts,
			&read,
			&jsonStr)
		if err != nil {
			return nil, 0, err
		}

		var n api.Notification
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

// SelectCount returns the count of notifications for a given user
func (s *notificationsTable) SelectCount(ctx context.Context, localpart string, serverName spec.ServerName, filter tables.NotificationFilter) (count int64, err error) {
	db := s.cm.Connection(ctx, true)
	row := db.Raw(s.selectNotificationCount, localpart, serverName, uint32(filter)).Row()
	err = row.Scan(&count)
	return
}

// SelectRoomCounts returns the total and highlight counts for a given room
func (s *notificationsTable) SelectRoomCounts(ctx context.Context, localpart string, serverName spec.ServerName, roomID string) (total int64, highlight int64, err error) {
	db := s.cm.Connection(ctx, true)
	row := db.Raw(s.selectRoomNotificationCounts, localpart, serverName, roomID).Row()
	err = row.Scan(&total, &highlight)
	return
}
