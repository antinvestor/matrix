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
	"github.com/lib/pq"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/eventutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/pitabwire/frame"
)

// Schema for notification data table
const notificationDataSchema = `
CREATE TABLE IF NOT EXISTS syncapi_notification_data (
	id BIGSERIAL PRIMARY KEY,
	user_id TEXT NOT NULL,
	room_id TEXT NOT NULL,
	notification_count BIGINT NOT NULL DEFAULT 0,
	highlight_count BIGINT NOT NULL DEFAULT 0,
	CONSTRAINT syncapi_notification_data_unique UNIQUE (user_id, room_id)
);`

// Revert schema for notification data table
const notificationDataSchemaRevert = `
DROP TABLE IF EXISTS syncapi_notification_data;
`

// SQL query to upsert room unread notification counts
const upsertRoomUnreadNotificationCountsSQL = `
INSERT INTO syncapi_notification_data
  (user_id, room_id, notification_count, highlight_count)
  VALUES ($1, $2, $3, $4)
  ON CONFLICT (user_id, room_id)
  DO UPDATE SET id = nextval('syncapi_notification_data_id_seq'), notification_count = $3, highlight_count = $4
  RETURNING id
`

// SQL query to select user unread notifications for rooms
const selectUserUnreadNotificationsForRooms = `
SELECT room_id, notification_count, highlight_count
	FROM syncapi_notification_data
	WHERE user_id = $1 AND
	      room_id = ANY($2)
`

// SQL query to select max notification ID
const selectMaxNotificationIDSQL = `
SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE MAX(id) END FROM syncapi_notification_data
`

// SQL query to purge notification data
const purgeNotificationDataSQL = `
DELETE FROM syncapi_notification_data WHERE room_id = $1
`

// notificationDataTable implements tables.NotificationData
type notificationDataTable struct {
	cm                                    sqlutil.ConnectionManager
	upsertRoomUnreadNotificationCountsSQL string
	selectUserUnreadNotificationsForRooms string
	selectMaxNotificationIDSQL            string
	purgeNotificationDataSQL              string
}

// NewPostgresNotificationDataTable creates a new notification data table
func NewPostgresNotificationDataTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.NotificationData, error) {
	t := &notificationDataTable{
		cm:                                    cm,
		upsertRoomUnreadNotificationCountsSQL: upsertRoomUnreadNotificationCountsSQL,
		selectUserUnreadNotificationsForRooms: selectUserUnreadNotificationsForRooms,
		selectMaxNotificationIDSQL:            selectMaxNotificationIDSQL,
		purgeNotificationDataSQL:              purgeNotificationDataSQL,
	}

	// Perform the migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "syncapi_notification_data_table_schema_001",
		Patch:       notificationDataSchema,
		RevertPatch: notificationDataSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// UpsertRoomUnreadCounts updates or inserts unread counts for a room
func (t *notificationDataTable) UpsertRoomUnreadCounts(
	ctx context.Context, userID, roomID string, notificationCount, highlightCount int,
) (pos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, false)
	row := db.Raw(t.upsertRoomUnreadNotificationCountsSQL, userID, roomID, notificationCount, highlightCount).Row()
	err = row.Scan(&pos)
	return
}

// SelectUserUnreadCountsForRooms retrieves unread counts for multiple rooms
func (t *notificationDataTable) SelectUserUnreadCountsForRooms(
	ctx context.Context, userID string, roomIDs []string,
) (map[string]*eventutil.NotificationData, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectUserUnreadNotificationsForRooms, userID, pq.Array(roomIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectUserUnreadCountsForRooms: rows.close() failed")

	roomCounts := map[string]*eventutil.NotificationData{}
	var roomID string
	var notificationCount, highlightCount int
	for rows.Next() {
		if err = rows.Scan(&roomID, &notificationCount, &highlightCount); err != nil {
			return nil, err
		}

		roomCounts[roomID] = &eventutil.NotificationData{
			RoomID:                  roomID,
			UnreadNotificationCount: notificationCount,
			UnreadHighlightCount:    highlightCount,
		}
	}
	return roomCounts, rows.Err()
}

// SelectMaxID returns the maximum notification ID
func (t *notificationDataTable) SelectMaxID(ctx context.Context) (int64, error) {
	var id int64
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectMaxNotificationIDSQL).Row()
	err := row.Scan(&id)
	return id, err
}

// PurgeNotificationData purges all notification data for a room
func (t *notificationDataTable) PurgeNotificationData(
	ctx context.Context, roomID string,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.purgeNotificationDataSQL, roomID).Error
}
