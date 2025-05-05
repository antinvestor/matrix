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
	"github.com/lib/pq"

	"github.com/antinvestor/matrix/internal/eventutil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
)

const notificationDataSchema = `
CREATE TABLE IF NOT EXISTS syncapi_notification_data (
	id BIGSERIAL PRIMARY KEY,
	user_id TEXT NOT NULL,
	room_id TEXT NOT NULL,
	notification_count BIGINT NOT NULL DEFAULT 0,
	highlight_count BIGINT NOT NULL DEFAULT 0,
	CONSTRAINT syncapi_notification_data_unique UNIQUE (user_id, room_id)
);`

const notificationDataSchemaRevert = `DROP TABLE IF EXISTS syncapi_notification_data;`

const upsertRoomUnreadNotificationCountsSQL = `INSERT INTO syncapi_notification_data
  (user_id, room_id, notification_count, highlight_count)
  VALUES ($1, $2, $3, $4)
  ON CONFLICT (user_id, room_id)
  DO UPDATE SET id = nextval('syncapi_notification_data_id_seq'), notification_count = $3, highlight_count = $4
  RETURNING id`

const selectUserUnreadNotificationsForRooms = `SELECT room_id, notification_count, highlight_count
	FROM syncapi_notification_data
	WHERE user_id = $1 AND
	      room_id = ANY($2)`

const selectMaxNotificationIDSQL = `SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE MAX(id) END FROM syncapi_notification_data`

const purgeNotificationDataSQL = "DELETE FROM syncapi_notification_data WHERE room_id = $1"

// notificationDataTable implements tables.NotificationData using a connection manager and SQL constants.
// This table stores notification data for users and provides methods for upserting, querying, and purging notification data.
type notificationDataTable struct {
	cm                                *sqlutil.Connections
	upsertRoomUnreadCountsSQL         string
	selectUserUnreadCountsForRoomsSQL string
	selectMaxIDSQL                    string
	purgeNotificationDataSQL          string
}

// NewPostgresNotificationDataTable creates a new NotificationData table using a connection manager.
func NewPostgresNotificationDataTable(cm *sqlutil.Connections) tables.NotificationData {
	return &notificationDataTable{
		cm:                                cm,
		upsertRoomUnreadCountsSQL:         upsertRoomUnreadNotificationCountsSQL,
		selectUserUnreadCountsForRoomsSQL: selectUserUnreadNotificationsForRooms,
		selectMaxIDSQL:                    selectMaxNotificationIDSQL,
		purgeNotificationDataSQL:          purgeNotificationDataSQL,
	}
}

// UpsertRoomUnreadCounts stores unread counts for a room and user.
func (t *notificationDataTable) UpsertRoomUnreadCounts(ctx context.Context, userID, roomID string, notificationCount, highlightCount int) (pos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, false)
	err = db.Raw(t.upsertRoomUnreadCountsSQL, userID, roomID, notificationCount, highlightCount).Row().Scan(&pos)
	return
}

// SelectUserUnreadCountsForRooms returns unread counts for a user in a given set of rooms.
func (t *notificationDataTable) SelectUserUnreadCountsForRooms(
	ctx context.Context, userID string, roomIDs []string,
) (map[string]*eventutil.NotificationData, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectUserUnreadCountsForRoomsSQL, userID, pq.Array(roomIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

// SelectMaxID returns the maximum stream position for notification data.
func (t *notificationDataTable) SelectMaxID(ctx context.Context) (int64, error) {
	db := t.cm.Connection(ctx, true)
	var maxID int64
	err := db.Raw(t.selectMaxIDSQL).Row().Scan(&maxID)
	return maxID, err
}

// PurgeNotificationData removes all notification data for a given room.
func (t *notificationDataTable) PurgeNotificationData(ctx context.Context, roomID string) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.purgeNotificationDataSQL, roomID).Error
}
