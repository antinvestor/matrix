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

const purgeNotificationDataSQL = "" +
	"DELETE FROM syncapi_notification_data WHERE room_id = $1"

type notificationDataTable struct {
	cm                                    *sqlutil.Connections
	upsertRoomUnreadNotificationCountsSQL string
	selectUserUnreadNotificationsForRoomsSQL string
	selectMaxNotificationIDSQL            string
	purgeNotificationDataSQL              string
}

func NewPostgresNotificationDataTable(ctx context.Context, cm *sqlutil.Connections) (tables.NotificationData, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(notificationDataSchema).Error; err != nil {
		return nil, err
	}
	
	// Initialize the table with SQL statements
	r := &notificationDataTable{
		cm:                                    cm,
		upsertRoomUnreadNotificationCountsSQL: upsertRoomUnreadNotificationCountsSQL,
		selectUserUnreadNotificationsForRoomsSQL: selectUserUnreadNotificationsForRooms,
		selectMaxNotificationIDSQL:            selectMaxNotificationIDSQL,
		purgeNotificationDataSQL:              purgeNotificationDataSQL,
	}
	return r, nil
}

func (r *notificationDataTable) UpsertRoomUnreadCounts(
	ctx context.Context, userID, roomID string, notificationCount, highlightCount int,
) (pos types.StreamPosition, err error) {
	// Get database connection
	db := r.cm.Connection(ctx, false)
	
	row := db.Raw(r.upsertRoomUnreadNotificationCountsSQL, userID, roomID, notificationCount, highlightCount).Row()
	err = row.Scan(&pos)
	return
}

func (r *notificationDataTable) SelectUserUnreadCountsForRooms(
	ctx context.Context, userID string, roomIDs []string,
) (map[string]*eventutil.NotificationData, error) {
	// Get database connection
	db := r.cm.Connection(ctx, true)
	
	rows, err := db.Raw(r.selectUserUnreadNotificationsForRoomsSQL, userID, pq.Array(roomIDs)).Rows()
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

func (r *notificationDataTable) SelectMaxID(ctx context.Context) (int64, error) {
	// Get database connection
	db := r.cm.Connection(ctx, true)
	
	var id int64
	err := db.Raw(r.selectMaxNotificationIDSQL).Scan(&id).Error
	return id, err
}

func (r *notificationDataTable) PurgeNotificationData(
	ctx context.Context, roomID string,
) error {
	// Get database connection
	db := r.cm.Connection(ctx, false)
	
	return db.Exec(r.purgeNotificationDataSQL, roomID).Error
}
