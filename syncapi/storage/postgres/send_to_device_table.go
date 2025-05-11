// Copyright 2019-2020 The Matrix.org Foundation C.I.C.
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

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/pitabwire/frame"
	"github.com/sirupsen/logrus"
)

// Schema for send-to-device table
const sendToDeviceSchema = `
CREATE SEQUENCE IF NOT EXISTS syncapi_send_to_device_id;

-- Stores send-to-device messages.
CREATE TABLE IF NOT EXISTS syncapi_send_to_device (
	-- The ID that uniquely identifies this message.
	id BIGINT PRIMARY KEY DEFAULT nextval('syncapi_send_to_device_id'),
	-- The user ID to send the message to.
	user_id TEXT NOT NULL,
	-- The device ID to send the message to.
	device_id TEXT NOT NULL,
	-- The event content JSON.
	content TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS syncapi_send_to_device_user_id_device_id_idx ON syncapi_send_to_device(user_id, device_id);
`

// Revert schema for send-to-device table
const sendToDeviceSchemaRevert = `
DROP INDEX IF EXISTS syncapi_send_to_device_user_id_device_id_idx;
DROP TABLE IF EXISTS syncapi_send_to_device;
DROP SEQUENCE IF EXISTS syncapi_send_to_device_id;
`

// SQL query to insert a send-to-device message
const insertSendToDeviceMessageSQL = `
INSERT INTO syncapi_send_to_device (user_id, device_id, content)
  VALUES ($1, $2, $3)
  RETURNING id
`

// SQL query to select send-to-device messages
const selectSendToDeviceMessagesSQL = `
SELECT id, user_id, device_id, content
  FROM syncapi_send_to_device
  WHERE user_id = $1 AND device_id = $2 AND id > $3 AND id <= $4
  ORDER BY id ASC
`

// SQL query to delete send-to-device messages
const deleteSendToDeviceMessagesSQL = `
DELETE FROM syncapi_send_to_device
  WHERE user_id = $1 AND device_id = $2 AND id <= $3
`

// SQL query to select max send-to-device ID
const selectMaxSendToDeviceIDSQL = `
SELECT MAX(id) FROM syncapi_send_to_device
`

// sendToDeviceTable implements tables.SendToDevice
type sendToDeviceTable struct {
	cm                            sqlutil.ConnectionManager
	insertSendToDeviceMessageSQL  string
	selectSendToDeviceMessagesSQL string
	deleteSendToDeviceMessagesSQL string
	selectMaxSendToDeviceIDSQL    string
}

// NewPostgresSendToDeviceTable creates a new send-to-device table
func NewPostgresSendToDeviceTable(_ context.Context, cm sqlutil.ConnectionManager) (tables.SendToDevice, error) {
	// Perform the migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "syncapi_send_to_device_table_schema_001",
		Patch:       sendToDeviceSchema,
		RevertPatch: sendToDeviceSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	t := &sendToDeviceTable{
		cm:                            cm,
		insertSendToDeviceMessageSQL:  insertSendToDeviceMessageSQL,
		selectSendToDeviceMessagesSQL: selectSendToDeviceMessagesSQL,
		deleteSendToDeviceMessagesSQL: deleteSendToDeviceMessagesSQL,
		selectMaxSendToDeviceIDSQL:    selectMaxSendToDeviceIDSQL,
	}

	return t, nil
}

// InsertSendToDeviceMessage adds a new send-to-device message
func (t *sendToDeviceTable) InsertSendToDeviceMessage(
	ctx context.Context, userID, deviceID, content string,
) (pos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, false)
	row := db.Raw(t.insertSendToDeviceMessageSQL, userID, deviceID, content).Row()
	err = row.Scan(&pos)
	return
}

// SelectSendToDeviceMessages retrieves send-to-device messages
func (t *sendToDeviceTable) SelectSendToDeviceMessages(
	ctx context.Context, userID, deviceID string, from, to types.StreamPosition,
) (lastPos types.StreamPosition, events []types.SendToDeviceEvent, err error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectSendToDeviceMessagesSQL, userID, deviceID, from, to).Rows()
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectSendToDeviceMessages: rows.close() failed")

	for rows.Next() {
		var id types.StreamPosition
		var userID, deviceID, content string
		if err = rows.Scan(&id, &userID, &deviceID, &content); err != nil {
			return
		}
		event := types.SendToDeviceEvent{
			ID:       id,
			UserID:   userID,
			DeviceID: deviceID,
		}
		if err = json.Unmarshal([]byte(content), &event.SendToDeviceEvent); err != nil {
			logrus.WithError(err).Errorf("Failed to unmarshal send-to-device message")
			continue
		}
		if id > lastPos {
			lastPos = id
		}
		events = append(events, event)
	}
	if lastPos == 0 {
		lastPos = to
	}
	return lastPos, events, rows.Err()
}

// DeleteSendToDeviceMessages removes send-to-device messages
func (t *sendToDeviceTable) DeleteSendToDeviceMessages(
	ctx context.Context, userID, deviceID string, pos types.StreamPosition,
) (err error) {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteSendToDeviceMessagesSQL, userID, deviceID, pos).Error
}

// SelectMaxSendToDeviceMessageID retrieves the maximum send-to-device message ID
func (t *sendToDeviceTable) SelectMaxSendToDeviceMessageID(
	ctx context.Context,
) (id int64, err error) {
	var nullableID sql.NullInt64
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectMaxSendToDeviceIDSQL).Row()
	err = row.Scan(&nullableID)
	if nullableID.Valid {
		id = nullableID.Int64
	}
	return
}
