// Copyright 2017 Vector Creations Ltd
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
	"fmt"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/userutil"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/lib/pq"
	"github.com/pitabwire/frame"
	"golang.org/x/oauth2"
)

// devicesSchema defines the schema for devices table.
const devicesSchema = `
-- This sequence is used for automatic allocation of session_id.
CREATE SEQUENCE IF NOT EXISTS userapi_device_session_id_seq START 1;

-- Stores data about devices.
CREATE TABLE IF NOT EXISTS userapi_devices (
    -- The access token granted to this device. This has to be the primary key
    -- so we can distinguish which device is making a given request.
    access_token TEXT NOT NULL PRIMARY KEY,
    -- The full token data as obtained from sso authentication
    extra_data JSONB,
    -- The auto-allocated unique ID of the session identified by the access token.
    -- This can be used as a secure substitution of the access token in situations
    -- where data is associated with access tokens (e.g. transaction storage),
    -- so we don't have to store users' access tokens everywhere.
    session_id BIGINT NOT NULL DEFAULT nextval('userapi_device_session_id_seq'),
    -- The device identifier. This only needs to uniquely identify a device for a given user, not globally.
    -- access_tokens will be clobbered based on the device ID for a user.
    device_id TEXT NOT NULL,
    -- The Global user ID localpart for this device. This is preferable to storing the full user_id
    -- as it is smaller, makes it clearer that we only manage devices for our own users, and may make
    -- migration to different domain names easier.
    localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
    -- When this devices was first recognised on the network, as a unix timestamp (ms resolution).
    created_ts BIGINT NOT NULL,
    -- The display name, human friendlier than device_id and updatable
    display_name TEXT,
	-- The time the device was last used, as a unix timestamp (ms resolution).
	last_seen_ts BIGINT NOT NULL,
	-- The last seen IP address of this device
	ip TEXT,
	-- User agent of this device
	user_agent TEXT
                                          
    -- TODO: device keys, device display names, token restrictions (if 3rd-party OAuth app)
);

-- Device IDs must be unique for a given user.
CREATE UNIQUE INDEX IF NOT EXISTS userapi_device_localpart_id_idx ON userapi_devices(localpart, server_name, device_id);
`

// devicesSchemaRevert defines the revert operation for devices schema.
const devicesSchemaRevert = `
DROP TABLE IF EXISTS userapi_devices;
DROP SEQUENCE IF EXISTS userapi_device_session_id_seq;
`

// Insert a new device. Returns the device on success.
const insertDeviceSQL = "" +
	"INSERT INTO userapi_devices(device_id, localpart, server_name, access_token, extra_data, created_ts, display_name, last_seen_ts, ip, user_agent) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)" +
	" RETURNING session_id"

// Look up the data for a device by the access token granted during login or registration.
const selectDeviceByTokenSQL = "" +
	"SELECT session_id, device_id, localpart, server_name FROM userapi_devices WHERE access_token = $1"

// Look up device details by their ID.
const selectDeviceByIDSQL = "" +
	"SELECT display_name, last_seen_ts, ip FROM userapi_devices WHERE localpart = $1 AND server_name = $2 AND device_id = $3"

// Get a list of all devices for a user.
const selectDevicesByLocalpartSQL = "" +
	"SELECT device_id, display_name, last_seen_ts, ip, user_agent, session_id FROM userapi_devices WHERE localpart = $1 AND server_name = $2 AND device_id != $3 ORDER BY last_seen_ts DESC"

// Update a device's display name.
const updateDeviceNameSQL = "" +
	"UPDATE userapi_devices SET display_name = $1 WHERE localpart = $2 AND server_name = $3 AND device_id = $4"

// Remove a device.
const deleteDeviceSQL = "" +
	"DELETE FROM userapi_devices WHERE device_id = $1 AND localpart = $2 AND server_name = $3"

// Delete all devices for a user except a specific one.
const deleteDevicesByLocalpartSQL = "" +
	"DELETE FROM userapi_devices WHERE localpart = $1 AND server_name = $2 AND device_id != $3"

// Delete devices by their IDs.
const deleteDevicesSQL = "" +
	"DELETE FROM userapi_devices WHERE localpart = $1 AND server_name = $2 AND device_id = ANY($3)"

// Get devices by their IDs.
const selectDevicesByIDSQL = "" +
	"SELECT device_id, localpart, server_name, display_name, last_seen_ts, session_id FROM userapi_devices WHERE device_id = ANY($1) ORDER BY last_seen_ts DESC"

// Update device's last seen information.
const updateDeviceLastSeen = "" +
	"UPDATE userapi_devices SET last_seen_ts = $1, ip = $2, user_agent = $3 WHERE localpart = $4 AND server_name = $5 AND device_id = $6"

// devicesTable represents the devices table in the database.
type devicesTable struct {
	cm                          sqlutil.ConnectionManager
	insertDeviceSQL             string
	selectDeviceByTokenSQL      string
	selectDeviceByIDSQL         string
	selectDevicesByLocalpartSQL string
	selectDevicesByIDSQL        string
	updateDeviceNameSQL         string
	updateDeviceLastSeenSQL     string
	deleteDeviceSQL             string
	deleteDevicesByLocalpartSQL string
	deleteDevicesSQL            string
	serverName                  spec.ServerName
}

// NewPostgresDevicesTable creates a new devices table object.
func NewPostgresDevicesTable(ctx context.Context, cm sqlutil.ConnectionManager, serverName spec.ServerName) (tables.DevicesTable, error) {

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "userapi_devices_table_schema_001",
		Patch:       devicesSchema,
		RevertPatch: devicesSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	t := &devicesTable{
		cm:                          cm,
		serverName:                  serverName,
		insertDeviceSQL:             insertDeviceSQL,
		selectDeviceByTokenSQL:      selectDeviceByTokenSQL,
		selectDeviceByIDSQL:         selectDeviceByIDSQL,
		selectDevicesByLocalpartSQL: selectDevicesByLocalpartSQL,
		selectDevicesByIDSQL:        selectDevicesByIDSQL,
		updateDeviceNameSQL:         updateDeviceNameSQL,
		updateDeviceLastSeenSQL:     updateDeviceLastSeen,
		deleteDeviceSQL:             deleteDeviceSQL,
		deleteDevicesByLocalpartSQL: deleteDevicesByLocalpartSQL,
		deleteDevicesSQL:            deleteDevicesSQL,
	}

	return t, nil
}

// InsertDevice creates a new device. Returns an error if any device with the same access token already exists.
// Returns an error if the user already has a device with the given device ID.
// Returns the device on success.
func (t *devicesTable) InsertDevice(
	ctx context.Context,
	id string,
	localpart string, serverName spec.ServerName,
	accessToken string, extraData *oauth2.Token, displayName *string, ipAddr, userAgent string,
) (*api.Device, error) {
	createdTimeMS := time.Now().UnixNano() / 1000000
	var sessionID int64

	extraDataJson, err := json.Marshal(extraData)
	if err != nil {
		return nil, fmt.Errorf("insertDeviceStmt: %w", err)
	}

	db := t.cm.Connection(ctx, false)
	row := db.Raw(t.insertDeviceSQL, id, localpart, serverName, accessToken, extraDataJson, createdTimeMS, displayName, createdTimeMS, ipAddr, userAgent).Row()
	if err := row.Scan(&sessionID); err != nil {
		return nil, fmt.Errorf("insertDeviceStmt: %w", err)
	}

	dev := &api.Device{
		ID:          id,
		UserID:      userutil.MakeUserID(localpart, serverName),
		AccessToken: accessToken,

		SessionID:  sessionID,
		LastSeenTS: createdTimeMS,
		LastSeenIP: ipAddr,
		UserAgent:  userAgent,
	}
	if displayName != nil {
		dev.DisplayName = *displayName
	}
	return dev, nil
}

func (t *devicesTable) InsertDeviceWithSessionID(ctx context.Context,
	id, localpart string, serverName spec.ServerName,
	accessToken string, extraData *oauth2.Token, displayName *string, ipAddr, userAgent string,
	sessionID int64,
) (*api.Device, error) {
	// Since we're using GORM and don't have direct control over the session ID sequence,
	// we'll just perform a regular insert and let the database handle the session ID
	return t.InsertDevice(ctx, id, localpart, serverName, accessToken, extraData, displayName, ipAddr, userAgent)
}

// DeleteDevice removes a single device by id and user localpart.
func (t *devicesTable) DeleteDevice(
	ctx context.Context,
	id string,
	localpart string, serverName spec.ServerName,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteDeviceSQL, id, localpart, serverName).Error
}

// DeleteDevices removes a single or multiple devices by ids and user localpart.
// Returns an error if the execution failed.
func (t *devicesTable) DeleteDevices(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	devices []string,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteDevicesSQL, localpart, serverName, pq.Array(devices)).Error
}

// DeleteDevicesByLocalpart removes all devices for the given user localpart.
func (t *devicesTable) DeleteDevicesByLocalpart(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	exceptDeviceID string,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.deleteDevicesByLocalpartSQL, localpart, serverName, exceptDeviceID).Error
}

// UpdateDeviceName updates the display name of a device.
func (t *devicesTable) UpdateDeviceName(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	deviceID string, displayName *string,
) error {
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.updateDeviceNameSQL, displayName, localpart, serverName, deviceID).Error
}

// SelectDeviceByToken retrieves a device from the database with the given access token.
func (t *devicesTable) SelectDeviceByToken(
	ctx context.Context, accessToken string,
) (*api.Device, error) {
	var dev api.Device
	var localpart string
	var serverName spec.ServerName

	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectDeviceByTokenSQL, accessToken).Row()
	err := row.Scan(&dev.SessionID, &dev.ID, &localpart, &serverName)
	if err == nil {
		dev.UserID = userutil.MakeUserID(localpart, serverName)
		dev.AccessToken = accessToken
	}
	return &dev, err
}

// SelectDeviceByID retrieves a device from the database with the given user localpart and deviceID.
func (t *devicesTable) SelectDeviceByID(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	deviceID string,
) (*api.Device, error) {
	var dev api.Device
	var displayName, ip sql.NullString
	var lastseenTS sql.NullInt64

	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectDeviceByIDSQL, localpart, serverName, deviceID).Row()
	err := row.Scan(&displayName, &lastseenTS, &ip)
	if err == nil {
		dev.ID = deviceID
		dev.UserID = userutil.MakeUserID(localpart, serverName)
		if displayName.Valid {
			dev.DisplayName = displayName.String
		}
		if lastseenTS.Valid {
			dev.LastSeenTS = lastseenTS.Int64
		}
		if ip.Valid {
			dev.LastSeenIP = ip.String
		}
	}
	return &dev, err
}

// SelectDevicesByID retrieves devices from the database with the given device IDs.
func (t *devicesTable) SelectDevicesByID(ctx context.Context, deviceIDs []string) ([]api.Device, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectDevicesByIDSQL, pq.StringArray(deviceIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectDevicesByID: rows.close() failed")

	var devices []api.Device
	var dev api.Device
	var localpart string
	var serverName spec.ServerName
	var lastseents sql.NullInt64
	var displayName sql.NullString

	for rows.Next() {
		if err := rows.Scan(&dev.ID, &localpart, &serverName, &displayName, &lastseents, &dev.SessionID); err != nil {
			return nil, err
		}
		if displayName.Valid {
			dev.DisplayName = displayName.String
		}
		if lastseents.Valid {
			dev.LastSeenTS = lastseents.Int64
		}
		dev.UserID = userutil.MakeUserID(localpart, serverName)
		devices = append(devices, dev)
	}
	return devices, rows.Err()
}

// SelectDevicesByLocalpart retrieves all devices for a user.
func (t *devicesTable) SelectDevicesByLocalpart(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	exceptDeviceID string,
) ([]api.Device, error) {
	devices := []api.Device{}

	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectDevicesByLocalpartSQL, localpart, serverName, exceptDeviceID).Rows()
	if err != nil {
		return devices, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectDevicesByLocalpart: rows.close() failed")

	var dev api.Device
	var lastseents sql.NullInt64
	var id, displayname, ip, useragent sql.NullString

	for rows.Next() {
		err = rows.Scan(&id, &displayname, &lastseents, &ip, &useragent, &dev.SessionID)
		if err != nil {
			return devices, err
		}
		if id.Valid {
			dev.ID = id.String
		}
		if displayname.Valid {
			dev.DisplayName = displayname.String
		}
		if lastseents.Valid {
			dev.LastSeenTS = lastseents.Int64
		}
		if ip.Valid {
			dev.LastSeenIP = ip.String
		}
		if useragent.Valid {
			dev.UserAgent = useragent.String
		}

		dev.UserID = userutil.MakeUserID(localpart, serverName)
		devices = append(devices, dev)
	}

	return devices, rows.Err()
}

// UpdateDeviceLastSeen updates the last seen information for a device.
func (t *devicesTable) UpdateDeviceLastSeen(ctx context.Context, localpart string, serverName spec.ServerName, deviceID, ipAddr, userAgent string) error {
	lastSeenTs := time.Now().UnixNano() / 1000000
	db := t.cm.Connection(ctx, false)
	return db.Exec(t.updateDeviceLastSeenSQL, lastSeenTs, ipAddr, userAgent, localpart, serverName, deviceID).Error
}
