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

	"golang.org/x/oauth2"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/userutil"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/lib/pq"
)

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
    -- The Matrix user ID localpart for this device. This is preferable to storing the full user_id
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

// SQL query constants for device operations
const (
	// insertDeviceSQL inserts a new device with provided details
	insertDeviceSQL = "INSERT INTO userapi_devices(device_id, localpart, server_name, access_token, extra_data, created_ts, display_name, last_seen_ts, ip, user_agent) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING session_id"
	
	// selectDeviceByTokenSQL selects device information by access token
	selectDeviceByTokenSQL = "SELECT session_id, device_id, localpart, server_name FROM userapi_devices WHERE access_token = $1"
	
	// selectDeviceByIDSQL selects device details by user and device ID
	selectDeviceByIDSQL = "SELECT display_name, last_seen_ts, ip FROM userapi_devices WHERE localpart = $1 AND server_name = $2 AND device_id = $3"
	
	// selectDevicesByLocalpartSQL selects all devices for a user except the specified device ID
	selectDevicesByLocalpartSQL = "SELECT device_id, display_name, last_seen_ts, ip, user_agent, session_id FROM userapi_devices WHERE localpart = $1 AND server_name = $2 AND device_id != $3 ORDER BY last_seen_ts DESC"
	
	// updateDeviceNameSQL updates device display name
	updateDeviceNameSQL = "UPDATE userapi_devices SET display_name = $1 WHERE localpart = $2 AND server_name = $3 AND device_id = $4"
	
	// deleteDeviceSQL deletes a specific device
	deleteDeviceSQL = "DELETE FROM userapi_devices WHERE device_id = $1 AND localpart = $2 AND server_name = $3"
	
	// deleteDevicesByLocalpartSQL deletes all devices for a user except the specified device ID
	deleteDevicesByLocalpartSQL = "DELETE FROM userapi_devices WHERE localpart = $1 AND server_name = $2 AND device_id != $3"
	
	// deleteDevicesSQL deletes multiple devices by IDs
	deleteDevicesSQL = "DELETE FROM userapi_devices WHERE localpart = $1 AND server_name = $2 AND device_id = ANY($3)"
	
	// selectDevicesByIDSQL selects devices by multiple device IDs
	selectDevicesByIDSQL = "SELECT device_id, localpart, server_name, display_name, last_seen_ts, session_id FROM userapi_devices WHERE device_id = ANY($1) ORDER BY last_seen_ts DESC"
	
	// updateDeviceLastSeenSQL updates device last seen timestamp and related info
	updateDeviceLastSeenSQL = "UPDATE userapi_devices SET last_seen_ts = $1, ip = $2, user_agent = $3 WHERE localpart = $4 AND server_name = $5 AND device_id = $6"
)

type devicesTable struct {
	cm         *sqlutil.Connections
	serverName spec.ServerName
	
	// SQL queries stored as direct fields for better maintainability
	insertDeviceStmt             string
	selectDeviceByTokenStmt      string
	selectDeviceByIDStmt         string
	selectDevicesByLocalpartStmt string
	updateDeviceNameStmt         string
	deleteDeviceStmt             string
	deleteDevicesByLocalpartStmt string
	deleteDevicesStmt            string
	selectDevicesByIDStmt        string
	updateDeviceLastSeenStmt     string
}

func NewPostgresDevicesTable(ctx context.Context, cm *sqlutil.Connections, serverName spec.ServerName) (tables.DevicesTable, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(devicesSchema).Error; err != nil {
		return nil, err
	}

	// Initialize migrator
	m := sqlutil.NewMigrator(db.DB())
	if err := m.Up(ctx); err != nil {
		return nil, err
	}

	// Initialize table with SQL statements
	t := &devicesTable{
		cm:         cm,
		serverName: serverName,
		insertDeviceStmt:             insertDeviceSQL,
		selectDeviceByTokenStmt:      selectDeviceByTokenSQL,
		selectDeviceByIDStmt:         selectDeviceByIDSQL,
		selectDevicesByLocalpartStmt: selectDevicesByLocalpartSQL,
		updateDeviceNameStmt:         updateDeviceNameSQL,
		deleteDeviceStmt:             deleteDeviceSQL,
		deleteDevicesByLocalpartStmt: deleteDevicesByLocalpartSQL,
		deleteDevicesStmt:            deleteDevicesSQL,
		selectDevicesByIDStmt:        selectDevicesByIDSQL,
		updateDeviceLastSeenStmt:     updateDeviceLastSeenSQL,
	}

	return t, nil
}

// InsertDevice creates a new device. Returns an error if any device with the same access token already exists.
// Returns an error if the user already has a device with the given device ID.
// Returns the device on success.
func (t *devicesTable) InsertDevice(
	ctx context.Context, id string,
	localpart string, serverName spec.ServerName,
	accessToken string, extraData *oauth2.Token, displayName *string, ipAddr, userAgent string,
) (*api.Device, error) {
	createdTimeMS := time.Now().UnixNano() / 1000000
	var sessionID int64

	extraDataJson, err := json.Marshal(extraData)
	if err != nil {
		return nil, fmt.Errorf("insertDevice: %w", err)
	}

	db := t.cm.Connection(ctx, false)

	row := db.Raw(t.insertDeviceStmt, id, localpart, serverName, accessToken, extraDataJson, createdTimeMS, displayName, createdTimeMS, ipAddr, userAgent).Row()
	if err := row.Scan(&sessionID); err != nil {
		return nil, fmt.Errorf("insertDevice: %w", err)
	}

	dev := &api.Device{
		ID:          id,
		UserID:      userutil.MakeUserID(localpart, serverName),
		AccessToken: accessToken,
		SessionID:   sessionID,
		LastSeenTS:  createdTimeMS,
		LastSeenIP:  ipAddr,
		UserAgent:   userAgent,
	}
	if displayName != nil {
		dev.DisplayName = *displayName
	}
	return dev, nil
}

// insertDeviceWithSessionID creates a new device. Returns an error if any device with the same access token already exists.
// Returns an error if the user already has a device with the given device ID.
// Returns the device on success. This method differs from InsertDevice in that it allows for the session ID to be specified
// This should only be used in exceptional circumstances, like recreating devices from other servers.
func (t *devicesTable) InsertDeviceWithSessionID(
	ctx context.Context, id string,
	localpart string, serverName spec.ServerName,
	accessToken string, sessionID int64, displayName *string, ipAddr, userAgent string,
) (*api.Device, error) {
	// Reuse the old session ID since we're recreating this device.
	createdTimeMS := time.Now().UnixNano() / 1000000

	db := t.cm.Connection(ctx, false)

	err := db.Exec("INSERT INTO userapi_devices(device_id, localpart, server_name, access_token, created_ts, display_name, session_id, last_seen_ts, ip, user_agent) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		id, localpart, serverName, accessToken, createdTimeMS, displayName, sessionID, createdTimeMS, ipAddr, userAgent).Error
	if err != nil {
		return nil, fmt.Errorf("insertDeviceWithSessionID: %w", err)
	}

	dev := &api.Device{
		ID:          id,
		UserID:      userutil.MakeUserID(localpart, serverName),
		AccessToken: accessToken,
		SessionID:   sessionID,
		LastSeenTS:  createdTimeMS,
		LastSeenIP:  ipAddr,
		UserAgent:   userAgent,
	}
	if displayName != nil {
		dev.DisplayName = *displayName
	}
	return dev, nil
}

// DeleteDevice removes a single device by id and user localpart.
func (t *devicesTable) DeleteDevice(
	ctx context.Context, id string,
	localpart string, serverName spec.ServerName,
) error {
	db := t.cm.Connection(ctx, false)
	
	return db.Exec(t.deleteDeviceStmt, id, localpart, serverName).Error
}

// deleteDevices removes a single or multiple devices by ids and user localpart.
// Returns an error if the execution failed.
func (t *devicesTable) DeleteDevices(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	devices []string,
) error {
	db := t.cm.Connection(ctx, false)
	
	return db.Exec(t.deleteDevicesStmt, localpart, serverName, pq.Array(devices)).Error
}

// deleteDevicesByLocalpart removes all devices for the
// given user localpart.
func (t *devicesTable) DeleteDevicesByLocalpart(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	exceptDeviceID string,
) error {
	db := t.cm.Connection(ctx, false)
	
	return db.Exec(t.deleteDevicesByLocalpartStmt, localpart, serverName, exceptDeviceID).Error
}

func (t *devicesTable) UpdateDeviceName(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	deviceID string, displayName *string,
) error {
	db := t.cm.Connection(ctx, false)
	
	return db.Exec(t.updateDeviceNameStmt, displayName, localpart, serverName, deviceID).Error
}

func (t *devicesTable) SelectDeviceByToken(
	ctx context.Context, accessToken string,
) (*api.Device, error) {
	var dev api.Device
	var localpart string
	var serverName string
	
	db := t.cm.Connection(ctx, true)
	
	row := db.Raw(t.selectDeviceByTokenStmt, accessToken).Row()
	if err := row.Scan(&dev.SessionID, &dev.ID, &localpart, &serverName); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selectDeviceByToken: %w", err)
	}
	
	dev.UserID = userutil.MakeUserID(localpart, spec.ServerName(serverName))
	dev.AccessToken = accessToken
	
	return &dev, nil
}

// selectDeviceByID retrieves a device from the database with the given user
// localpart and deviceID
func (t *devicesTable) SelectDeviceByID(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	deviceID string,
) (*api.Device, error) {
	var dev api.Device
	var displayname sql.NullString
	var ipAddr, userAgent sql.NullString
	
	db := t.cm.Connection(ctx, true)
	
	row := db.Raw(t.selectDeviceByIDStmt, localpart, serverName, deviceID).Row()
	if err := row.Scan(&displayname, &dev.LastSeenTS, &ipAddr); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selectDeviceByID: %w", err)
	}
	
	dev.ID = deviceID
	dev.UserID = userutil.MakeUserID(localpart, serverName)
	
	if displayname.Valid {
		dev.DisplayName = displayname.String
	}
	if ipAddr.Valid {
		dev.LastSeenIP = ipAddr.String
	}
	if userAgent.Valid {
		dev.UserAgent = userAgent.String
	}
	
	return &dev, nil
}

func (t *devicesTable) SelectDevicesByID(ctx context.Context, deviceIDs []string) ([]api.Device, error) {
	db := t.cm.Connection(ctx, true)
	
	rows, err := db.Raw(t.selectDevicesByIDStmt, pq.Array(deviceIDs)).Rows()
	if err != nil {
		return nil, fmt.Errorf("selectDevicesByID: %w", err)
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectDevicesByID: rows.close() failed")
	
	var devices []api.Device
	var sessionID int64
	for rows.Next() {
		var dev api.Device
		var localpart, serverName string
		var displayName sql.NullString
		
		if err := rows.Scan(&dev.ID, &localpart, &serverName, &displayName, &dev.LastSeenTS, &sessionID); err != nil {
			return nil, fmt.Errorf("selectDevicesByID: %w", err)
		}
		
		dev.UserID = userutil.MakeUserID(localpart, spec.ServerName(serverName))
		if displayName.Valid {
			dev.DisplayName = displayName.String
		}
		
		devices = append(devices, dev)
	}
	
	return devices, rows.Err()
}

func (t *devicesTable) SelectDevicesByLocalpart(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	exceptDeviceID string,
) ([]api.Device, error) {
	db := t.cm.Connection(ctx, true)
	
	rows, err := db.Raw(t.selectDevicesByLocalpartStmt, localpart, serverName, exceptDeviceID).Rows()
	if err != nil {
		return nil, fmt.Errorf("selectDevicesByLocalpart: %w", err)
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectDevicesByLocalpart: rows.close() failed")
	
	var devices []api.Device
	var sessionID int64
	for rows.Next() {
		var dev api.Device
		var displayName sql.NullString
		var ipAddr, userAgent sql.NullString
		
		if err := rows.Scan(&dev.ID, &displayName, &dev.LastSeenTS, &ipAddr, &userAgent, &sessionID); err != nil {
			return nil, fmt.Errorf("selectDevicesByLocalpart: %w", err)
		}
		
		dev.UserID = userutil.MakeUserID(localpart, serverName)
		dev.SessionID = sessionID
		
		if displayName.Valid {
			dev.DisplayName = displayName.String
		}
		if ipAddr.Valid {
			dev.LastSeenIP = ipAddr.String
		}
		if userAgent.Valid {
			dev.UserAgent = userAgent.String
		}
		
		devices = append(devices, dev)
	}
	
	return devices, rows.Err()
}

func (t *devicesTable) UpdateDeviceLastSeen(ctx context.Context, localpart string, serverName spec.ServerName, deviceID, ipAddr, userAgent string) error {
	lastSeenTimeMS := time.Now().UnixNano() / 1000000
	
	db := t.cm.Connection(ctx, false)
	
	return db.Exec(t.updateDeviceLastSeenStmt, lastSeenTimeMS, ipAddr, userAgent, localpart, serverName, deviceID).Error
}
