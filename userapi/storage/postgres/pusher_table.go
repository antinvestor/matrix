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
	"github.com/antinvestor/matrix/internal/sqlutil"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

// See https://matrix.org/docs/spec/client_server/r0.6.1#get-matrix-client-r0-pushers
const pushersSchema = `
CREATE TABLE IF NOT EXISTS userapi_pushers (
	id BIGSERIAL PRIMARY KEY,
	-- The Global user ID localpart for this pusher
	localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
	session_id BIGINT DEFAULT NULL,
	profile_tag TEXT,
	kind TEXT NOT NULL,
	app_id TEXT NOT NULL,
	app_display_name TEXT NOT NULL,
	device_display_name TEXT NOT NULL,
	pushkey TEXT NOT NULL,
	pushkey_ts_ms BIGINT NOT NULL DEFAULT 0,
	lang TEXT NOT NULL,
	data TEXT NOT NULL
);

-- For faster deleting by app_id, pushkey pair.
CREATE INDEX IF NOT EXISTS userapi_pusher_app_id_pushkey_idx ON userapi_pushers(app_id, pushkey);

-- For faster retrieving by localpart.
CREATE INDEX IF NOT EXISTS userapi_pusher_localpart_idx ON userapi_pushers(localpart, server_name);

-- Pushkey must be unique for a given user and app.
CREATE UNIQUE INDEX IF NOT EXISTS userapi_pusher_app_id_pushkey_localpart_idx ON userapi_pushers(app_id, pushkey, localpart, server_name);
`

const pushersSchemaRevert = "DROP TABLE IF EXISTS userapi_pushers CASCADE;"

// SQL: Insert a new pusher
const insertPusherSQL = "INSERT INTO userapi_pushers (localpart, server_name, session_id, pushkey, pushkey_ts_ms, kind, app_id, app_display_name, device_display_name, profile_tag, lang, data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"

// SQL: Select pushers for a user
const selectPushersSQL = "SELECT session_id, pushkey, pushkey_ts_ms, kind, app_id, app_display_name, device_display_name, profile_tag, lang, data FROM userapi_pushers WHERE localpart = $1 AND server_name = $2"

// SQL: Delete a pusher by appid, pushkey, localpart, and server name
const deletePusherSQL = "DELETE FROM userapi_pushers WHERE app_id = $1 AND pushkey = $2 AND localpart = $3 AND server_name = $4"

// SQL: Delete pushers by appid and pushkey
const deletePushersByAppIdAndPushKeySQL = "DELETE FROM userapi_pushers WHERE app_id = $1 AND pushkey = $2"

// pushersTable implements tables.PusherTable using GORM and a connection manager.
type pushersTable struct {
	cm *sqlutil.Connections

	insertPusherSQL                   string
	selectPushersSQL                  string
	deletePusherSQL                   string
	deletePushersByAppIdAndPushKeySQL string
}

// NewPostgresPusherTable returns a new PusherTable using the provided connection manager.
func NewPostgresPusherTable(cm *sqlutil.Connections) tables.PusherTable {
	return &pushersTable{
		cm:                                cm,
		insertPusherSQL:                   insertPusherSQL,
		selectPushersSQL:                  selectPushersSQL,
		deletePusherSQL:                   deletePusherSQL,
		deletePushersByAppIdAndPushKeySQL: deletePushersByAppIdAndPushKeySQL,
	}
}

// InsertPusher creates a new pusher.
// Returns an error if the user already has a pusher with the given pusher pushkey.
// Returns nil error on success.
func (t *pushersTable) InsertPusher(ctx context.Context, session_id int64, pushkey string, pushkeyTS int64, kind api.PusherKind, appid, appdisplayname, devicedisplayname, profiletag, lang, data, localpart string, serverName spec.ServerName) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.insertPusherSQL, localpart, serverName, session_id, pushkey, pushkeyTS, kind, appid, appdisplayname, devicedisplayname, profiletag, lang, data)
	return result.Error
}

// SelectPushers retrieves all pushers for a user.
func (t *pushersTable) SelectPushers(ctx context.Context, localpart string, serverName spec.ServerName) ([]api.Pusher, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectPushersSQL, localpart, serverName).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pushers []api.Pusher
	for rows.Next() {
		var pusher api.Pusher
		var data []byte
		if err := rows.Scan(&pusher.SessionID, &pusher.PushKey, &pusher.PushKeyTS, &pusher.Kind, &pusher.AppID, &pusher.AppDisplayName, &pusher.DeviceDisplayName, &pusher.ProfileTag, &pusher.Language, &data); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &pusher.Data); err != nil {
			return nil, err
		}
		pushers = append(pushers, pusher)
	}
	return pushers, rows.Err()
}

// DeletePusher removes a single pusher by pushkey and user localpart.
func (t *pushersTable) DeletePusher(ctx context.Context, appid, pushkey, localpart string, serverName spec.ServerName) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deletePusherSQL, appid, pushkey, localpart, serverName)
	return result.Error
}

// DeletePushers removes pushers by appid and pushkey.
func (t *pushersTable) DeletePushers(ctx context.Context, appid, pushkey string) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deletePushersByAppIdAndPushKeySQL, appid, pushkey)
	return result.Error
}
