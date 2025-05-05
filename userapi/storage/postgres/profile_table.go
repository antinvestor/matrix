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
	"fmt"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

const profilesSchema = `
-- Stores data about accounts profiles.
CREATE TABLE IF NOT EXISTS userapi_profiles (
    -- The Global user ID localpart for this account
    localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
    -- The display name for this account
    display_name TEXT,
    -- The URL of the avatar for this account
    avatar_url TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS userapi_profiles_idx ON userapi_profiles(localpart, server_name);
`

const profilesSchemaRevert = "DROP TABLE IF EXISTS userapi_profiles CASCADE; DROP INDEX IF EXISTS userapi_profiles_idx;"

const insertProfileSQL = "" +
	"INSERT INTO userapi_profiles(localpart, server_name, display_name, avatar_url) VALUES ($1, $2, $3, $4)"

const selectProfileByLocalpartSQL = "" +
	"SELECT localpart, server_name, display_name, avatar_url FROM userapi_profiles WHERE localpart = $1 AND server_name = $2"

const setAvatarURLSQL = "" +
	"UPDATE userapi_profiles AS new" +
	" SET avatar_url = $1" +
	" FROM userapi_profiles AS old" +
	" WHERE new.localpart = $2 AND new.server_name = $3" +
	" RETURNING new.display_name, old.avatar_url <> new.avatar_url"

const setDisplayNameSQL = "" +
	"UPDATE userapi_profiles AS new" +
	" SET display_name = $1" +
	" FROM userapi_profiles AS old" +
	" WHERE new.localpart = $2 AND new.server_name = $3" +
	" RETURNING new.avatar_url, old.display_name <> new.display_name"

const selectProfilesBySearchSQL = "" +
	"SELECT localpart, server_name, display_name, avatar_url FROM userapi_profiles WHERE localpart LIKE $1 OR display_name LIKE $1 LIMIT $2"

// profilesTable implements tables.ProfileTable using GORM and a connection manager.
type profilesTable struct {
	cm                     *sqlutil.Connections
	serverNoticesLocalpart string

	insertProfileSQL            string
	selectProfileByLocalpartSQL string
	setAvatarURLSQL             string
	setDisplayNameSQL           string
	selectProfilesBySearchSQL   string
}

// NewPostgresProfilesTable returns a new ProfileTable using the provided connection manager.
func NewPostgresProfilesTable(cm *sqlutil.Connections, serverNoticesLocalpart string) tables.ProfileTable {
	return &profilesTable{
		cm:                          cm,
		serverNoticesLocalpart:      serverNoticesLocalpart,
		insertProfileSQL:            insertProfileSQL,
		selectProfileByLocalpartSQL: selectProfileByLocalpartSQL,
		setAvatarURLSQL:             setAvatarURLSQL,
		setDisplayNameSQL:           setDisplayNameSQL,
		selectProfilesBySearchSQL:   selectProfilesBySearchSQL,
	}
}

func (t *profilesTable) InsertProfile(ctx context.Context, localpart string, serverName spec.ServerName) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.insertProfileSQL, localpart, serverName, "", "")
	return result.Error
}

func (t *profilesTable) SelectProfileByLocalpart(ctx context.Context, localpart string, serverName spec.ServerName) (*authtypes.Profile, error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectProfileByLocalpartSQL, localpart, serverName).Row()
	var profile authtypes.Profile
	var displayName, avatarURL sql.NullString
	if err := row.Scan(&profile.Localpart, &profile.ServerName, &displayName, &avatarURL); err != nil {
		return nil, err
	}
	if displayName.Valid {
		profile.DisplayName = displayName.String
	}
	if avatarURL.Valid {
		profile.AvatarURL = avatarURL.String
	}
	return &profile, nil
}

func (t *profilesTable) SetAvatarURL(ctx context.Context, localpart string, serverName spec.ServerName, avatarURL string) (*authtypes.Profile, bool, error) {
	db := t.cm.Connection(ctx, false)
	profile := &authtypes.Profile{
		Localpart:  localpart,
		ServerName: string(serverName),
		AvatarURL:  avatarURL,
	}
	var displayName sql.NullString
	var changed bool
	row := db.Raw(t.setAvatarURLSQL, avatarURL, localpart, serverName).Row()
	err := row.Scan(&displayName, &changed)
	if displayName.Valid {
		profile.DisplayName = displayName.String
	}
	return profile, changed, err
}

func (t *profilesTable) SetDisplayName(ctx context.Context, localpart string, serverName spec.ServerName, displayName string) (*authtypes.Profile, bool, error) {
	db := t.cm.Connection(ctx, false)
	profile := &authtypes.Profile{
		Localpart:   localpart,
		ServerName:  string(serverName),
		DisplayName: displayName,
	}
	var avatarURL sql.NullString
	var changed bool
	row := db.Raw(t.setDisplayNameSQL, displayName, localpart, serverName).Row()
	err := row.Scan(&avatarURL, &changed)
	if avatarURL.Valid {
		profile.AvatarURL = avatarURL.String
	}
	return profile, changed, err
}

func (t *profilesTable) SelectProfilesBySearch(ctx context.Context, localpart, searchString string, limit int) ([]authtypes.Profile, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectProfilesBySearchSQL, fmt.Sprintf("%%%s%%", searchString), limit).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var profiles []authtypes.Profile
	for rows.Next() {
		var profile authtypes.Profile
		var displayName, avatarURL sql.NullString
		if err := rows.Scan(&profile.Localpart, &profile.ServerName, &displayName, &avatarURL); err != nil {
			return nil, err
		}
		if displayName.Valid {
			profile.DisplayName = displayName.String
		}
		if avatarURL.Valid {
			profile.AvatarURL = avatarURL.String
		}
		if profile.Localpart != t.serverNoticesLocalpart {
			profiles = append(profiles, profile)
		}
	}
	return profiles, rows.Err()
}
