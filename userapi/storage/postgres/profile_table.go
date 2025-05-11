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
	"fmt"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/pitabwire/frame"
)

// profilesSchema defines the schema for profiles table.
const profilesSchema = `
-- Stores data about accounts profiles.
CREATE TABLE IF NOT EXISTS userapi_profiles (
    -- The Matrix user ID localpart for this account
    localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
    -- The display name for this account
    display_name TEXT,
    -- The URL of the avatar for this account
    avatar_url TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS userapi_profiles_idx ON userapi_profiles(localpart, server_name);
`

// profilesSchemaRevert defines the revert operation for profiles schema.
const profilesSchemaRevert = `
DROP TABLE IF EXISTS userapi_profiles;
`

// insertProfileSQL defines the SQL query to insert a new profile.
const insertProfileSQL = `
INSERT INTO userapi_profiles(localpart, server_name, display_name, avatar_url) VALUES ($1, $2, $3, $4)
`

// selectProfileByLocalpartSQL defines the SQL query to select a profile by localpart.
const selectProfileByLocalpartSQL = `
SELECT localpart, server_name, display_name, avatar_url FROM userapi_profiles WHERE localpart = $1 AND server_name = $2
`

// setAvatarURLSQL defines the SQL query to update a profile's avatar URL.
const setAvatarURLSQL = `
UPDATE userapi_profiles AS new
SET avatar_url = $1
FROM userapi_profiles AS old
WHERE new.localpart = $2 AND new.server_name = $3
RETURNING new.display_name, old.avatar_url <> new.avatar_url
`

// setDisplayNameSQL defines the SQL query to update a profile's display name.
const setDisplayNameSQL = `
UPDATE userapi_profiles AS new
SET display_name = $1
FROM userapi_profiles AS old
WHERE new.localpart = $2 AND new.server_name = $3
RETURNING new.avatar_url, old.display_name <> new.display_name
`

// selectProfilesBySearchSQL defines the SQL query to search for profiles.
const selectProfilesBySearchSQL = `
SELECT localpart, server_name, display_name, avatar_url FROM userapi_profiles WHERE localpart LIKE $1 OR display_name LIKE $1 LIMIT $2
`

// profileTable represents the profiles table in the database.
type profileTable struct {
	cm                          sqlutil.ConnectionManager
	serverNoticesLocalpart      string
	insertProfileSQL            string
	selectProfileByLocalpartSQL string
	setAvatarURLSQL             string
	setDisplayNameSQL           string
	selectProfilesBySearchSQL   string
}

// NewPostgresProfilesTable creates a new profile table object.
func NewPostgresProfilesTable(ctx context.Context, cm sqlutil.ConnectionManager, serverNoticesLocalpart string) (tables.ProfileTable, error) {
	s := &profileTable{
		cm:                          cm,
		serverNoticesLocalpart:      serverNoticesLocalpart,
		insertProfileSQL:            insertProfileSQL,
		selectProfileByLocalpartSQL: selectProfileByLocalpartSQL,
		setAvatarURLSQL:             setAvatarURLSQL,
		setDisplayNameSQL:           setDisplayNameSQL,
		selectProfilesBySearchSQL:   selectProfilesBySearchSQL,
	}

	// Perform schema migration
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "userapi_profiles_table_schema_001",
		Patch:       profilesSchema,
		RevertPatch: profilesSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// InsertProfile creates a new profile for a given user.
func (s *profileTable) InsertProfile(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (err error) {
	db := s.cm.Connection(ctx, false)
	err = db.Exec(s.insertProfileSQL, localpart, serverName, "", "").Error
	return
}

// SelectProfileByLocalpart retrieves a profile for a given user.
func (s *profileTable) SelectProfileByLocalpart(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (*authtypes.Profile, error) {
	var profile authtypes.Profile
	db := s.cm.Connection(ctx, true)
	row := db.Raw(s.selectProfileByLocalpartSQL, localpart, serverName).Row()
	err := row.Scan(
		&profile.Localpart, &profile.ServerName, &profile.DisplayName, &profile.AvatarURL,
	)
	if err != nil {
		return nil, err
	}
	return &profile, nil
}

// SetAvatarURL updates the avatar URL for a user's profile.
func (s *profileTable) SetAvatarURL(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	avatarURL string,
) (*authtypes.Profile, bool, error) {
	profile := &authtypes.Profile{
		Localpart:  localpart,
		ServerName: string(serverName),
		AvatarURL:  avatarURL,
	}
	var changed bool
	db := s.cm.Connection(ctx, false)
	row := db.Raw(s.setAvatarURLSQL, avatarURL, localpart, serverName).Row()
	err := row.Scan(&profile.DisplayName, &changed)
	return profile, changed, err
}

// SetDisplayName updates the display name for a user's profile.
func (s *profileTable) SetDisplayName(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	displayName string,
) (*authtypes.Profile, bool, error) {
	profile := &authtypes.Profile{
		Localpart:   localpart,
		ServerName:  string(serverName),
		DisplayName: displayName,
	}
	var changed bool
	db := s.cm.Connection(ctx, false)
	row := db.Raw(s.setDisplayNameSQL, displayName, localpart, serverName).Row()
	err := row.Scan(&profile.AvatarURL, &changed)
	return profile, changed, err
}

// SelectProfilesBySearch searches for profiles by localpart or display name.
func (s *profileTable) SelectProfilesBySearch(
	ctx context.Context, searchLocalpart, searchString string, limit int,
) ([]authtypes.Profile, error) {
	var profiles []authtypes.Profile
	// The fmt.Sprintf directive below is building a parameter for the
	// "LIKE" condition in the SQL query. %% escapes the % char, so the
	// statement in the end will look like "LIKE %searchString%".
	db := s.cm.Connection(ctx, true)
	rows, err := db.Raw(s.selectProfilesBySearchSQL, fmt.Sprintf("%%%s%%", searchString), limit).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectProfilesBySearch: rows.close() failed")
	for rows.Next() {
		var profile authtypes.Profile
		if err := rows.Scan(&profile.Localpart, &profile.ServerName, &profile.DisplayName, &profile.AvatarURL); err != nil {
			return nil, err
		}
		if profile.Localpart != s.serverNoticesLocalpart {
			profiles = append(profiles, profile)
		}
	}
	return profiles, rows.Err()
}
