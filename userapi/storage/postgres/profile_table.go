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
)

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

// SQL query constants for profile operations
const (
	insertProfileSQL = "INSERT INTO userapi_profiles(localpart, server_name, display_name, avatar_url) VALUES ($1, $2, $3, $4)"

	selectProfileByLocalpartSQL = "SELECT localpart, server_name, display_name, avatar_url FROM userapi_profiles WHERE localpart = $1 AND server_name = $2"

	setAvatarURLSQL = "UPDATE userapi_profiles AS new" +
		" SET avatar_url = $1" +
		" FROM userapi_profiles AS old" +
		" WHERE new.localpart = $2 AND new.server_name = $3" +
		" RETURNING new.display_name, old.avatar_url <> new.avatar_url"

	setDisplayNameSQL = "UPDATE userapi_profiles AS new" +
		" SET display_name = $1" +
		" FROM userapi_profiles AS old" +
		" WHERE new.localpart = $2 AND new.server_name = $3" +
		" RETURNING new.avatar_url, old.display_name <> new.display_name"

	selectProfilesBySearchSQL = "SELECT localpart, server_name, display_name, avatar_url FROM userapi_profiles WHERE localpart LIKE $1 OR display_name LIKE $1 LIMIT $2"
)

type profilesTable struct {
	cm                     *sqlutil.Connections
	serverNoticesLocalpart string

	// SQL queries stored as fields for better maintainability
	insertProfileStmt            string
	selectProfileByLocalpartStmt string
	setAvatarURLStmt             string
	setDisplayNameStmt           string
	selectProfilesBySearchStmt   string
}

func NewPostgresProfilesTable(ctx context.Context, cm *sqlutil.Connections, serverNoticesLocalpart string) (tables.ProfileTable, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(profilesSchema).Error; err != nil {
		return nil, err
	}

	// Initialize table with SQL statements
	t := &profilesTable{
		cm:                           cm,
		serverNoticesLocalpart:       serverNoticesLocalpart,
		insertProfileStmt:            insertProfileSQL,
		selectProfileByLocalpartStmt: selectProfileByLocalpartSQL,
		setAvatarURLStmt:             setAvatarURLSQL,
		setDisplayNameStmt:           setDisplayNameSQL,
		selectProfilesBySearchStmt:   selectProfilesBySearchSQL,
	}

	return t, nil
}

func (s *profilesTable) InsertProfile(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (err error) {
	db := s.cm.Connection(ctx, false)

	return db.Exec(s.insertProfileStmt, localpart, serverName, "", "").Error
}

func (s *profilesTable) SelectProfileByLocalpart(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (*authtypes.Profile, error) {
	db := s.cm.Connection(ctx, true)

	var profile authtypes.Profile
	row := db.Raw(s.selectProfileByLocalpartStmt, localpart, serverName).Row()
	err := row.Scan(&profile.Localpart, &profile.ServerName, &profile.DisplayName, &profile.AvatarURL)
	if err != nil {
		return nil, err
	}
	return &profile, nil
}

func (s *profilesTable) SetAvatarURL(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	avatarURL string,
) (*authtypes.Profile, bool, error) {
	db := s.cm.Connection(ctx, false)

	profile := &authtypes.Profile{
		Localpart:  localpart,
		ServerName: string(serverName),
		AvatarURL:  avatarURL,
	}
	var changed bool
	row := db.Raw(s.setAvatarURLStmt, avatarURL, localpart, serverName).Row()
	err := row.Scan(&profile.DisplayName, &changed)
	return profile, changed, err
}

func (s *profilesTable) SetDisplayName(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	displayName string,
) (*authtypes.Profile, bool, error) {
	db := s.cm.Connection(ctx, false)

	profile := &authtypes.Profile{
		Localpart:   localpart,
		ServerName:  string(serverName),
		DisplayName: displayName,
	}
	var changed bool
	row := db.Raw(s.setDisplayNameStmt, displayName, localpart, serverName).Row()
	err := row.Scan(&profile.AvatarURL, &changed)
	return profile, changed, err
}

func (s *profilesTable) SelectProfilesBySearch(
	ctx context.Context, localpart, searchString string, limit int,
) ([]authtypes.Profile, error) {
	db := s.cm.Connection(ctx, true)

	var profiles []authtypes.Profile
	// The fmt.Sprintf directive below is building a parameter for the
	// "LIKE" condition in the SQL query. %% escapes the % char, so the
	// statement in the end will look like "LIKE %searchString%".
	rows, err := db.Raw(s.selectProfilesBySearchStmt, fmt.Sprintf("%%%s%%", searchString), limit).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectProfilesBySearch: rows.close() failed")

	for rows.Next() {
		var profile authtypes.Profile
		if err := rows.Scan(&profile.Localpart, &profile.ServerName, &profile.DisplayName, &profile.AvatarURL); err != nil {
			return nil, err
		}
		// Filter out server notices profiles
		if profile.Localpart != s.serverNoticesLocalpart {
			profiles = append(profiles, profile)
		}
	}

	return profiles, rows.Err()
}
