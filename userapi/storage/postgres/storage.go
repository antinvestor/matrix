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
	"github.com/pitabwire/frame"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/shared"

	// Import the postgres database driver.
	_ "github.com/lib/pq"
)

// userapi_accounts Centralised migrations for userapi tables
var Migrations = []frame.MigrationPatch{
	{
		Name:        "userapi_001_account_data",
		Patch:       accountDataSchema,
		RevertPatch: accountDataSchemaRevert,
	},
	{
		Name:        "userapi_002_accounts",
		Patch:       accountsSchema,
		RevertPatch: accountsSchemaRevert,
	},
	{
		Name:        "userapi_003_devices",
		Patch:       devicesSchema,
		RevertPatch: devicesSchemaRevert,
	},
	{
		Name:        "userapi_008_key_backup",
		Patch:       keyBackupTableSchema,
		RevertPatch: keyBackupTableSchemaRevert,
	},
	{
		Name:        "userapi_009_key_backup_version",
		Patch:       keyBackupVersionTableSchema,
		RevertPatch: keyBackupVersionTableSchemaRevert,
	},
	{
		Name:        "userapi_010_logintoken",
		Patch:       loginTokenSchema,
		RevertPatch: loginTokenSchemaRevert,
	},
	{
		Name:        "userapi_011_notifications",
		Patch:       notificationSchema,
		RevertPatch: notificationsSchemaRevert,
	},
	{
		Name:        "userapi_012_openid",
		Patch:       openIDTokenSchema,
		RevertPatch: openIDTokenSchemaRevert,
	},
	{
		Name:        "userapi_013_profile",
		Patch:       profilesSchema,
		RevertPatch: profilesSchemaRevert,
	},
	{
		Name:        "userapi_014_pusher",
		Patch:       pushersSchema,
		RevertPatch: pushersSchemaRevert,
	},
	{
		Name:        "userapi_015_registration_tokens",
		Patch:       registrationTokensSchema,
		RevertPatch: registrationTokensSchemaRevert,
	},

	{
		Name:        "userapi_017_stats_daily_visits",
		Patch:       userDailyVisitsSchema,
		RevertPatch: userDailyVisitsSchemaRevert,
	},
	{
		Name:        "userapi_018_stats_daily_stats",
		Patch:       messagesDailySchema,
		RevertPatch: messagesDailySchemaRevert,
	},
	{
		Name:        "userapi_019_threepid",
		Patch:       threepidSchema,
		RevertPatch: threepidSchemaRevert,
	},
}

// NewDatabase creates a new accounts and profiles database
func NewDatabase(ctx context.Context, cm *sqlutil.Connections, serverName spec.ServerName, bcryptCost int, openIDTokenLifetimeMS int64, loginTokenLifetime time.Duration, serverNoticesLocalpart string) (*shared.Database, error) {

	err := cm.MigrateStrings(ctx, Migrations...)
	if err != nil {
		return nil, err
	}

	registationTokensTable := NewPostgresRegistrationTokensTable(cm)
	accountsTable := NewPostgresAccountsTable(cm)
	accountDataTable := NewPostgresAccountDataTable(cm)
	devicesTable := NewPostgresDevicesTable(cm, serverName)
	keyBackupTable := NewPostgresKeyBackupTable(cm)
	keyBackupVersionTable := NewPostgresKeyBackupVersionTable(cm)
	loginTokenTable := NewPostgresLoginTokenTable(cm)
	openIDTable := NewPostgresOpenIDTable(cm)
	profilesTable := NewPostgresProfilesTable(cm, serverNoticesLocalpart)
	threePIDTable := NewPostgresThreePIDTable(cm)
	pusherTable := NewPostgresPusherTable(cm)
	notificationsTable := NewPostgresNotificationsTable(cm)
	statsTable := NewPostgresStatsTable(cm)

	return &shared.Database{
		AccountDatas:          accountDataTable,
		Accounts:              accountsTable,
		Devices:               devicesTable,
		KeyBackups:            keyBackupTable,
		KeyBackupVersions:     keyBackupVersionTable,
		LoginTokens:           loginTokenTable,
		OpenIDTokens:          openIDTable,
		Profiles:              profilesTable,
		ThreePIDs:             threePIDTable,
		Pushers:               pusherTable,
		Notifications:         notificationsTable,
		RegistrationTokens:    registationTokensTable,
		Stats:                 statsTable,
		ServerName:            serverName,
		LoginTokenLifetime:    loginTokenLifetime,
		BcryptCost:            bcryptCost,
		OpenIDTokenLifetimeMS: openIDTokenLifetimeMS,
	}, nil
}

var MigrationsKeys = []frame.MigrationPatch{
	{
		Name:        "userapi_004_device_keys",
		Patch:       deviceKeysSchema,
		RevertPatch: deviceKeysSchemaRevert,
	},
	{
		Name:        "userapi_005_one_time_keys",
		Patch:       oneTimeKeysSchema,
		RevertPatch: oneTimeKeysSchemaRevert,
	},
	{
		Name:        "userapi_006_cross_signing_keys",
		Patch:       crossSigningKeysSchema,
		RevertPatch: crossSigningKeysSchemaRevert,
	},
	{
		Name:        "userapi_007_cross_signing_sigs",
		Patch:       crossSigningSigsSchema,
		RevertPatch: crossSigningSigsSchemaRevert,
	},
	{
		Name:        "userapi_016_stale_device_lists",
		Patch:       staleDeviceListsSchema,
		RevertPatch: staleDeviceListsSchemaRevert,
	},
	{
		Name:        "userapi_020_key_changes",
		Patch:       keyChangesSchema,
		RevertPatch: keyChangesSchemaRevert,
	},
}

func NewKeyDatabase(ctx context.Context, cm *sqlutil.Connections) (*shared.KeyDatabase, error) {

	err := cm.MigrateStrings(ctx, MigrationsKeys...)
	if err != nil {
		return nil, err
	}

	otk := NewPostgresOneTimeKeysTable(cm)
	dk := NewPostgresDeviceKeysTable(cm)
	kc := NewPostgresKeyChangesTable(cm)
	sdl := NewPostgresStaleDeviceListsTable(cm)
	csk := NewPostgresCrossSigningKeysTable(cm)
	css := NewPostgresCrossSigningSigsTable(cm)

	return &shared.KeyDatabase{
		OneTimeKeysTable:      otk,
		DeviceKeysTable:       dk,
		KeyChangesTable:       kc,
		StaleDeviceListsTable: sdl,
		CrossSigningKeysTable: csk,
		CrossSigningSigsTable: css,
	}, nil
}
