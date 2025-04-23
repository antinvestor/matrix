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
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/userapi/storage/shared"

	// Import the postgres database driver.
	_ "github.com/lib/pq"
)

// Centralised migrations for userapi tables
var Migrations = []sqlutil.Migration{
	{
		Version:   "userapi_001_account_data",
		QueryUp:   accountDataSchema,
		QueryDown: accountDataSchemaRevert,
	},
	{
		Version:   "userapi_002_accounts",
		QueryUp:   accountsSchema,
		QueryDown: accountsSchemaRevert,
	},
	{
		Version:   "userapi_003_devices",
		QueryUp:   devicesSchema,
		QueryDown: devicesSchemaRevert,
	},
	{
		Version:   "userapi_008_key_backup",
		QueryUp:   keyBackupTableSchema,
		QueryDown: keyBackupTableSchemaRevert,
	},
	{
		Version:   "userapi_009_key_backup_version",
		QueryUp:   keyBackupVersionTableSchema,
		QueryDown: keyBackupVersionTableSchemaRevert,
	},
	{
		Version:   "userapi_010_logintoken",
		QueryUp:   loginTokenSchema,
		QueryDown: loginTokenSchemaRevert,
	},
	{
		Version:   "userapi_011_notifications",
		QueryUp:   notificationSchema,
		QueryDown: notificationsSchemaRevert,
	},
	{
		Version:   "userapi_012_openid",
		QueryUp:   openIDTokenSchema,
		QueryDown: openIDTokenSchemaRevert,
	},
	{
		Version:   "userapi_013_profile",
		QueryUp:   profilesSchema,
		QueryDown: profilesSchemaRevert,
	},
	{
		Version:   "userapi_014_pusher",
		QueryUp:   pushersSchema,
		QueryDown: pushersSchemaRevert,
	},
	{
		Version:   "userapi_015_registration_tokens",
		QueryUp:   registrationTokensSchema,
		QueryDown: registrationTokensSchemaRevert,
	},

	{
		Version:   "userapi_017_stats_daily_visits",
		QueryUp:   userDailyVisitsSchema,
		QueryDown: userDailyVisitsSchemaRevert,
	},
	{
		Version:   "userapi_018_stats_daily_stats",
		QueryUp:   messagesDailySchema,
		QueryDown: messagesDailySchemaRevert,
	},
	{
		Version:   "userapi_019_threepid",
		QueryUp:   threepidSchema,
		QueryDown: threepidSchemaRevert,
	},
}

// NewDatabase creates a new accounts and profiles database
func NewDatabase(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions, serverName spec.ServerName, bcryptCost int, openIDTokenLifetimeMS int64, loginTokenLifetime time.Duration, serverNoticesLocalpart string) (*shared.Database, error) {
	db, writer, err := conMan.Connection(ctx, dbProperties)
	if err != nil {
		return nil, err
	}

	m := sqlutil.NewMigrator(db)
	m.AddMigrations(Migrations...)
	if err = m.Up(ctx); err != nil {
		return nil, err
	}

	registationTokensTable, err := NewPostgresRegistrationTokensTable(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresRegistrationsTokenTable: %w", err)
	}
	accountsTable, err := NewPostgresAccountsTable(ctx, db, serverName)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresAccountsTable: %w", err)
	}
	accountDataTable, err := NewPostgresAccountDataTable(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresAccountDataTable: %w", err)
	}
	devicesTable, err := NewPostgresDevicesTable(ctx, db, serverName)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresDevicesTable: %w", err)
	}
	keyBackupTable, err := NewPostgresKeyBackupTable(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresKeyBackupTable: %w", err)
	}
	keyBackupVersionTable, err := NewPostgresKeyBackupVersionTable(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresKeyBackupVersionTable: %w", err)
	}
	loginTokenTable, err := NewPostgresLoginTokenTable(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresLoginTokenTable: %w", err)
	}
	openIDTable, err := NewPostgresOpenIDTable(ctx, db, serverName)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresOpenIDTable: %w", err)
	}
	profilesTable, err := NewPostgresProfilesTable(db, serverNoticesLocalpart)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresProfilesTable: %w", err)
	}
	threePIDTable, err := NewPostgresThreePIDTable(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresThreePIDTable: %w", err)
	}
	pusherTable, err := NewPostgresPusherTable(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresPusherTable: %w", err)
	}
	notificationsTable, err := NewPostgresNotificationTable(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresNotificationTable: %w", err)
	}
	statsTable, err := NewPostgresStatsTable(ctx, db, serverName)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresStatsTable: %w", err)
	}

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
		DB:                    db,
		Writer:                writer,
		LoginTokenLifetime:    loginTokenLifetime,
		BcryptCost:            bcryptCost,
		OpenIDTokenLifetimeMS: openIDTokenLifetimeMS,
	}, nil
}

var MigrationsKeys = []sqlutil.Migration{
	{
		Version:   "userapi_004_device_keys",
		QueryUp:   deviceKeysSchema,
		QueryDown: deviceKeysSchemaRevert,
	},
	{
		Version:   "userapi_005_one_time_keys",
		QueryUp:   oneTimeKeysSchema,
		QueryDown: oneTimeKeysSchemaRevert,
	},
	{
		Version:   "userapi_006_cross_signing_keys",
		QueryUp:   crossSigningKeysSchema,
		QueryDown: crossSigningKeysSchemaRevert,
	},
	{
		Version:   "userapi_007_cross_signing_sigs",
		QueryUp:   crossSigningSigsSchema,
		QueryDown: crossSigningSigsSchemaRevert,
	},
	{
		Version:   "userapi_016_stale_device_lists",
		QueryUp:   staleDeviceListsSchema,
		QueryDown: staleDeviceListsSchemaRevert,
	},
	{
		Version:   "userapi_020_key_changes",
		QueryUp:   keyChangesSchema,
		QueryDown: keyChangesSchemaRevert,
	},
}

func NewKeyDatabase(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions) (*shared.KeyDatabase, error) {
	db, writer, err := conMan.Connection(ctx, dbProperties)
	if err != nil {
		return nil, err
	}

	m := sqlutil.NewMigrator(db)
	m.AddMigrations(MigrationsKeys...)
	if err = m.Up(ctx); err != nil {
		return nil, err
	}

	otk, err := NewPostgresOneTimeKeysTable(ctx, db)
	if err != nil {
		return nil, err
	}
	dk, err := NewPostgresDeviceKeysTable(ctx, db)
	if err != nil {
		return nil, err
	}
	kc, err := NewPostgresKeyChangesTable(ctx, db)
	if err != nil {
		return nil, err
	}
	sdl, err := NewPostgresStaleDeviceListsTable(ctx, db)
	if err != nil {
		return nil, err
	}
	csk, err := NewPostgresCrossSigningKeysTable(ctx, db)
	if err != nil {
		return nil, err
	}
	css, err := NewPostgresCrossSigningSigsTable(ctx, db)
	if err != nil {
		return nil, err
	}

	return &shared.KeyDatabase{
		OneTimeKeysTable:      otk,
		DeviceKeysTable:       dk,
		KeyChangesTable:       kc,
		StaleDeviceListsTable: sdl,
		CrossSigningKeysTable: csk,
		CrossSigningSigsTable: css,
		Writer:                writer,
	}, nil
}
