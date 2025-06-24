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
	"github.com/antinvestor/matrix/userapi/storage/shared"

	// Import the postgres database driver.
	_ "github.com/lib/pq"
)

// NewDatabase creates a new accounts and profiles database
func NewDatabase(ctx context.Context, cm sqlutil.ConnectionManager, serverName spec.ServerName, bcryptCost int, openIDTokenLifetimeMS int64, loginTokenLifetime time.Duration, serverNoticesLocalpart string) (*shared.Database, error) {

	registationTokensTable, err := NewPostgresRegistrationTokensTable(ctx, cm)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresRegistrationsTokenTable: %w", err)
	}
	accountsTbl, err := NewPostgresAccountsTable(ctx, cm, serverName)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresAccountsTable: %w", err)
	}
	accountDataTbl, err := NewPostgresAccountDataTable(ctx, cm)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresAccountDataTable: %w", err)
	}
	devicesTbl, err := NewPostgresDevicesTable(ctx, cm, serverName)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresDevicesTable: %w", err)
	}
	keyBackupTbl, err := NewPostgresKeyBackupTable(ctx, cm)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresKeyBackupTable: %w", err)
	}
	keyBackupVersionTbl, err := NewPostgresKeyBackupVersionTable(ctx, cm)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresKeyBackupVersionTable: %w", err)
	}
	loginTokenTbl, err := NewPostgresLoginTokenTable(ctx, cm)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresLoginTokenTable: %w", err)
	}
	openIDTbl, err := NewPostgresOpenIDTable(ctx, cm, serverName)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresOpenIDTable: %w", err)
	}
	profileTbl, err := NewPostgresProfilesTable(ctx, cm, serverNoticesLocalpart)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresProfilesTable: %w", err)
	}
	threePIDTbl, err := NewPostgresThreePIDTable(ctx, cm)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresThreePIDTable: %w", err)
	}
	pusherTbl, err := NewPostgresPusherTable(ctx, cm)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresPusherTable: %w", err)
	}
	notificationsTbl, err := NewPostgresNotificationTable(ctx, cm)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresNotificationTable: %w", err)
	}
	statsTbl, err := NewPostgresStatsTable(ctx, cm, serverName)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresStatsTable: %w", err)
	}

	err = cm.Migrate(ctx)
	if err != nil {
		return nil, err
	}

	return &shared.Database{
		Cm:                    cm,
		AccountDatas:          accountDataTbl,
		Accounts:              accountsTbl,
		Devices:               devicesTbl,
		KeyBackups:            keyBackupTbl,
		KeyBackupVersions:     keyBackupVersionTbl,
		LoginTokens:           loginTokenTbl,
		OpenIDTokens:          openIDTbl,
		Profiles:              profileTbl,
		ThreePIDs:             threePIDTbl,
		Pushers:               pusherTbl,
		Notifications:         notificationsTbl,
		RegistrationTokens:    registationTokensTable,
		Stats:                 statsTbl,
		ServerName:            serverName,
		LoginTokenLifetime:    loginTokenLifetime,
		BcryptCost:            bcryptCost,
		OpenIDTokenLifetimeMS: openIDTokenLifetimeMS,
	}, nil
}

func NewKeyDatabase(ctx context.Context, cm sqlutil.ConnectionManager) (*shared.KeyDatabase, error) {

	otk, err := NewPostgresOneTimeKeysTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	dk, err := NewPostgresDeviceKeysTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	kc, err := NewPostgresKeyChangesTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	sdl, err := NewPostgresStaleDeviceListsTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	csk, err := NewPostgresCrossSigningKeysTable(ctx, cm)
	if err != nil {
		return nil, err
	}
	css, err := NewPostgresCrossSigningSigsTable(ctx, cm)
	if err != nil {
		return nil, err
	}

	err = cm.Migrate(ctx)
	if err != nil {
		return nil, err
	}

	return &shared.KeyDatabase{
		Cm:                    cm,
		OneTimeKeysTable:      otk,
		DeviceKeysTable:       dk,
		KeyChangesTable:       kc,
		StaleDeviceListsTable: sdl,
		CrossSigningKeysTable: csk,
		CrossSigningSigsTable: css,
	}, nil
}
