// Copyright 2020 The Matrix.org Foundation C.I.C.
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

package storage

import (
	"context"
	"fmt"

	profilev1 "github.com/antinvestor/apis/go/profile/v1"

	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/userapi/storage/distributed"
	"github.com/antinvestor/matrix/userapi/storage/postgres"
)

// NewUserDatabase opens a new Postgres database (based on dataSourceName scheme)
// and sets postgres connection parameters
func NewUserDatabase(
	ctx context.Context,
	profileCli *profilev1.ProfileClient,
	conMan *sqlutil.Connections,
	dbProperties *config.DatabaseOptions,
	serverName spec.ServerName,
	bcryptCost int,
	openIDTokenLifetimeMS int64,
	loginTokenLifetime time.Duration,
	serverNoticesLocalpart string,
) (UserDatabase, error) {
	if !dbProperties.ConnectionString.IsPostgres() {
		return nil, fmt.Errorf("unexpected database type")
	}
	pgUserDb, err := postgres.NewDatabase(ctx, conMan, dbProperties, serverName, bcryptCost, openIDTokenLifetimeMS, loginTokenLifetime, serverNoticesLocalpart)
	if err != nil {
		return nil, err
	}

	if profileCli == nil {
		return pgUserDb, nil
	}

	distributedDb, err := distributed.NewDatabase(ctx, profileCli, pgUserDb)
	if err != nil {
		return nil, err
	}

	return distributedDb, nil
}

// NewKeyDatabase opens a new Postgres database (base on dataSourceName) scheme)
// and sets postgres connection parameters.
func NewKeyDatabase(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions) (KeyDatabase, error) {
	switch {
	case dbProperties.ConnectionString.IsPostgres():
		return postgres.NewKeyDatabase(ctx, conMan, dbProperties)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
