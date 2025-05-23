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

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/mediaapi/storage/postgres"
	"github.com/antinvestor/matrix/setup/config"
)

// NewMediaAPIDatasource opens a database connection.
func NewMediaAPIDatasource(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions) (Database, error) {
	switch {
	case dbProperties.ConnectionString.IsPostgres():
		return postgres.NewDatabase(ctx, conMan, dbProperties)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
