// Copyright 2025 Ant Investor Ltd.
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
	"github.com/antinvestor/matrix/syncapi/storage/postgres"
)

// NewSyncServerDatabase opens a database connection.
func NewSyncServerDatabase(ctx context.Context, cm sqlutil.ConnectionManager) (Database, error) {
	switch {
	case cm.DS().IsPostgres():
		return postgres.NewDatabase(ctx, cm)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
