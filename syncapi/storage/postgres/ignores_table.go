// Copyright 2022 The Matrix.org Foundation C.I.C.
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
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/types"
	"gorm.io/gorm"
)

const ignoresSchema = `
-- Stores data about ignoress
CREATE TABLE IF NOT EXISTS syncapi_ignores (
	-- The user ID whose ignore list this belongs to.
	user_id TEXT NOT NULL,
	ignores_json TEXT NOT NULL,
	PRIMARY KEY(user_id)
);
`

const selectIgnoresSQL = "" +
	"SELECT ignores_json FROM syncapi_ignores WHERE user_id = $1"

const upsertIgnoresSQL = "" +
	"INSERT INTO syncapi_ignores (user_id, ignores_json) VALUES ($1, $2)" +
	" ON CONFLICT (user_id) DO UPDATE set ignores_json = $2"

type ignoresTable struct {
	cm                *sqlutil.Connections
	selectIgnoresSQL  string
	upsertIgnoresSQL  string
}

func NewPostgresIgnoresTable(ctx context.Context, cm *sqlutil.Connections) (tables.Ignores, error) {
	// Create the table first
	db := cm.Connection(ctx, false)
	if err := db.Exec(ignoresSchema).Error; err != nil {
		return nil, err
	}
	
	// Initialize the table with SQL statements
	s := &ignoresTable{
		cm:               cm,
		selectIgnoresSQL: selectIgnoresSQL,
		upsertIgnoresSQL: upsertIgnoresSQL,
	}
	
	return s, nil
}

func (s *ignoresTable) SelectIgnores(
	ctx context.Context, userID string,
) (*types.IgnoredUsers, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)
	
	var ignoresData []byte
	err := db.Raw(s.selectIgnoresSQL, userID).Scan(&ignoresData).Error
	if err != nil {
		return nil, err
	}
	var ignores types.IgnoredUsers
	if err = json.Unmarshal(ignoresData, &ignores); err != nil {
		return nil, err
	}
	return &ignores, nil
}

func (s *ignoresTable) UpsertIgnores(
	ctx context.Context, userID string, ignores *types.IgnoredUsers,
) error {
	// Get database connection
	db := s.cm.Connection(ctx, false)
	
	ignoresJSON, err := json.Marshal(ignores)
	if err != nil {
		return err
	}
	return db.Exec(s.upsertIgnoresSQL, userID, ignoresJSON).Error
}
