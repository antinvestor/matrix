// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Matrix.org Foundation C.I.C.
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
	// Import the postgres database driver.
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/mediaapi/storage/shared"
	"github.com/antinvestor/matrix/setup/config"
	_ "github.com/lib/pq"
)

// Migrations All mediaapi migrations for the postgres module
var Migrations = []sqlutil.Migration{
	{
		Version:   "mediaapi_001_create_media_repository_table",
		QueryUp:   mediaSchema,
		QueryDown: mediaSchemaRevert,
	},
	{
		Version:   "mediaapi_002_create_thumbnail_table",
		QueryUp:   thumbnailSchema,
		QueryDown: thumbnailSchemaRevert,
	},
}

// NewDatabase opens a postgres database.
func NewDatabase(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions) (*shared.Database, error) {
	db, writer, err := conMan.Connection(ctx, dbProperties)
	if err != nil {
		return nil, err
	}

	m := sqlutil.NewMigrator(db, Migrations...)
	err = m.Up(ctx)
	if err != nil {
		return nil, err
	}

	mediaRepo, err := NewPostgresMediaRepositoryTable(ctx, db)
	if err != nil {
		return nil, err
	}
	thumbnails, err := NewPostgresThumbnailsTable(ctx, db)
	if err != nil {
		return nil, err
	}
	return &shared.Database{
		MediaRepository: mediaRepo,
		Thumbnails:      thumbnails,
		DB:              db,
		Writer:          writer,
	}, nil
}
