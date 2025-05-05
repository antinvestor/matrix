// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Global.org Foundation C.I.C.
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

	// Import the postgres database driver.
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/mediaapi/storage/shared"
	_ "github.com/lib/pq"
)

// Migrations All mediaapi migrations for the postgres module
var Migrations = []frame.MigrationPatch{
	{
		Name:        "mediaapi_001_create_media_repository_table",
		Patch:       mediaSchema,
		RevertPatch: mediaSchemaRevert,
	},
	{
		Name:        "mediaapi_002_create_thumbnail_table",
		Patch:       thumbnailSchema,
		RevertPatch: thumbnailSchemaRevert,
	},
}

// NewDatabase opens a postgres database.
func NewDatabase(ctx context.Context, cm *sqlutil.Connections) (*shared.Database, error) {

	err := cm.MigrateStrings(ctx, Migrations...)
	if err != nil {
		return nil, err
	}

	mediaRepo := NewPostgresMediaRepositoryTable(cm)
	thumbnails := NewPostgresThumbnailsTable(cm)
	return &shared.Database{
		MediaRepository: mediaRepo,
		Thumbnails:      thumbnails,
	}, nil
}
