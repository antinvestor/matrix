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
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/mediaapi/storage/tables"
	"github.com/antinvestor/matrix/mediaapi/types"
	"github.com/pitabwire/frame"
)

const thumbnailSchema = `
-- The mediaapi_thumbnail table holds metadata for each thumbnail file stored and accessible to the local server,
-- the actual file is stored separately.
CREATE TABLE IF NOT EXISTS mediaapi_thumbnail (
    -- The id used to refer to the media.
    -- For uploads to this server this is a base64-encoded sha256 hash of the file data
    -- For media from remote servers, this can be any unique identifier string
    media_id TEXT NOT NULL,
    -- The origin of the media as requested by the client. Should be a homeserver domain.
    media_origin TEXT NOT NULL,
    -- The MIME-type of the thumbnail file.
    content_type TEXT NOT NULL,
    -- Size of the thumbnail file in bytes.
    file_size_bytes BIGINT NOT NULL,
    -- When the thumbnail was generated in UNIX epoch ms.
    creation_ts BIGINT NOT NULL,
    -- The width of the thumbnail
    width INTEGER NOT NULL,
    -- The height of the thumbnail
    height INTEGER NOT NULL,
    -- The resize method used to generate the thumbnail. Can be crop or scale.
    resize_method TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS mediaapi_thumbnail_index ON mediaapi_thumbnail (media_id, media_origin, width, height, resize_method);
`

const thumbnailSchemaRevert = `
DROP TABLE IF EXISTS mediaapi_thumbnail;
`

const insertThumbnailSQL = `
INSERT INTO mediaapi_thumbnail (media_id, media_origin, content_type, file_size_bytes, creation_ts, width, height, resize_method)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
`

// Note: this selects one specific thumbnail
const selectThumbnailSQL = `
SELECT content_type, file_size_bytes, creation_ts FROM mediaapi_thumbnail WHERE media_id = $1 AND media_origin = $2 AND width = $3 AND height = $4 AND resize_method = $5
`

// Note: this selects all thumbnails for a media_origin and media_id
const selectThumbnailsSQL = `
SELECT content_type, file_size_bytes, creation_ts, width, height, resize_method FROM mediaapi_thumbnail WHERE media_id = $1 AND media_origin = $2 ORDER BY creation_ts ASC
`

type thumbnailStatements struct {
	cm sqlutil.ConnectionManager

	// SQL statements stored as struct fields
	insertThumbnailStmt  string
	selectThumbnailStmt  string
	selectThumbnailsStmt string
}

func NewPostgresThumbnailsTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.Thumbnails, error) {
	// Create the table using standardized migration approach
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "mediaapi_thumbnail_table_schema_001",
		Patch:       thumbnailSchema,
		RevertPatch: thumbnailSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Initialize the statements
	s := &thumbnailStatements{
		cm:                   cm,
		insertThumbnailStmt:  insertThumbnailSQL,
		selectThumbnailStmt:  selectThumbnailSQL,
		selectThumbnailsStmt: selectThumbnailsSQL,
	}

	return s, nil
}

func (s *thumbnailStatements) InsertThumbnail(
	ctx context.Context, thumbnailMetadata *types.ThumbnailMetadata,
) error {
	thumbnailMetadata.MediaMetadata.CreationTimestamp = spec.AsTimestamp(time.Now())

	// Get database connection
	db := s.cm.Connection(ctx, false)

	// Execute the insertion
	return db.Exec(
		s.insertThumbnailStmt,
		thumbnailMetadata.MediaMetadata.MediaID,
		thumbnailMetadata.MediaMetadata.Origin,
		thumbnailMetadata.MediaMetadata.ContentType,
		thumbnailMetadata.MediaMetadata.FileSizeBytes,
		thumbnailMetadata.MediaMetadata.CreationTimestamp,
		thumbnailMetadata.ThumbnailSize.Width,
		thumbnailMetadata.ThumbnailSize.Height,
		thumbnailMetadata.ThumbnailSize.ResizeMethod,
	).Error
}

func (s *thumbnailStatements) SelectThumbnail(
	ctx context.Context,
	mediaID types.MediaID,
	mediaOrigin spec.ServerName,
	width, height int,
	resizeMethod string,
) (*types.ThumbnailMetadata, error) {
	thumbnailMetadata := types.ThumbnailMetadata{
		MediaMetadata: &types.MediaMetadata{
			MediaID: mediaID,
			Origin:  mediaOrigin,
		},
		ThumbnailSize: types.ThumbnailSize{
			Width:        width,
			Height:       height,
			ResizeMethod: resizeMethod,
		},
	}

	// Get database connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	row := db.Raw(
		s.selectThumbnailStmt,
		thumbnailMetadata.MediaMetadata.MediaID,
		thumbnailMetadata.MediaMetadata.Origin,
		thumbnailMetadata.ThumbnailSize.Width,
		thumbnailMetadata.ThumbnailSize.Height,
		thumbnailMetadata.ThumbnailSize.ResizeMethod,
	).Row()

	err := row.Scan(
		&thumbnailMetadata.MediaMetadata.ContentType,
		&thumbnailMetadata.MediaMetadata.FileSizeBytes,
		&thumbnailMetadata.MediaMetadata.CreationTimestamp,
	)
	return &thumbnailMetadata, err
}

func (s *thumbnailStatements) SelectThumbnails(
	ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName,
) ([]*types.ThumbnailMetadata, error) {
	// Get database connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	rows, err := db.Raw(
		s.selectThumbnailsStmt,
		mediaID,
		mediaOrigin,
	).Rows()

	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectThumbnails: rows.close() failed")

	var thumbnails []*types.ThumbnailMetadata
	for rows.Next() {
		thumbnailMetadata := types.ThumbnailMetadata{
			MediaMetadata: &types.MediaMetadata{
				MediaID: mediaID,
				Origin:  mediaOrigin,
			},
		}
		err = rows.Scan(
			&thumbnailMetadata.MediaMetadata.ContentType,
			&thumbnailMetadata.MediaMetadata.FileSizeBytes,
			&thumbnailMetadata.MediaMetadata.CreationTimestamp,
			&thumbnailMetadata.ThumbnailSize.Width,
			&thumbnailMetadata.ThumbnailSize.Height,
			&thumbnailMetadata.ThumbnailSize.ResizeMethod,
		)
		if err != nil {
			return nil, err
		}
		thumbnails = append(thumbnails, &thumbnailMetadata)
	}

	return thumbnails, rows.Err()
}
