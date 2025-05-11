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
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/mediaapi/storage/tables"
	"github.com/antinvestor/matrix/mediaapi/types"
	"github.com/pitabwire/frame"
)

const mediaSchema = `
-- The media_repository table holds metadata for each media file stored and accessible to the local server,
-- the actual file is stored separately.
CREATE TABLE IF NOT EXISTS mediaapi_media_repository (
    -- The id used to refer to the media.
    -- For uploads to this server this is a base64-encoded sha256 hash of the file data
    -- For media from remote servers, this can be any unique identifier string
    media_id TEXT NOT NULL,
    -- The origin of the media as requested by the client. Should be a homeserver domain.
    media_origin TEXT NOT NULL,
    -- The MIME-type of the media file as specified when uploading.
    content_type TEXT NOT NULL,
    -- Size of the media file in bytes.
    file_size_bytes BIGINT NOT NULL,
    -- When the content was uploaded in UNIX epoch ms.
    creation_ts BIGINT NOT NULL,
    -- The file name with which the media was uploaded.
    upload_name TEXT NOT NULL,
    -- Alternate RFC 4648 unpadded base64 encoding string representation of a SHA-256 hash sum of the file data.
    base64hash TEXT NOT NULL,
    -- The user who uploaded the file. Should be a Matrix user ID.
    user_id TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS mediaapi_media_repository_index ON mediaapi_media_repository (media_id, media_origin);
`

const mediaSchemaRevert = `
DROP TABLE IF EXISTS mediaapi_media_repository;
`

const insertMediaSQL = `
INSERT INTO mediaapi_media_repository (media_id, media_origin, content_type, file_size_bytes, creation_ts, upload_name, base64hash, user_id)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
`

const selectMediaSQL = `
SELECT content_type, file_size_bytes, creation_ts, upload_name, base64hash, user_id FROM mediaapi_media_repository WHERE media_id = $1 AND media_origin = $2
`

const selectMediaByHashSQL = `
SELECT content_type, file_size_bytes, creation_ts, upload_name, media_id, user_id FROM mediaapi_media_repository WHERE base64hash = $1 AND media_origin = $2
`

type mediaStatements struct {
	cm sqlutil.ConnectionManager

	// SQL statements stored as struct fields
	insertMediaStmt       string
	selectMediaStmt       string
	selectMediaByHashStmt string
}

func NewPostgresMediaRepositoryTable(ctx context.Context, cm sqlutil.ConnectionManager) (tables.MediaRepository, error) {
	// Create the table using standardized migration approach
	err := cm.Collect(&frame.MigrationPatch{
		Name:        "mediaapi_media_repository_table_schema_001",
		Patch:       mediaSchema,
		RevertPatch: mediaSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	// Initialize the statements
	s := &mediaStatements{
		cm:                    cm,
		insertMediaStmt:       insertMediaSQL,
		selectMediaStmt:       selectMediaSQL,
		selectMediaByHashStmt: selectMediaByHashSQL,
	}

	return s, nil
}

func (s *mediaStatements) InsertMedia(
	ctx context.Context, mediaMetadata *types.MediaMetadata,
) error {
	mediaMetadata.CreationTimestamp = spec.AsTimestamp(time.Now())

	// Get database connection
	db := s.cm.Connection(ctx, false)

	// Execute the insertion
	return db.Exec(
		s.insertMediaStmt,
		mediaMetadata.MediaID,
		mediaMetadata.Origin,
		mediaMetadata.ContentType,
		mediaMetadata.FileSizeBytes,
		mediaMetadata.CreationTimestamp,
		mediaMetadata.UploadName,
		mediaMetadata.Base64Hash,
		mediaMetadata.UserID,
	).Error
}

func (s *mediaStatements) SelectMedia(
	ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName,
) (*types.MediaMetadata, error) {
	mediaMetadata := types.MediaMetadata{
		MediaID: mediaID,
		Origin:  mediaOrigin,
	}

	// Get database connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	row := db.Raw(
		s.selectMediaStmt,
		mediaMetadata.MediaID,
		mediaMetadata.Origin,
	).Row()

	err := row.Scan(
		&mediaMetadata.ContentType,
		&mediaMetadata.FileSizeBytes,
		&mediaMetadata.CreationTimestamp,
		&mediaMetadata.UploadName,
		&mediaMetadata.Base64Hash,
		&mediaMetadata.UserID,
	)
	return &mediaMetadata, err
}

func (s *mediaStatements) SelectMediaByHash(
	ctx context.Context, mediaHash types.Base64Hash, mediaOrigin spec.ServerName,
) (*types.MediaMetadata, error) {
	mediaMetadata := types.MediaMetadata{
		Base64Hash: mediaHash,
		Origin:     mediaOrigin,
	}

	// Get database connection
	db := s.cm.Connection(ctx, true)

	// Execute the query
	row := db.Raw(
		s.selectMediaByHashStmt,
		mediaMetadata.Base64Hash,
		mediaMetadata.Origin,
	).Row()

	err := row.Scan(
		&mediaMetadata.ContentType,
		&mediaMetadata.FileSizeBytes,
		&mediaMetadata.CreationTimestamp,
		&mediaMetadata.UploadName,
		&mediaMetadata.MediaID,
		&mediaMetadata.UserID,
	)
	return &mediaMetadata, err
}
