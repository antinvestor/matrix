// Copyright 2022 The Global.org Foundation C.I.C.
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

package shared

import (
	"context"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/mediaapi/storage/tables"
	"github.com/antinvestor/matrix/mediaapi/types"
)

type Database struct {
	Cm              sqlutil.ConnectionManager
	Writer          sqlutil.Writer
	MediaRepository tables.MediaRepository
	Thumbnails      tables.Thumbnails
}

// StoreMediaMetadata inserts the metadata about the uploaded media into the database.
// Returns an error if the combination of MediaID and Origin are not unique in the table.
func (d *Database) StoreMediaMetadata(ctx context.Context, mediaMetadata *types.MediaMetadata) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.MediaRepository.InsertMedia(ctx, mediaMetadata)
	})
}

// GetMediaMetadata returns metadata about media stored on this server.
// The media could have been uploaded to this server or fetched from another server and cached here.
// Returns nil metadata if there is no metadata associated with this media.
func (d *Database) GetMediaMetadata(ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName) (*types.MediaMetadata, error) {
	mediaMetadata, err := d.MediaRepository.SelectMedia(ctx, mediaID, mediaOrigin)
	if sqlutil.ErrorIsNoRows(err) {
		return nil, nil
	}
	return mediaMetadata, err
}

// GetMediaMetadataByHash returns metadata about media stored on this server.
// The media could have been uploaded to this server or fetched from another server and cached here.
// Returns nil metadata if there is no metadata associated with this media.
func (d *Database) GetMediaMetadataByHash(ctx context.Context, mediaHash types.Base64Hash, mediaOrigin spec.ServerName) (*types.MediaMetadata, error) {
	mediaMetadata, err := d.MediaRepository.SelectMediaByHash(ctx, mediaHash, mediaOrigin)
	if sqlutil.ErrorIsNoRows(err) {
		return nil, nil
	}
	return mediaMetadata, err
}

// StoreThumbnail inserts the metadata about the thumbnail into the database.
// Returns an error if the combination of MediaID and Origin are not unique in the table.
func (d *Database) StoreThumbnail(ctx context.Context, thumbnailMetadata *types.ThumbnailMetadata) error {
	return d.Cm.Do(ctx, func(ctx context.Context) error {
		return d.Thumbnails.InsertThumbnail(ctx, thumbnailMetadata)
	})
}

// GetThumbnail returns metadata about a specific thumbnail.
// The media could have been uploaded to this server or fetched from another server and cached here.
// Returns nil metadata if there is no metadata associated with this thumbnail.
func (d *Database) GetThumbnail(ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName, width, height int, resizeMethod string) (*types.ThumbnailMetadata, error) {
	metadata, err := d.Thumbnails.SelectThumbnail(ctx, mediaID, mediaOrigin, width, height, resizeMethod)
	if sqlutil.ErrorIsNoRows(err) {
		return nil, nil
	}
	return metadata, err
}

// GetThumbnails returns metadata about all thumbnails for a specific media stored on this server.
// The media could have been uploaded to this server or fetched from another server and cached here.
// Returns nil metadata if there are no thumbnails associated with this media.
func (d *Database) GetThumbnails(ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName) ([]*types.ThumbnailMetadata, error) {
	metadatas, err := d.Thumbnails.SelectThumbnails(ctx, mediaID, mediaOrigin)
	if sqlutil.ErrorIsNoRows(err) {
		return nil, nil
	}
	return metadatas, err
}
