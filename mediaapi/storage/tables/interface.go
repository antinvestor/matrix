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

package tables

import (
	"context"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/mediaapi/types"
)

type Thumbnails interface {
	InsertThumbnail(ctx context.Context, thumbnailMetadata *types.ThumbnailMetadata) error
	SelectThumbnail(
		ctx context.Context,
		mediaID types.MediaID, mediaOrigin spec.ServerName,
		width, height int,
		resizeMethod string,
	) (*types.ThumbnailMetadata, error)
	SelectThumbnails(
		ctx context.Context, mediaID types.MediaID,
		mediaOrigin spec.ServerName,
	) ([]*types.ThumbnailMetadata, error)
}

type MediaRepository interface {
	InsertMedia(ctx context.Context, mediaMetadata *types.MediaMetadata) error
	SelectMedia(ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName) (*types.MediaMetadata, error)
	SelectMediaByHash(
		ctx context.Context,
		mediaHash types.Base64Hash, mediaOrigin spec.ServerName,
	) (*types.MediaMetadata, error)
}
