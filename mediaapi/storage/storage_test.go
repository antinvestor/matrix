package storage_test

import (
	"context"
	"github.com/pitabwire/frame"
	"reflect"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/mediaapi/storage"
	"github.com/antinvestor/matrix/mediaapi/types"
	"github.com/antinvestor/matrix/test"
)

func mustCreateDatabase(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) storage.Database {

	cm := sqlutil.NewConnectionManager(svc)
	db, err := storage.NewMediaAPIDatasource(ctx, cm)
	if err != nil {
		t.Fatalf("NewSyncServerDatasource returned %s", err)
	}
	return db
}
func TestMediaRepository(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		db := mustCreateDatabase(ctx, svc, t, testOpts)

		t.Run("can insert media & query media", func(t *testing.T) {
			metadata := &types.MediaMetadata{
				MediaID:       "testing",
				Origin:        "localhost",
				ContentType:   "image/png",
				FileSizeBytes: 10,
				UploadName:    "upload test",
				Base64Hash:    "dGVzdGluZw==",
				UserID:        "@alice:localhost",
			}
			if err := db.StoreMediaMetadata(ctx, metadata); err != nil {
				t.Fatalf("unable to store media metadata: %v", err)
			}
			// query by media id
			gotMetadata, err := db.GetMediaMetadata(ctx, metadata.MediaID, metadata.Origin)
			if err != nil {
				t.Fatalf("unable to query media metadata: %v", err)
			}
			if !reflect.DeepEqual(metadata, gotMetadata) {
				t.Fatalf("expected metadata %+v, got %v", metadata, gotMetadata)
			}
			// query by media hash
			gotMetadata, err = db.GetMediaMetadataByHash(ctx, metadata.Base64Hash, metadata.Origin)
			if err != nil {
				t.Fatalf("unable to query media metadata by hash: %v", err)
			}
			if !reflect.DeepEqual(metadata, gotMetadata) {
				t.Fatalf("expected metadata %+v, got %v", metadata, gotMetadata)
			}
		})
	})
}

func TestThumbnailsStorage(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateDatabase(ctx, svc, t, testOpts)

		t.Run("can insert thumbnails & query media", func(t *testing.T) {
			thumbnails := []*types.ThumbnailMetadata{
				{
					MediaMetadata: &types.MediaMetadata{
						MediaID:       "testing",
						Origin:        "localhost",
						ContentType:   "image/png",
						FileSizeBytes: 6,
					},
					ThumbnailSize: types.ThumbnailSize{
						Width:        5,
						Height:       5,
						ResizeMethod: types.Crop,
					},
				},
				{
					MediaMetadata: &types.MediaMetadata{
						MediaID:       "testing",
						Origin:        "localhost",
						ContentType:   "image/png",
						FileSizeBytes: 7,
					},
					ThumbnailSize: types.ThumbnailSize{
						Width:        1,
						Height:       1,
						ResizeMethod: types.Scale,
					},
				},
			}
			for i := range thumbnails {
				if err := db.StoreThumbnail(ctx, thumbnails[i]); err != nil {
					t.Fatalf("unable to store thumbnail metadata: %v", err)
				}
			}
			// query by single thumbnail
			gotMetadata, err := db.GetThumbnail(ctx,
				thumbnails[0].MediaMetadata.MediaID,
				thumbnails[0].MediaMetadata.Origin,
				thumbnails[0].ThumbnailSize.Width, thumbnails[0].ThumbnailSize.Height,
				thumbnails[0].ThumbnailSize.ResizeMethod,
			)
			if err != nil {
				t.Fatalf("unable to query thumbnail metadata: %v", err)
			}
			if !reflect.DeepEqual(thumbnails[0].MediaMetadata, gotMetadata.MediaMetadata) {
				t.Fatalf("expected metadata %+v, got %+v", thumbnails[0].MediaMetadata, gotMetadata.MediaMetadata)
			}
			if !reflect.DeepEqual(thumbnails[0].ThumbnailSize, gotMetadata.ThumbnailSize) {
				t.Fatalf("expected metadata %+v, got %+v", thumbnails[0].MediaMetadata, gotMetadata.MediaMetadata)
			}
			// query by all thumbnails
			gotMediadatas, err := db.GetThumbnails(ctx, thumbnails[0].MediaMetadata.MediaID, thumbnails[0].MediaMetadata.Origin)
			if err != nil {
				t.Fatalf("unable to query media metadata by hash: %v", err)
			}
			if len(gotMediadatas) != len(thumbnails) {
				t.Fatalf("expected %d stored thumbnail metadata, got %d", len(thumbnails), len(gotMediadatas))
			}
			for i := range gotMediadatas {
				// metadata may be returned in a different order than it was stored, perform a search
				metaDataMatches := func() bool {
					for _, t := range thumbnails {
						if reflect.DeepEqual(t.MediaMetadata, gotMediadatas[i].MediaMetadata) && reflect.DeepEqual(t.ThumbnailSize, gotMediadatas[i].ThumbnailSize) {
							return true
						}
					}
					return false
				}

				if !metaDataMatches() {
					t.Fatalf("expected metadata %+v, got %+v", thumbnails[i].MediaMetadata, gotMediadatas[i].MediaMetadata)

				}
			}
		})
	})
}
