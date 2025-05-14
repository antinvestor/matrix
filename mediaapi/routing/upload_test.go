package routing

import (
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/antinvestor/matrix/test"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/mediaapi/storage"
	"github.com/antinvestor/matrix/mediaapi/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
	log "github.com/sirupsen/logrus"
)

func Test_uploadRequest_doUpload(t *testing.T) {
	type fields struct {
		MediaMetadata *types.MediaMetadata
		Logger        *log.Entry
	}
	type args struct {
		reqReader                 io.Reader
		cfg                       *config.MediaAPI
		activeThumbnailGeneration *types.ActiveThumbnailGeneration
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Errorf("failed to get current working directory: %v", err)
	}

	maxSize := config.FileSizeBytes(8)
	logger := log.New().WithField("mediaapi", "test")
	testdataPath := filepath.Join(wd, "./testdata")

	cfg := &config.MediaAPI{
		MaxFileSizeBytes:  maxSize,
		BasePath:          config.Path(testdataPath),
		AbsBasePath:       config.Path(testdataPath),
		DynamicThumbnails: false,
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		want   *util.JSONResponse
	}{
		{
			name: "upload ok",
			args: args{
				reqReader: strings.NewReader("test"),
				cfg:       cfg,
			},
			fields: fields{
				Logger: logger,
				MediaMetadata: &types.MediaMetadata{
					MediaID:    "1337",
					UploadName: "test ok",
				},
			},
			want: nil,
		},
		{
			name: "upload ok (exact size)",
			args: args{
				reqReader: strings.NewReader("testtest"),
				cfg:       cfg,
			},
			fields: fields{
				Logger: logger,
				MediaMetadata: &types.MediaMetadata{
					MediaID:    "1338",
					UploadName: "test ok (exact size)",
				},
			},
			want: nil,
		},
		{
			name: "upload not ok",
			args: args{
				reqReader: strings.NewReader("test test test"),
				cfg:       cfg,
			},
			fields: fields{
				Logger: logger,
				MediaMetadata: &types.MediaMetadata{
					MediaID:    "1339",
					UploadName: "test fail",
				},
			},
			want: requestEntityTooLargeJSONResponse(maxSize),
		},
		{
			name: "upload ok with unlimited filesize",
			args: args{
				reqReader: strings.NewReader("test test test"),
				cfg: &config.MediaAPI{
					MaxFileSizeBytes:  config.FileSizeBytes(0),
					BasePath:          config.Path(testdataPath),
					AbsBasePath:       config.Path(testdataPath),
					DynamicThumbnails: false,
				},
			},
			fields: fields{
				Logger: logger,
				MediaMetadata: &types.MediaMetadata{
					MediaID:    "1339",
					UploadName: "test fail",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

				ctx, svc, _ := testrig.Init(t, testOpts)
				defer svc.Stop(ctx)

				cm := sqlutil.NewConnectionManager(svc)
				db, err0 := storage.NewMediaAPIDatasource(ctx, cm)
				if err0 != nil {
					t.Errorf("error opening mediaapi database: %v", err0)
				}

				r := &uploadRequest{
					MediaMetadata: tt.fields.MediaMetadata,
					Logger:        tt.fields.Logger,
				}
				if got := r.doUpload(ctx, tt.args.reqReader, tt.args.cfg, db, tt.args.activeThumbnailGeneration); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("doUpload() = %+v, want %+v", got, tt.want)
				}
			})
		})
	}
}
