// Copyright 2017 Vector Creations Ltd
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

package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/mediaapi/fileutils"
	"github.com/antinvestor/matrix/mediaapi/storage"
	"github.com/antinvestor/matrix/mediaapi/thumbnailer"
	"github.com/antinvestor/matrix/mediaapi/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
	"github.com/pkg/errors"
)

const mediaIDCharacters = "A-Za-z0-9_=-"

// Note: unfortunately regex.MustCompile() cannot be assigned to a const
var mediaIDRegex = regexp.MustCompile("^[" + mediaIDCharacters + "]+$")

// Regular expressions to help us cope with Content-Disposition parsing
var rfc2183 = regexp.MustCompile(`filename\=utf-8\"(.*)\"`)
var rfc6266 = regexp.MustCompile(`filename\*\=utf-8\'\'(.*)`)

// downloadRequest metadata included in or derivable from a download or thumbnail request
// https://matrix.org/docs/spec/client_server/r0.2.0.html#get-matrix-media-r0-download-servername-mediaid
// http://matrix.org/docs/spec/client_server/r0.2.0.html#get-matrix-media-r0-thumbnail-servername-mediaid
type downloadRequest struct {
	MediaMetadata      *types.MediaMetadata
	IsThumbnailRequest bool
	ThumbnailSize      types.ThumbnailSize
	Logger             *util.LogEntry
	DownloadFilename   string
	multipartResponse  bool // whether we need to return a multipart/mixed response (for requests coming in over federation)
	fedClient          fclient.FederationClient
	origin             spec.ServerName
}

// Taken from: https://github.com/matrix-org/synapse/blob/c3627d0f99ed5a23479305dc2bd0e71ca25ce2b1/synapse/media/_base.py#L53C1-L84
// A list of all content types that are "safe" to be rendered inline in a browser.
var allowInlineTypes = map[types.ContentType]struct{}{
	"text/css":            {},
	"text/plain":          {},
	"text/csv":            {},
	"application/json":    {},
	"application/ld+json": {},
	// We allow some media files deemed as safe, which comes from the matrix-react-sdk.
	// https://github.com/matrix-org/matrix-react-sdk/blob/a70fcfd0bcf7f8c85986da18001ea11597989a7c/src/utils/blobs.ts#L51
	// SVGs are *intentionally* omitted.
	"image/jpeg":      {},
	"image/gif":       {},
	"image/png":       {},
	"image/apng":      {},
	"image/webp":      {},
	"image/avif":      {},
	"video/mp4":       {},
	"video/webm":      {},
	"video/ogg":       {},
	"video/quicktime": {},
	"audio/mp4":       {},
	"audio/webm":      {},
	"audio/aac":       {},
	"audio/mpeg":      {},
	"audio/ogg":       {},
	"audio/wave":      {},
	"audio/wav":       {},
	"audio/x-wav":     {},
	"audio/x-pn-wav":  {},
	"audio/flac":      {},
	"audio/x-flac":    {},
}

// Download implements GET /download and GET /thumbnail
// Files from this server (i.e. origin == cfg.ServerName) are served directly
// Files from remote servers (i.e. origin != cfg.ServerName) are cached locally.
// If they are present in the cache, they are served directly.
// If they are not present in the cache, they are obtained from the remote server and
// simultaneously served back to the client and written into the cache.
func Download(
	w http.ResponseWriter,
	req *http.Request,
	origin spec.ServerName,
	mediaID types.MediaID,
	cfg *config.MediaAPI,
	db storage.Database,
	client *fclient.Client,
	fedClient fclient.FederationClient,
	activeRemoteRequests *types.ActiveRemoteRequests,
	activeThumbnailGeneration *types.ActiveThumbnailGeneration,
	isThumbnailRequest bool,
	customFilename string,
	federationRequest bool,
) {
	// This happens if we call Download for a federation request
	if federationRequest && origin == "" {
		origin = cfg.Global.ServerName
	}
	dReq := &downloadRequest{
		MediaMetadata: &types.MediaMetadata{
			MediaID: mediaID,
			Origin:  origin,
		},
		IsThumbnailRequest: isThumbnailRequest,
		Logger: util.Log(req.Context()).
			WithField("Origin", origin).
			WithField("MediaID", mediaID),
		DownloadFilename:  customFilename,
		multipartResponse: federationRequest,
		origin:            cfg.Global.ServerName,
		fedClient:         fedClient,
	}

	if dReq.IsThumbnailRequest {
		width, err := strconv.Atoi(req.FormValue("width"))
		if err != nil {
			width = -1
		}
		height, err := strconv.Atoi(req.FormValue("height"))
		if err != nil {
			height = -1
		}
		dReq.ThumbnailSize = types.ThumbnailSize{
			Width:        width,
			Height:       height,
			ResizeMethod: strings.ToLower(req.FormValue("method")),
		}
		dReq.Logger.
			WithField("RequestedWidth", dReq.ThumbnailSize.Width).
			WithField("RequestedHeight", dReq.ThumbnailSize.Height).
			WithField("RequestedResizeMethod", dReq.ThumbnailSize.ResizeMethod)
	}

	// request validation
	if resErr := dReq.Validate(); resErr != nil {
		dReq.jsonErrorResponse(w, *resErr)
		return
	}

	metadata, err := dReq.doDownload(
		req.Context(), w, cfg, db, client,
		activeRemoteRequests, activeThumbnailGeneration,
	)
	if err != nil {
		// If we bubbled up a os.PathError, e.g. no such file or directory, don't send
		// it to the client, be more generic.
		var perr *fs.PathError
		if errors.As(err, &perr) {
			dReq.Logger.WithError(err).Error("failed to open file")
			dReq.jsonErrorResponse(w, util.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("File not found"),
			})
			return
		}
		// TODO: Handle the fact we might have started writing the response
		dReq.jsonErrorResponse(w, util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Failed to download: " + err.Error()),
		})
		return
	}

	if metadata == nil {
		dReq.jsonErrorResponse(w, util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("File not found"),
		})
		return
	}

}

func (r *downloadRequest) jsonErrorResponse(w http.ResponseWriter, res util.JSONResponse) {
	// Marshal JSON response into raw bytes to send as the HTTP body
	resBytes, err := json.Marshal(res.JSON)
	if err != nil {
		r.Logger.WithError(err).Error("Failed to marshal JSONResponse")
		// this should never fail to be marshalled so drop err to the floor
		res = util.MessageResponse(http.StatusNotFound, "Download request failed: "+err.Error())
		resBytes, _ = json.Marshal(res.JSON)
	}

	// Set status code and write the body
	w.WriteHeader(res.Code)
	r.Logger.WithField("code", res.Code).
		WithField("response_len", len(resBytes)).Debug("Sending response")

	// we don't really care that much if we fail to write the error response
	w.Write(resBytes) // nolint: errcheck
}

// Validate validates the downloadRequest fields
func (r *downloadRequest) Validate() *util.JSONResponse {
	if !mediaIDRegex.MatchString(string(r.MediaMetadata.MediaID)) {
		return &util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound(fmt.Sprintf("mediaId must be a non-empty string using only characters in %v", mediaIDCharacters)),
		}
	}
	// Note: the origin will be validated either by comparison to the configured server name of this homeserver
	// or by a DNS SRV record lookup when creating a request for remote files
	if r.MediaMetadata.Origin == "" {
		return &util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("serverName must be a non-empty string"),
		}
	}

	if r.IsThumbnailRequest {
		if r.ThumbnailSize.Width <= 0 || r.ThumbnailSize.Height <= 0 {
			return &util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.Unknown("width and height must be greater than 0"),
			}
		}
		// Default method to scale if not set
		if r.ThumbnailSize.ResizeMethod == "" {
			r.ThumbnailSize.ResizeMethod = types.Scale
		}
		if r.ThumbnailSize.ResizeMethod != types.Crop && r.ThumbnailSize.ResizeMethod != types.Scale {
			return &util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.Unknown("method must be one of crop or scale"),
			}
		}
	}
	return nil
}

func (r *downloadRequest) doDownload(
	ctx context.Context,
	w http.ResponseWriter,
	cfg *config.MediaAPI,
	db storage.Database,
	client *fclient.Client,
	activeRemoteRequests *types.ActiveRemoteRequests,
	activeThumbnailGeneration *types.ActiveThumbnailGeneration,
) (*types.MediaMetadata, error) {
	// check if we have a record of the media in our database
	mediaMetadata, err := db.GetMediaMetadata(
		ctx, r.MediaMetadata.MediaID, r.MediaMetadata.Origin,
	)
	if err != nil {
		return nil, fmt.Errorf("db.GetMediaMetadata: %w", err)
	}
	if mediaMetadata == nil {
		if r.MediaMetadata.Origin == cfg.Global.ServerName {
			// If we do not have a record and the origin is local, the file is not found
			return nil, nil
		}
		// If we do not have a record and the origin is remote, we need to fetch it and respond with that file
		resErr := r.getRemoteFile(
			ctx, client, cfg, db, activeRemoteRequests, activeThumbnailGeneration,
		)
		if resErr != nil {
			return nil, resErr
		}
	} else {
		// If we have a record, we can respond from the local file
		r.MediaMetadata = mediaMetadata
	}
	return r.respondFromLocalFile(
		ctx, w, cfg.AbsBasePath, activeThumbnailGeneration,
		cfg.MaxThumbnailGenerators, db,
		cfg.DynamicThumbnails, cfg.ThumbnailSizes,
	)
}

// respondFromLocalFile reads a file from local storage and writes it to the http.ResponseWriter
// If no file was found then returns nil, nil
func (r *downloadRequest) respondFromLocalFile(
	ctx context.Context,
	w http.ResponseWriter,
	absBasePath config.Path,
	activeThumbnailGeneration *types.ActiveThumbnailGeneration,
	maxThumbnailGenerators int,
	db storage.Database,
	dynamicThumbnails bool,
	thumbnailSizes []config.ThumbnailSize,
) (*types.MediaMetadata, error) {
	filePath, err := fileutils.GetPathFromBase64Hash(r.MediaMetadata.Base64Hash, absBasePath)
	if err != nil {
		return nil, fmt.Errorf("fileutils.GetPathFromBase64Hash: %w", err)
	}
	file, err := os.Open(filePath)
	defer file.Close() // nolint: errcheck, staticcheck
	if err != nil {
		return nil, fmt.Errorf("os.Open: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("file.Stat: %w", err)
	}

	if r.MediaMetadata.FileSizeBytes > 0 && int64(r.MediaMetadata.FileSizeBytes) != stat.Size() {
		r.Logger.WithField("fileSizeDatabase", r.MediaMetadata.FileSizeBytes).
			WithField("fileSizeDisk", stat.Size()).
			Warn("File size in database and on-disk differ.")
		return nil, errors.New("file size in database and on-disk differ")
	}

	var responseFile *os.File
	var responseMetadata *types.MediaMetadata
	if r.IsThumbnailRequest {
		thumbFile, thumbMetadata, resErr := r.getThumbnailFile(
			ctx, types.Path(filePath), activeThumbnailGeneration, maxThumbnailGenerators,
			db, dynamicThumbnails, thumbnailSizes,
		)
		if thumbFile != nil {
			defer thumbFile.Close() // nolint: errcheck
		}
		if resErr != nil {
			return nil, resErr
		}
		if thumbFile == nil {
			r.Logger.WithField("UploadName", r.MediaMetadata.UploadName).
				WithField("Base64Hash", r.MediaMetadata.Base64Hash).
				WithField("FileSizeBytes", r.MediaMetadata.FileSizeBytes).
				WithField("ContentType", r.MediaMetadata.ContentType).
				Debug("No good thumbnail found. Responding with original file.")
			responseFile = file
			responseMetadata = r.MediaMetadata
		} else {
			r.Logger.Debug("Responding with thumbnail")
			responseFile = thumbFile
			responseMetadata = thumbMetadata.MediaMetadata
		}
	} else {
		r.Logger.WithField("UploadName", r.MediaMetadata.UploadName).
			WithField("Base64Hash", r.MediaMetadata.Base64Hash).
			WithField("FileSizeBytes", r.MediaMetadata.FileSizeBytes).
			WithField("ContentType", r.MediaMetadata.ContentType).
			Debug("Responding with file")
		responseFile = file
		responseMetadata = r.MediaMetadata
		if err = r.addDownloadFilenameToHeaders(w, responseMetadata); err != nil {
			return nil, err
		}
	}

	w.Header().Set("Content-Type", string(responseMetadata.ContentType))
	w.Header().Set("Content-Length", strconv.FormatInt(int64(responseMetadata.FileSizeBytes), 10))
	contentSecurityPolicy := "default-src 'none';" +
		" script-src 'none';" +
		" plugin-types application/pdf;" +
		" style-src 'unsafe-inline';" +
		" object-src 'self';"

	if !r.multipartResponse {
		w.Header().Set("Content-Security-Policy", contentSecurityPolicy)
		if _, err = io.Copy(w, responseFile); err != nil {
			return nil, fmt.Errorf("io.Copy: %w", err)
		}
	} else {
		var written int64
		written, err = multipartResponse(w, r, string(responseMetadata.ContentType), responseFile)
		if err != nil {
			return nil, err
		}
		responseMetadata.FileSizeBytes = types.FileSizeBytes(written)
	}
	return responseMetadata, nil
}

func multipartResponse(w http.ResponseWriter, r *downloadRequest, contentType string, responseFile io.Reader) (int64, error) {
	mw := multipart.NewWriter(w)
	// Update the header to be multipart/mixed; boundary=$randomBoundary
	w.Header().Set("Content-Type", "multipart/mixed; boundary="+mw.Boundary())
	w.Header().Del("Content-Length") // let Go handle the content length
	defer func() {
		if err := mw.Close(); err != nil {
			r.Logger.WithError(err).Error("Failed to close multipart writer")
		}
	}()

	// JSON object part
	jsonWriter, err := mw.CreatePart(textproto.MIMEHeader{
		"Content-Type": {"application/json"},
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create json writer: %w", err)
	}
	if _, err = jsonWriter.Write([]byte("{}")); err != nil {
		return 0, fmt.Errorf("failed to write to json writer: %w", err)
	}

	// media part
	mediaWriter, err := mw.CreatePart(textproto.MIMEHeader{
		"Content-Type": {contentType},
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create media writer: %w", err)
	}
	return io.Copy(mediaWriter, responseFile)
}

func (r *downloadRequest) addDownloadFilenameToHeaders(
	w http.ResponseWriter,
	responseMetadata *types.MediaMetadata,
) error {
	// If the requestor supplied a filename to name the download then
	// use that, otherwise use the filename from the response metadata.
	filename := string(responseMetadata.UploadName)
	if r.DownloadFilename != "" {
		filename = r.DownloadFilename
	}

	if len(filename) == 0 {
		w.Header().Set("Content-Disposition", contentDispositionFor(""))
		return nil
	}

	unescaped, err := url.PathUnescape(filename)
	if err != nil {
		return fmt.Errorf("url.PathUnescape: %w", err)
	}

	isASCII := true // Is the string ASCII or UTF-8?
	quote := ``     // Encloses the string (ASCII only)
	for i := 0; i < len(unescaped); i++ {
		if unescaped[i] > unicode.MaxASCII {
			isASCII = false
		}
		if unescaped[i] == 0x20 || unescaped[i] == 0x3B {
			// If the filename contains a space or a semicolon, which
			// are special characters in Content-Disposition
			quote = `"`
		}
	}

	// We don't necessarily want a full escape as the Content-Disposition
	// can take many of the characters that PathEscape would otherwise and
	// browser support for encoding is a bit wild, so we'll escape only
	// the characters that we know will mess up the parsing of the
	// Content-Disposition header elements themselves
	unescaped = strings.ReplaceAll(unescaped, `\`, `\\"`)
	unescaped = strings.ReplaceAll(unescaped, `"`, `\"`)

	disposition := contentDispositionFor(responseMetadata.ContentType)
	if isASCII {
		// For ASCII filenames, we should only quote the filename if
		// it needs to be done, e.g. it contains a space or a character
		// that would otherwise be parsed as a control character in the
		// Content-Disposition header
		w.Header().Set("Content-Disposition", fmt.Sprintf(
			`%s; filename=%s%s%s`,
			disposition, quote, unescaped, quote,
		))
	} else {
		// For UTF-8 filenames, we quote always, as that's the standard
		w.Header().Set("Content-Disposition", fmt.Sprintf(
			`%s; filename*=utf-8''%s`,
			disposition, url.QueryEscape(unescaped),
		))
	}

	return nil
}

// Note: Thumbnail generation may be ongoing asynchronously.
// If no thumbnail was found then returns nil, nil, nil
func (r *downloadRequest) getThumbnailFile(
	ctx context.Context,
	filePath types.Path,
	activeThumbnailGeneration *types.ActiveThumbnailGeneration,
	maxThumbnailGenerators int,
	db storage.Database,
	dynamicThumbnails bool,
	thumbnailSizes []config.ThumbnailSize,
) (*os.File, *types.ThumbnailMetadata, error) {
	var thumbnail *types.ThumbnailMetadata
	var err error

	if dynamicThumbnails {
		thumbnail, err = r.generateThumbnail(
			ctx, filePath, r.ThumbnailSize, activeThumbnailGeneration,
			maxThumbnailGenerators, db,
		)
		if err != nil {
			return nil, nil, err
		}
	}
	// If dynamicThumbnails is true but there are too many thumbnails being actively generated, we can fall back
	// to trying to use a pre-generated thumbnail
	if thumbnail == nil {
		var thumbnails []*types.ThumbnailMetadata
		thumbnails, err = db.GetThumbnails(
			ctx, r.MediaMetadata.MediaID, r.MediaMetadata.Origin,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("db.GetThumbnails: %w", err)
		}

		// If we get a thumbnailSize, a pre-generated thumbnail would be best but it is not yet generated.
		// If we get a thumbnail, we're done.
		var thumbnailSize *types.ThumbnailSize
		thumbnail, thumbnailSize = thumbnailer.SelectThumbnail(r.ThumbnailSize, thumbnails, thumbnailSizes)
		// If dynamicThumbnails is true and we are not over-loaded then we would have generated what was requested above.
		// So we don't try to generate a pre-generated thumbnail here.
		if thumbnailSize != nil && !dynamicThumbnails {
			r.Logger.WithField("Width", thumbnailSize.Width).
				WithField("Height", thumbnailSize.Height).
				WithField("ResizeMethod", thumbnailSize.ResizeMethod).
				Debug("Pre-generating thumbnail for immediate response.")
			thumbnail, err = r.generateThumbnail(
				ctx, filePath, *thumbnailSize, activeThumbnailGeneration,
				maxThumbnailGenerators, db,
			)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	if thumbnail == nil {
		return nil, nil, nil
	}
	r.Logger = r.Logger.
		WithField("Width", thumbnail.ThumbnailSize.Width).
		WithField("Height", thumbnail.ThumbnailSize.Height).
		WithField("ResizeMethod", thumbnail.ThumbnailSize.ResizeMethod).
		WithField("FileSizeBytes", thumbnail.MediaMetadata.FileSizeBytes).
		WithField("ContentType", thumbnail.MediaMetadata.ContentType)
	thumbPath := string(thumbnailer.GetThumbnailPath(types.Path(filePath), thumbnail.ThumbnailSize))
	thumbFile, err := os.Open(string(thumbPath))
	if err != nil {
		if thumbFile != nil {
			_ = thumbFile.Close() // nolint: errcheck
		}
		return nil, nil, fmt.Errorf("os.Open: %w", err)
	}
	thumbStat, err := thumbFile.Stat()
	if err != nil {
		_ = thumbFile.Close() // nolint: errcheck
		return nil, nil, fmt.Errorf("thumbFile.Stat: %w", err)
	}
	if types.FileSizeBytes(thumbStat.Size()) != thumbnail.MediaMetadata.FileSizeBytes {
		_ = thumbFile.Close() // nolint: errcheck
		return nil, nil, errors.New("thumbnail file sizes on disk and in database differ")
	}
	return thumbFile, thumbnail, nil
}

func (r *downloadRequest) generateThumbnail(
	ctx context.Context,
	filePath types.Path,
	thumbnailSize types.ThumbnailSize,
	activeThumbnailGeneration *types.ActiveThumbnailGeneration,
	maxThumbnailGenerators int,
	db storage.Database,
) (*types.ThumbnailMetadata, error) {
	r.Logger.WithField("Width", thumbnailSize.Width).
		WithField("Height", thumbnailSize.Height).
		WithField("ResizeMethod", thumbnailSize.ResizeMethod)
	busy, err := thumbnailer.GenerateThumbnail(
		ctx, filePath, thumbnailSize, r.MediaMetadata,
		activeThumbnailGeneration, maxThumbnailGenerators, db, r.Logger,
	)
	if err != nil {
		return nil, fmt.Errorf("thumbnailer.GenerateThumbnail: %w", err)
	}
	if busy {
		return nil, nil
	}
	var thumbnail *types.ThumbnailMetadata
	thumbnail, err = db.GetThumbnail(
		ctx, r.MediaMetadata.MediaID, r.MediaMetadata.Origin,
		thumbnailSize.Width, thumbnailSize.Height, thumbnailSize.ResizeMethod,
	)
	if err != nil {
		return nil, fmt.Errorf("db.GetThumbnail: %w", err)
	}
	return thumbnail, nil
}

// getRemoteFile fetches the remote file and caches it locally
// A hash map of active remote requests to a struct containing a sync.Cond is used to only download remote files once,
// regardless of how many download requests are received.
// Note: The named errorResponse return variable is used in a deferred broadcast of the metadata and error response to waiting goroutines.
func (r *downloadRequest) getRemoteFile(
	ctx context.Context,
	client *fclient.Client,
	cfg *config.MediaAPI,
	db storage.Database,
	activeRemoteRequests *types.ActiveRemoteRequests,
	activeThumbnailGeneration *types.ActiveThumbnailGeneration,
) (errorResponse error) {
	// Note: getMediaMetadataFromActiveRequest uses mutexes and conditions from activeRemoteRequests
	mediaMetadata, resErr := r.getMediaMetadataFromActiveRequest(activeRemoteRequests)
	if resErr != nil {
		return resErr
	} else if mediaMetadata != nil {
		// If we got metadata from an active request, we can respond from the local file
		r.MediaMetadata = mediaMetadata
	} else {
		// Note: This is an active request that MUST broadcastMediaMetadata to wake up waiting goroutines!
		// Note: broadcastMediaMetadata uses mutexes and conditions from activeRemoteRequests
		defer func() {
			// Note: errorResponse is the named return variable so we wrap this in a closure to re-evaluate the arguments at defer-time
			if err := recover(); err != nil {
				r.broadcastMediaMetadata(activeRemoteRequests, errors.New("paniced"))
				panic(err)
			}
			r.broadcastMediaMetadata(activeRemoteRequests, errorResponse)
		}()

		// check if we have a record of the media in our database
		mediaMetadata, err := db.GetMediaMetadata(
			ctx, r.MediaMetadata.MediaID, r.MediaMetadata.Origin,
		)
		if err != nil {
			return fmt.Errorf("db.GetMediaMetadata: %w", err)
		}

		if mediaMetadata == nil {
			// If we do not have a record, we need to fetch the remote file first and then respond from the local file
			err := r.fetchRemoteFileAndStoreMetadata(
				ctx, client,
				cfg.AbsBasePath, cfg.MaxFileSizeBytes, db,
				cfg.ThumbnailSizes, activeThumbnailGeneration,
				cfg.MaxThumbnailGenerators,
			)
			if err != nil {
				r.Logger.WithError(err).Error("r.fetchRemoteFileAndStoreMetadata: failed to fetch remote file")
				return err
			}
		} else {
			// If we have a record, we can respond from the local file
			r.MediaMetadata = mediaMetadata
		}
	}
	return nil
}

func (r *downloadRequest) getMediaMetadataFromActiveRequest(activeRemoteRequests *types.ActiveRemoteRequests) (*types.MediaMetadata, error) {
	// Check if there is an active remote request for the file
	mxcURL := "mxc://" + string(r.MediaMetadata.Origin) + "/" + string(r.MediaMetadata.MediaID)

	activeRemoteRequests.Lock()
	defer activeRemoteRequests.Unlock()

	if activeRemoteRequestResult, ok := activeRemoteRequests.MXCToResult[mxcURL]; ok {
		r.Logger.Debug("Waiting for another goroutine to fetch the remote file.")

		// NOTE: Wait unlocks and locks again internally. There is still a deferred Unlock() that will unlock this.
		activeRemoteRequestResult.Cond.Wait()
		if activeRemoteRequestResult.Error != nil {
			return nil, activeRemoteRequestResult.Error
		}

		if activeRemoteRequestResult.MediaMetadata == nil {
			return nil, nil
		}

		return activeRemoteRequestResult.MediaMetadata, nil
	}

	// No active remote request so create one
	activeRemoteRequests.MXCToResult[mxcURL] = &types.RemoteRequestResult{
		Cond: &sync.Cond{L: activeRemoteRequests},
	}

	return nil, nil
}

// broadcastMediaMetadata broadcasts the media metadata and error response to waiting goroutines
// Only the owner of the activeRemoteRequestResult for this origin and media ID should call this function.
func (r *downloadRequest) broadcastMediaMetadata(activeRemoteRequests *types.ActiveRemoteRequests, err error) {
	activeRemoteRequests.Lock()
	defer activeRemoteRequests.Unlock()
	mxcURL := "mxc://" + string(r.MediaMetadata.Origin) + "/" + string(r.MediaMetadata.MediaID)
	if activeRemoteRequestResult, ok := activeRemoteRequests.MXCToResult[mxcURL]; ok {
		r.Logger.Debug("Signalling other goroutines waiting for this goroutine to fetch the file.")
		activeRemoteRequestResult.MediaMetadata = r.MediaMetadata
		activeRemoteRequestResult.Error = err
		activeRemoteRequestResult.Cond.Broadcast()
	}
	delete(activeRemoteRequests.MXCToResult, mxcURL)
}

// fetchRemoteFileAndStoreMetadata fetches the file from the remote server and stores its metadata in the database
func (r *downloadRequest) fetchRemoteFileAndStoreMetadata(
	ctx context.Context,
	client *fclient.Client,
	absBasePath config.Path,
	maxFileSizeBytes config.FileSizeBytes,
	db storage.Database,
	thumbnailSizes []config.ThumbnailSize,
	activeThumbnailGeneration *types.ActiveThumbnailGeneration,
	maxThumbnailGenerators int,
) error {
	finalPath, duplicate, err := r.fetchRemoteFile(
		ctx, client, absBasePath, maxFileSizeBytes,
	)
	if err != nil {
		return err
	}

	r.Logger.WithField("Base64Hash", r.MediaMetadata.Base64Hash).
		WithField("UploadName", r.MediaMetadata.UploadName).
		WithField("FileSizeBytes", r.MediaMetadata.FileSizeBytes).
		WithField("ContentType", r.MediaMetadata.ContentType).
		Debug("Storing file metadata to media repository database")

	// FIXME: timeout db request
	if err := db.StoreMediaMetadata(ctx, r.MediaMetadata); err != nil {
		// If the file is a duplicate (has the same hash as an existing file) then
		// there is valid metadata in the database for that file. As such we only
		// remove the file if it is not a duplicate.
		if !duplicate {
			finalDir := filepath.Dir(string(finalPath))
			fileutils.RemoveDir(types.Path(finalDir), r.Logger)
		}
		// NOTE: It should really not be possible to fail the uniqueness test here so
		// there is no need to handle that separately
		return errors.New("failed to store file metadata in Cm")
	}

	go func() {
		busy, err := thumbnailer.GenerateThumbnails(
			ctx, finalPath, thumbnailSizes, r.MediaMetadata,
			activeThumbnailGeneration, maxThumbnailGenerators, db, r.Logger,
		)
		if err != nil {
			r.Logger.WithError(err).Warn("Error generating thumbnails")
		}
		if busy {
			r.Logger.Warn("Maximum number of active thumbnail generators reached. Skipping pre-generation.")
		}
	}()

	r.Logger.WithField("UploadName", r.MediaMetadata.UploadName).
		WithField("Base64Hash", r.MediaMetadata.Base64Hash).
		WithField("FileSizeBytes", r.MediaMetadata.FileSizeBytes).
		WithField("ContentType", r.MediaMetadata.ContentType).
		Debug("Remote file cached")

	return nil
}

func (r *downloadRequest) GetContentLengthAndReader(contentLengthHeader string, reader io.ReadCloser, maxFileSizeBytes config.FileSizeBytes) (int64, io.Reader, error) {
	var contentLength int64

	if contentLengthHeader != "" {
		// A Content-Length header is provided. Let's try to parse it.
		parsedLength, parseErr := strconv.ParseInt(contentLengthHeader, 10, 64)
		if parseErr != nil {
			r.Logger.WithError(parseErr).Warn("Failed to parse content length")
			return 0, nil, fmt.Errorf("strconv.ParseInt: %w", parseErr)
		}
		if maxFileSizeBytes > 0 && parsedLength > int64(maxFileSizeBytes) {
			return 0, nil, fmt.Errorf(
				"remote file size (%d bytes) exceeds locally configured max media size (%d bytes)",
				parsedLength, maxFileSizeBytes,
			)
		}

		// We successfully parsed the Content-Length, so we'll return a limited
		// reader that restricts us to reading only up to this size.
		reader = io.NopCloser(io.LimitReader(reader, parsedLength))
		contentLength = parsedLength
	} else {
		// Content-Length header is missing. If we have a maximum file size
		// configured then we'll just make sure that the reader is limited to
		// that size. We'll return a zero content length, but that's OK, since
		// ultimately it will get rewritten later when the temp file is written
		// to disk.
		if maxFileSizeBytes > 0 {
			reader = io.NopCloser(io.LimitReader(reader, int64(maxFileSizeBytes)))
		}
		contentLength = 0
	}

	return contentLength, reader, nil
}

// mediaMeta contains information about a multipart media response.
// TODO: extend once something is defined.
type mediaMeta struct{}

// nolint: gocyclo
func (r *downloadRequest) fetchRemoteFile(
	ctx context.Context,
	client *fclient.Client,
	absBasePath config.Path,
	maxFileSizeBytes config.FileSizeBytes,
) (types.Path, bool, error) {
	r.Logger.Debug("Fetching remote file")

	// Attempt to download via authenticated media endpoint
	isAuthed := true
	resp, err := r.fedClient.DownloadMedia(ctx, r.origin, r.MediaMetadata.Origin, string(r.MediaMetadata.MediaID))
	if err != nil || (resp != nil && resp.StatusCode != http.StatusOK) {
		isAuthed = false
		// try again on the unauthed endpoint
		// create request for remote file
		resp, err = client.CreateMediaDownloadRequest(ctx, r.MediaMetadata.Origin, string(r.MediaMetadata.MediaID))
		if err != nil || (resp != nil && resp.StatusCode != http.StatusOK) {
			if resp != nil && resp.StatusCode == http.StatusNotFound {
				return "", false, fmt.Errorf("file with media ID %q does not exist on %s", r.MediaMetadata.MediaID, r.MediaMetadata.Origin)
			}
			return "", false, fmt.Errorf("file with media ID %q could not be downloaded from %s: %w", r.MediaMetadata.MediaID, r.MediaMetadata.Origin, err)
		}
	}
	defer resp.Body.Close() // nolint: errcheck

	// If this wasn't a multipart response, set the Content-Type now. Will be overwritten
	// by the multipart Content-Type below.
	r.MediaMetadata.ContentType = types.ContentType(resp.Header.Get("Content-Type"))

	var contentLength int64
	var reader io.Reader
	var parseErr error
	if isAuthed {
		contentLength, reader, parseErr = parseMultipartResponse(r, resp, maxFileSizeBytes)
	} else {
		// The reader returned here will be limited either by the Content-Length
		// and/or the configured maximum media size.
		contentLength, reader, parseErr = r.GetContentLengthAndReader(resp.Header.Get("Content-Length"), resp.Body, maxFileSizeBytes)
	}

	if parseErr != nil {
		return "", false, parseErr
	}

	if maxFileSizeBytes > 0 && contentLength > int64(maxFileSizeBytes) {
		// TODO: Bubble up this as a 413
		return "", false, fmt.Errorf("remote file is too large (%v > %v bytes)", contentLength, maxFileSizeBytes)
	}

	r.MediaMetadata.FileSizeBytes = types.FileSizeBytes(contentLength)

	dispositionHeader := resp.Header.Get("Content-Disposition")
	if _, params, e := mime.ParseMediaType(dispositionHeader); e == nil {
		if params["filename"] != "" {
			r.MediaMetadata.UploadName = types.Filename(params["filename"])
		} else if params["filename*"] != "" {
			r.MediaMetadata.UploadName = types.Filename(params["filename*"])
		}
	} else {
		if matches := rfc6266.FindStringSubmatch(dispositionHeader); len(matches) > 1 {
			// Always prefer the RFC6266 UTF-8 name if possible
			r.MediaMetadata.UploadName = types.Filename(matches[1])
		} else if matches := rfc2183.FindStringSubmatch(dispositionHeader); len(matches) > 1 {
			// Otherwise, see if an RFC2183 name was provided (ASCII only)
			r.MediaMetadata.UploadName = types.Filename(matches[1])
		}
	}

	r.Logger.Debug("Transferring remote file")

	// The file data is hashed but is NOT used as the MediaID, unlike in Upload. The hash is useful as a
	// method of deduplicating files to save storage, as well as a way to conduct
	// integrity checks on the file data in the repository.
	// Data is truncated to maxFileSizeBytes. Content-Length was reported as 0 < Content-Length <= maxFileSizeBytes so this is OK.
	hash, bytesWritten, tmpDir, err := fileutils.WriteTempFile(ctx, reader, absBasePath)
	if err != nil {
		r.Logger.WithError(err).WithField("MaxFileSizeBytes", maxFileSizeBytes).Warn("Error while downloading file from remote server")
		return "", false, errors.New("file could not be downloaded from remote server")
	}

	r.Logger.Debug("Remote file transferred")

	// It's possible the bytesWritten to the temporary file is different to the reported Content-Length from the remote
	// request's response. bytesWritten is therefore used as it is what would be sent to clients when reading from the local
	// file.
	r.MediaMetadata.FileSizeBytes = types.FileSizeBytes(bytesWritten)
	r.MediaMetadata.Base64Hash = hash

	// The database is the source of truth so we need to have moved the file first
	finalPath, duplicate, err := fileutils.MoveFileWithHashCheck(tmpDir, r.MediaMetadata, absBasePath, r.Logger)
	if err != nil {
		return "", false, fmt.Errorf("fileutils.MoveFileWithHashCheck: %w", err)
	}
	if duplicate {
		r.Logger.WithField("dst", finalPath).Debug("File was stored previously - discarding duplicate")
		// Continue on to store the metadata in the database
	}

	return types.Path(finalPath), duplicate, nil
}

func parseMultipartResponse(r *downloadRequest, resp *http.Response, maxFileSizeBytes config.FileSizeBytes) (int64, io.Reader, error) {
	_, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return 0, nil, err
	}
	if params["boundary"] == "" {
		return 0, nil, fmt.Errorf("no boundary header found on media %s from %s", r.MediaMetadata.MediaID, r.MediaMetadata.Origin)
	}
	mr := multipart.NewReader(resp.Body, params["boundary"])

	// Get the first, JSON, part
	p, err := mr.NextPart()
	if err != nil {
		return 0, nil, err
	}
	defer p.Close() // nolint: errcheck

	if p.Header.Get("Content-Type") != "application/json" {
		return 0, nil, fmt.Errorf("first part of the response must be application/json")
	}
	// Try to parse media meta information
	meta := mediaMeta{}
	if err = json.NewDecoder(p).Decode(&meta); err != nil {
		return 0, nil, err
	}
	defer p.Close() // nolint: errcheck

	// Get the actual media content
	p, err = mr.NextPart()
	if err != nil {
		return 0, nil, err
	}

	redirect := p.Header.Get("Location")
	if redirect != "" {
		return 0, nil, fmt.Errorf("location header is not yet supported")
	}

	contentLength, reader, err := r.GetContentLengthAndReader(p.Header.Get("Content-Length"), p, maxFileSizeBytes)
	// For multipart requests, we need to get the Content-Type of the second part, which is the actual media
	r.MediaMetadata.ContentType = types.ContentType(p.Header.Get("Content-Type"))
	return contentLength, reader, err
}

// contentDispositionFor returns the Content-Disposition for a given
// content type.
func contentDispositionFor(contentType types.ContentType) string {
	if _, ok := allowInlineTypes[contentType]; ok {
		return "inline"
	}
	return "attachment"
}
