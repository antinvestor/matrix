package config

import (
	"fmt"
)

type MediaAPI struct {
	Global *Global `yaml:"-"`

	// The MediaAPI database stores information about files uploaded and downloaded
	// by local users. It is only accessed by the MediaAPI.
	Database DatabaseOptions `yaml:"database,omitempty"`

	// The base path to where the media files will be stored. May be relative or absolute.
	BasePath Path `yaml:"base_path"`

	// The absolute base path to where media files will be stored.
	AbsBasePath Path `yaml:"-"`

	// The maximum file size in bytes that is allowed to be stored on this server.
	// Note: if max_file_size_bytes is set to 0, the size is unlimited.
	// Note: if max_file_size_bytes is not set, it will default to 10485760 (10MB)
	MaxFileSizeBytes FileSizeBytes `yaml:"max_file_size_bytes,omitempty"`

	// Whether to dynamically generate thumbnails on-the-fly if the requested resolution is not already generated
	DynamicThumbnails bool `yaml:"dynamic_thumbnails"`

	// The maximum number of simultaneous thumbnail generators. default: 10
	MaxThumbnailGenerators int `yaml:"max_thumbnail_generators"`

	// A list of thumbnail sizes to be pre-generated for downloaded remote / uploaded content
	ThumbnailSizes []ThumbnailSize `yaml:"thumbnail_sizes"`
}

// DefaultMaxFileSizeBytes defines the default file size allowed in transfers
var DefaultMaxFileSizeBytes = FileSizeBytes(10485760)

func (c *MediaAPI) Defaults(opts DefaultOpts) {
	c.MaxFileSizeBytes = DefaultMaxFileSizeBytes
	c.MaxThumbnailGenerators = 10
	c.ThumbnailSizes = []ThumbnailSize{
		{
			Width:        32,
			Height:       32,
			ResizeMethod: "crop",
		},
		{
			Width:        96,
			Height:       96,
			ResizeMethod: "crop",
		},
		{
			Width:        640,
			Height:       480,
			ResizeMethod: "scale",
		},
	}
	c.Database.Reference = "MediaAPI"
	c.Database.Prefix = opts.RandomnessPrefix
	c.Database.DatabaseURI = opts.DSDatabaseConn
	c.BasePath = "/tmp/media_store"

}

func (c *MediaAPI) Verify(configErrs *Errors) {
	checkNotEmpty(configErrs, "media_api.base_path", string(c.BasePath))
	checkPositive(configErrs, "media_api.max_file_size_bytes", int64(c.MaxFileSizeBytes))
	checkPositive(configErrs, "media_api.max_thumbnail_generators", int64(c.MaxThumbnailGenerators))

	for i, size := range c.ThumbnailSizes {
		checkPositive(configErrs, fmt.Sprintf("media_api.thumbnail_sizes[%d].width", i), int64(size.Width))
		checkPositive(configErrs, fmt.Sprintf("media_api.thumbnail_sizes[%d].height", i), int64(size.Height))
	}

	if c.Database.DatabaseURI == "" {
		checkNotEmpty(configErrs, "media_api.database.database_uri", string(c.Database.DatabaseURI))
	}
}
