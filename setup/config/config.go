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

package config

import (
	"bytes"
	"encoding/pem"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	jaegerconfig "github.com/uber/jaeger-client-go/config"
	"golang.org/x/crypto/ed25519"
	"gopkg.in/yaml.v3"
)

// keyIDRegexp defines allowable characters in K IDs.
var keyIDRegexp = regexp.MustCompile("^ed25519:[a-zA-Z0-9_]+$")

// Version is the current version of the config format.
// This will change whenever we make breaking changes to the config format.
const Version = 2

// Matrix contains all the config used by a matrix process.
// Relative paths are resolved relative to the current working directory
type Matrix struct {
	// The version of the configuration file.
	// If the version in a file doesn't match the current dendrite config
	// version then we can give a clear error message telling the user
	// to update their config file to the current version.
	// The version of the file should only be different if there has
	// been a breaking change to the config file format.
	Version int `yaml:"version"`

	Global        Global        `yaml:"global"`
	AppServiceAPI AppServiceAPI `yaml:"app_service_api"`
	ClientAPI     ClientAPI     `yaml:"client_api"`
	FederationAPI FederationAPI `yaml:"federation_api"`
	KeyServer     KeyServer     `yaml:"key_server"`
	MediaAPI      MediaAPI      `yaml:"media_api"`
	RoomServer    RoomServer    `yaml:"room_server"`
	SyncAPI       SyncAPI       `yaml:"sync_api"`
	UserAPI       UserAPI       `yaml:"user_api"`
	RelayAPI      RelayAPI      `yaml:"relay_api"`

	MSCs MSCs `yaml:"mscs"`

	// The config for tracing the dendrite servers.
	Tracing struct {
		// Set to true to enable tracer hooks. If false, no tracing is set up.
		Enabled bool `yaml:"enabled"`
		// The config for the jaeger opentracing reporter.
		Jaeger jaegerconfig.Configuration `yaml:"jaeger"`
	} `yaml:"tracing"`

	// Any information derived from the configuration options for later use.
	Derived Derived `yaml:"-"`
}

// TODO: Kill Derived
type Derived struct {
	Registration struct {
		// Flows is a slice of flows, which represent one possible way that the client can authenticate a request.
		// http://matrix.org/docs/spec/HEAD/client_server/r0.3.0.html#user-interactive-authentication-api
		// As long as the generated flows only rely on config file options,
		// we can generate them on startup and store them until needed
		Flows []authtypes.Flow `json:"flows"`

		// Params that need to be returned to the client during
		// registration in order to complete registration stages.
		Params map[string]interface{} `json:"params"`
	}

	// Application services parsed from their config files
	// The paths of which were given above in the main config file
	ApplicationServices []ApplicationService

	// Meta-regexes compiled from all exclusive application service
	// Regexes.
	//
	// When a user registers, we check that their username does not match any
	// exclusive application service namespaces
	ExclusiveApplicationServicesUsernameRegexp *regexp.Regexp
	// When a user creates a room alias, we check that it isn't already
	// reserved by an application service
	ExclusiveApplicationServicesAliasRegexp *regexp.Regexp
	// Note: An Exclusive Regex for room ID isn't necessary as we aren't blocking
	// servers from creating RoomIDs in exclusive application service namespaces
}

// A Path on the filesystem.
type Path string

// A DataSource for opening a postgresql database using lib/pq.
type DataSource string

func (d DataSource) ToArray() []DataSource {

	var connectionDSList []DataSource
	connectionUris := strings.Split(string(d), ",")
	for _, connectionUri := range connectionUris {
		dataSourceUri := DataSource(connectionUri)
		connectionDSList = append(connectionDSList, dataSourceUri)
	}

	return connectionDSList
}

func (d DataSource) IsSQLite() bool {
	return strings.HasPrefix(string(d), "file:")
}

func (d DataSource) IsPostgres() bool {
	// Check if the string starts with "postgres://"
	if strings.HasPrefix(string(d), "postgres://") {
		// Try parsing the URI format
		u, err := url.Parse(string(d))
		if err != nil || u.Scheme != "postgres" {
			return false
		}
		// Validate required parts for PostgreSQL URI
		if u.Host == "" || u.Path == "" {
			return false
		}
		return true
	}

	// Check for key-value format using regex
	keyValueRegex := regexp.MustCompile(`(?i)^(user=\S+|password=\S+|host=\S+|port=\d+|dbname=\S+|sslmode=\S+)(\s+\S+=\S+)*$`)
	return keyValueRegex.MatchString(string(d))
}

func (d DataSource) IsDB() bool {
	return d.IsPostgres() || d.IsSQLite()
}

func (d DataSource) IsRedis() bool {
	return strings.HasPrefix(string(d), "redis://") || strings.HasPrefix(string(d), "rediss://")
}
func (d DataSource) IsCache() bool {
	return d.IsRedis()
}

func (d DataSource) IsNats() bool {
	return strings.HasPrefix(string(d), "nats://")
}

func (d DataSource) IsMem() bool {
	return strings.HasPrefix(string(d), "mem://")
}

func (d DataSource) IsQueue() bool {
	return d.IsMem() || d.IsNats()
}

func (d DataSource) ToURI() (*url.URL, error) {
	return url.Parse(string(d))
}

func (d DataSource) ExtendPath(epath ...string) DataSource {
	nuUri, err := d.ToURI()
	if err != nil {
		return d
	}

	nuPathPieces := []string{nuUri.Path}
	nuPathPieces = append(nuPathPieces, epath...)

	nuUri.Path = path.Join(nuPathPieces...)

	return DataSource(nuUri.String())
}

func (d DataSource) SuffixPath(suffix string) DataSource {
	nuUri, err := d.ToURI()
	if err != nil {
		return d
	}

	if nuUri.Path != "" {

		nuUri.Path = strings.Join([]string{nuUri.Path, suffix}, "")
	}
	return DataSource(nuUri.String())
}

func (d DataSource) PrefixPath(prefix string) DataSource {
	nuUri, err := d.ToURI()
	if err != nil {
		return d
	}

	if nuUri.Path != "" {
		nuUri.Path = strings.Join([]string{prefix, nuUri.Path}, "")
	}

	return DataSource(nuUri.String())
}

func (d DataSource) DelPath() DataSource {
	nuUri, err := d.ToURI()
	if err != nil {
		return d
	}

	nuUri.Path = ""

	return DataSource(nuUri.String())
}

func (d DataSource) ExtendQuery(key, value string) DataSource {
	nuUri, err := d.ToURI()
	if err != nil {
		return d
	}

	q := nuUri.Query()
	q.Set(key, value)

	nuUri.RawQuery = q.Encode()

	return DataSource(nuUri.String())
}

func (d DataSource) RemoveQuery(key ...string) DataSource {
	nuUri, err := d.ToURI()
	if err != nil {
		return d
	}

	q := nuUri.Query()

	for _, k := range key {
		q.Del(k)
	}

	nuUri.RawQuery = q.Encode()

	return DataSource(nuUri.String())
}

func (d DataSource) GetQuery(key string) string {
	nuUri, err := d.ToURI()
	if err != nil {
		return ""
	}

	return nuUri.Query().Get(key)
}

func (d DataSource) String() string {
	return string(d)
}

// A Topic in kafka.
type Topic string

// FileSizeBytes is a file size in bytes
type FileSizeBytes int64

// ThumbnailSize contains a single thumbnail size configuration
type ThumbnailSize struct {
	// Maximum width of the thumbnail image
	Width int `yaml:"width"`
	// Maximum height of the thumbnail image
	Height int `yaml:"height"`
	// ResizeMethod is one of crop or scale.
	// crop scales to fill the requested dimensions and crops the excess.
	// scale scales to fit the requested dimensions and one dimension may be smaller than requested.
	ResizeMethod string `yaml:"method,omitempty"`
}

// Errors stores problems encountered when parsing a config file.
// It implements the error interface.
type Errors []string

// Load a yaml config file for a server run as multiple processes or as a monolith.
// Checks the config to ensure that it is valid.
func Load(configPath string) (*Matrix, error) {
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	basePath, err := filepath.Abs(".")
	if err != nil {
		return nil, err
	}
	// Pass the current working directory and os.ReadFile so that they can
	// be mocked in the tests
	return loadConfig(basePath, configData, os.ReadFile)
}

func loadConfig(
	basePath string,
	configData []byte,
	readFile func(string) ([]byte, error),
) (*Matrix, error) {
	var c Matrix
	c.Defaults(DefaultOpts{
		DSDatabaseConn:   DataSource(os.Getenv("DATABASE_URL")),
		DSCacheConn:      DataSource(os.Getenv("CACHE_URI")),
		DSQueueConn:      DataSource(os.Getenv("QUEUE_URI")),
		RandomnessPrefix: os.Getenv("RANDOMNESS_PREFIX"),
	})

	err := yaml.Unmarshal(configData, &c)
	if err != nil {
		return nil, err
	}

	err = c.LoadEnv()
	if err != nil {
		return nil, err
	}

	// Generate data from config options
	err = c.Derive()
	if err != nil {
		return nil, err
	}

	c.Wiring()

	if err = c.check(); err != nil {
		return nil, err
	}

	privateKeyPath := absPath(basePath, c.Global.PrivateKeyPath)
	if c.Global.KeyID, c.Global.PrivateKey, err = LoadMatrixKey(privateKeyPath, readFile); err != nil {
		return nil, fmt.Errorf("failed to load private_key: %w", err)
	}

	for _, v := range c.Global.VirtualHosts {
		if v.KeyValidityPeriod == 0 {
			v.KeyValidityPeriod = c.Global.KeyValidityPeriod
		}
		if v.PrivateKeyPath == "" || v.PrivateKey == nil || v.KeyID == "" {
			v.KeyID = c.Global.KeyID
			v.PrivateKey = c.Global.PrivateKey
			continue
		}
		privateKeyPath = absPath(basePath, v.PrivateKeyPath)
		if v.KeyID, v.PrivateKey, err = LoadMatrixKey(privateKeyPath, readFile); err != nil {
			return nil, fmt.Errorf("failed to load private_key for virtualhost %s: %w", v.ServerName, err)
		}
	}

	for _, key := range c.Global.OldVerifyKeys {
		switch {
		case key.PrivateKeyPath != "":
			var oldPrivateKeyData []byte
			oldPrivateKeyPath := absPath(basePath, key.PrivateKeyPath)
			oldPrivateKeyData, err = readFile(oldPrivateKeyPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read %q: %w", oldPrivateKeyPath, err)
			}

			// NOTSPEC: Ordinarily we should enforce key ID formatting, but since there are
			// a number of private keys out there with non-compatible symbols in them due
			// to lack of validation in Synapse, we won't enforce that for old verify keys.
			keyID, privateKey, perr := readKeyPEM(oldPrivateKeyPath, oldPrivateKeyData, false)
			if perr != nil {
				return nil, fmt.Errorf("failed to parse %q: %w", oldPrivateKeyPath, perr)
			}

			key.KeyID = keyID
			key.PrivateKey = privateKey
			key.PublicKey = spec.Base64Bytes(privateKey.Public().(ed25519.PublicKey))

		case key.KeyID == "":
			return nil, fmt.Errorf("'key_id' must be specified if 'public_key' is specified")

		case len(key.PublicKey) == ed25519.PublicKeySize:
			continue

		case len(key.PublicKey) > 0:
			return nil, fmt.Errorf("the supplied 'public_key' is the wrong length")

		default:
			return nil, fmt.Errorf("either specify a 'private_key' path or supply both 'public_key' and 'key_id'")
		}
	}

	c.MediaAPI.AbsBasePath = Path(absPath(basePath, c.MediaAPI.BasePath))

	return &c, nil
}

func LoadMatrixKey(privateKeyPath string, readFile func(string) ([]byte, error)) (gomatrixserverlib.KeyID, ed25519.PrivateKey, error) {
	privateKeyData, err := readFile(privateKeyPath)
	if err != nil {
		return "", nil, err
	}
	return readKeyPEM(privateKeyPath, privateKeyData, true)
}

// Derive generates data that is derived from various values provided in
// the config file.
func (config *Matrix) Derive() error {
	// Determine registration flows based off config values

	config.Derived.Registration.Params = make(map[string]interface{})

	// TODO: Add email auth type
	// TODO: Add MSISDN auth type

	if config.ClientAPI.RecaptchaEnabled {
		config.Derived.Registration.Params[authtypes.LoginTypeRecaptcha] = map[string]string{"public_key": config.ClientAPI.RecaptchaPublicKey}
		config.Derived.Registration.Flows = []authtypes.Flow{
			{Stages: []authtypes.LoginType{authtypes.LoginTypeRecaptcha}},
		}
	} else {
		config.Derived.Registration.Flows = []authtypes.Flow{
			{Stages: []authtypes.LoginType{authtypes.LoginTypeDummy}},
		}
	}

	// Load application service configuration files
	if err := loadAppServices(&config.AppServiceAPI, &config.Derived); err != nil {
		return err
	}

	return nil
}

func (config *Matrix) LoadEnv() error {
	return config.Global.LoadEnv()
}

type DefaultOpts struct {
	DSDatabaseConn   DataSource
	DSCacheConn      DataSource
	DSQueueConn      DataSource
	RandomnessPrefix string
}

type KVOpt struct {
	K string
	V string
}

func (d *DefaultOpts) defaultQ(s string, opts ...KVOpt) QueueOptions {

	ref := fmt.Sprintf("%s_Ref", s)

	if d.DSQueueConn.IsNats() {

		durable := strings.ReplaceAll(fmt.Sprintf("CnsDurable_%s", s), ".", "_")

		ds := d.DSQueueConn.
			ExtendQuery("jetstream", "true").
			ExtendQuery("subject", s).
			ExtendQuery("stream_name", s).
			ExtendQuery("stream_subjects", s).
			ExtendQuery("stream_storage", "file").
			ExtendQuery("stream_retention", "workqueue").
			ExtendQuery("consumer_filter_subject", s).
			ExtendQuery("consumer_durable_name", durable).
			ExtendQuery("consumer_deliver_policy", "all").
			ExtendQuery("consumer_ack_policy", "explicit").
			ExtendQuery("consumer_replay_policy", "instant")

		for _, kv := range opts {
			ds = ds.ExtendQuery(kv.K, kv.V)
		}

		return QueueOptions{Prefix: d.RandomnessPrefix, QReference: ref, DS: ds}
	}
	return QueueOptions{Prefix: d.RandomnessPrefix, QReference: ref, DS: d.DSQueueConn.SuffixPath(s)}
}

// Defaults sets default config values if they are not explicitly set.
func (config *Matrix) Defaults(opts DefaultOpts) {
	config.Version = Version

	config.Global.Defaults(opts)
	config.ClientAPI.Defaults(opts)
	config.FederationAPI.Defaults(opts)
	config.KeyServer.Defaults(opts)
	config.MediaAPI.Defaults(opts)
	config.RoomServer.Defaults(opts)
	config.SyncAPI.Defaults(opts)
	config.UserAPI.Defaults(opts)
	config.AppServiceAPI.Defaults(opts)
	config.RelayAPI.Defaults(opts)
	config.MSCs.Defaults(opts)
	config.Wiring()
}

func (config *Matrix) Verify(configErrs *Errors) {
	type verifiable interface {
		Verify(configErrs *Errors)
	}
	for _, c := range []verifiable{
		&config.Global, &config.ClientAPI, &config.MediaAPI, &config.RoomServer,
		&config.SyncAPI, &config.UserAPI,
	} {
		c.Verify(configErrs)
	}
}

func (config *Matrix) Wiring() {
	config.ClientAPI.Global = &config.Global
	config.FederationAPI.Global = &config.Global

	config.KeyServer.Global = &config.Global

	config.MediaAPI.Global = &config.Global

	config.RoomServer.Global = &config.Global

	config.SyncAPI.Global = &config.Global

	config.UserAPI.Global = &config.Global
	config.AppServiceAPI.Global = &config.Global
	config.RelayAPI.Global = &config.Global

	config.MSCs.Matrix = &config.Global

	config.ClientAPI.Derived = &config.Derived
	config.AppServiceAPI.Derived = &config.Derived
	config.ClientAPI.MSCs = &config.MSCs
}

// Error returns a string detailing how many errors were contained within a
// configErrors type.
func (errs Errors) Error() string {
	if len(errs) == 1 {
		return errs[0]
	}
	return fmt.Sprintf(
		"%s (and %d other problems)", errs[0], len(errs)-1,
	)
}

// Add appends an error to the list of errors in this configErrors.
// It is a wrapper to the builtin append and hides pointers from
// the client code.
// This method is safe to use with an uninitialise configErrors because
// if it is nil, it will be properly allocated.
func (errs *Errors) Add(str string) {
	*errs = append(*errs, str)
}

// checkNotEmpty verifies the given value is not empty in the configuration.
// If it is, adds an error to the list.
func checkNotEmpty(configErrs *Errors, key, value string) {
	if value == "" {
		configErrs.Add(fmt.Sprintf("missing config key %q", key))
	}
}

// checkPositive verifies the given value is positive (zero included)
// in the configuration. If it is not, adds an error to the list.
func checkPositive(configErrs *Errors, key string, value int64) {
	if value < 0 {
		configErrs.Add(fmt.Sprintf("invalid value for config key %q: %d", key, value))
	}
}

// check returns an error type containing all errors found within the config
// file.
func (config *Matrix) check() error { // monolithic
	var configErrs Errors

	if config.Version != Version {
		configErrs.Add(fmt.Sprintf(
			"config version is %q, expected %q - this means that the format of the configuration "+
				"file has changed in some significant way, so please revisit the sample config "+
				"and ensure you are not missing any important options that may have been added "+
				"or changed recently!",
			config.Version, Version,
		))
		return &configErrs
	}

	// Due to how Golang manages its interface types, this condition is not redundant.
	// In order to get the proper behaviour, it is necessary to return an explicit nil
	// and not a nil configErrors.
	// This is because the following equalities hold:
	// error(nil) == nil
	// error(configErrors(nil)) != nil
	if len(configErrs) > 0 {
		return &configErrs
	}
	return nil
}

// absPath returns the absolute path for a given relative or absolute path.
func absPath(dir string, path Path) string {
	if filepath.IsAbs(string(path)) {
		// filepath.Join cleans the path so we should clean the absolute paths as well for consistency.
		return filepath.Clean(string(path))
	}
	return filepath.Join(dir, string(path))
}

func readKeyPEM(path string, data []byte, enforceKeyIDFormat bool) (gomatrixserverlib.KeyID, ed25519.PrivateKey, error) {
	for {
		var keyBlock *pem.Block
		keyBlock, data = pem.Decode(data)
		if data == nil {
			return "", nil, fmt.Errorf("no matrix private key PEM data in %q", path)
		}
		if keyBlock == nil {
			return "", nil, fmt.Errorf("keyBlock is nil %q", path)
		}
		if keyBlock.Type == "MATRIX PRIVATE KEY" {
			keyID := keyBlock.Headers["Key-ID"]
			if keyID == "" {
				return "", nil, fmt.Errorf("missing key ID in PEM data in %q", path)
			}
			if !strings.HasPrefix(keyID, "ed25519:") {
				return "", nil, fmt.Errorf("key ID %q doesn't start with \"ed25519:\" in %q", keyID, path)
			}
			if enforceKeyIDFormat && !keyIDRegexp.MatchString(keyID) {
				return "", nil, fmt.Errorf("key ID %q in %q contains illegal characters (use a-z, A-Z, 0-9 and _ only)", keyID, path)
			}
			_, privKey, err := ed25519.GenerateKey(bytes.NewReader(keyBlock.Bytes))
			if err != nil {
				return "", nil, err
			}
			return gomatrixserverlib.KeyID(keyID), privKey, nil
		}
	}
}
