package config

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/pitabwire/frame"
	"golang.org/x/crypto/ed25519"
)

type Global struct {
	frame.ConfigurationDefault `yaml:"embedded_config"`
	// Signing identity contains the server name, private key and key ID of
	// the deployment.
	fclient.SigningIdentity `env:"-" yaml:",inline"`

	// The secondary server names, used for virtual hosting.
	VirtualHosts []*VirtualHost `env:"-" yaml:"-"`

	// Path to the private key which will be used to sign requests and events.
	PrivateKeyPath Path `env:"-" yaml:"private_key"`

	// Information about old private keys that used to be used to sign requests and
	// events on this domain. They will not be used but will be advertised to other
	// servers that ask for them to help verify old events.
	OldVerifyKeys []*OldVerifyKeys `env:"-" yaml:"old_private_keys"`

	// How long a remote server can cache our server key for before requesting it again.
	// Increasing this number will reduce the number of requests made by remote servers
	// for our key, but increases the period a compromised key will be considered valid
	// by remote servers.
	// Defaults to 24 hours.
	KeyValidityPeriod time.Duration `env:"-" yaml:"key_validity_period"`

	// Global pool of database connections, which is used only in monolith mode. If a
	// component does not specify any database options of its own, then this pool of
	// connections will be used instead. This way we don't have to manage connection
	// counts on a per-component basis, but can instead do it for the entire monolith.
	DatabaseOptions DatabaseOptions `yaml:"database,omitempty"`

	// The server name to delegate server-server communications to, with optional port
	WellKnownServerName string `yaml:"well_known_server_name"`

	// The server name to delegate client-server communications to, with optional port
	WellKnownClientName string `yaml:"well_known_client_name"`

	// The server name to delegate sliding sync communications to, with optional port.
	// Requires `well_known_client_name` to also be configured.
	WellKnownSlidingSyncProxy string `yaml:"well_known_sliding_sync_proxy"`

	// Disables federation. Dendrite will not be able to make any outbound HTTP requests
	// to other servers and the federation API will not be exposed.
	DisableFederation bool `yaml:"disable_federation"`

	// Configures the handling of presence events.
	Presence PresenceOptions `yaml:"presence"`

	// List of domains that the server will trust as identity servers to
	// verify third-party identifiers.
	// Defaults to an empty array.
	TrustedIDServers []string `yaml:"trusted_third_party_id_servers"`

	// JetStream configuration
	JetStream JetStream `yaml:"jetstream"`

	// Metrics configuration
	Metrics Metrics `yaml:"metrics"`

	// Sentry configuration
	Sentry Sentry `yaml:"sentry"`

	// DNS caching options for all outbound HTTP requests
	DNSCache DNSCacheOptions `yaml:"dns_cache"`

	// ServerNotices configuration used for sending server notices
	ServerNotices ServerNotices `yaml:"server_notices"`

	// ReportStats configures opt-in phone-home statistics reporting.
	ReportStats ReportStats `yaml:"report_stats"`

	// Configuration for the caches.
	Cache CacheOptions `yaml:"cache"`

	DistributedAPI DistributedAPI `yaml:"distributed_api"`
}

func (c *Global) Defaults(opts DefaultOpts) {

	c.ServerName = "localhost"

	c.PrivateKeyPath = "matrix_key.pem"
	_, c.PrivateKey, _ = ed25519.GenerateKey(rand.New(rand.NewSource(0)))
	c.KeyID = "ed25519:auto"
	c.TrustedIDServers = []string{
		"matrix.org",
		"vector.im",
	}

	c.KeyValidityPeriod = time.Hour * 24 * 7
	c.DatabaseOptions.Defaults(opts)
	c.JetStream.Defaults(opts)
	c.Metrics.Defaults(opts)
	c.DNSCache.Defaults()
	c.Sentry.Defaults()
	c.ServerNotices.Defaults(opts)
	c.ReportStats.Defaults()
	c.Cache.Defaults(opts)
	c.DistributedAPI.Defaults()
}

func (c *Global) LoadEnv() error {
	err := frame.ConfigFillFromEnv(c)
	if err != nil {
		return err
	}
	err = c.DatabaseOptions.LoadEnv()
	if err != nil {
		return err
	}
	err = c.JetStream.LoadEnv()
	if err != nil {
		return err
	}
	err = c.Cache.LoadEnv()
	if err != nil {
		return err
	}
	return nil
}

func (c *Global) Verify(configErrs *ConfigErrors) {
	checkNotEmpty(configErrs, "global.server_name", string(c.ServerName))
	checkNotEmpty(configErrs, "global.private_key", string(c.PrivateKeyPath))

	// Check that client well-known has a proper format
	if c.WellKnownClientName != "" && !strings.HasPrefix(c.WellKnownClientName, "http://") && !strings.HasPrefix(c.WellKnownClientName, "https://") {
		configErrs.Add("The configuration for well_known_client_name does not have a proper format, consider adding http:// or https://. Some clients may fail to connect.")
	}

	for _, v := range c.VirtualHosts {
		v.Verify(configErrs)
	}

	c.DatabaseOptions.Verify(configErrs)
	c.JetStream.Verify(configErrs)
	c.Metrics.Verify(configErrs)
	c.Sentry.Verify(configErrs)
	c.DNSCache.Verify(configErrs)
	c.ServerNotices.Verify(configErrs)
	c.ReportStats.Verify(configErrs)
	c.Cache.Verify(configErrs)
}

func (c *Global) IsLocalServerName(serverName spec.ServerName) bool {
	if c.ServerName == serverName {
		return true
	}
	for _, v := range c.VirtualHosts {
		if v.ServerName == serverName {
			return true
		}
	}
	return false
}

func (c *Global) SplitLocalID(sigil byte, id string) (string, spec.ServerName, error) {
	u, s, err := gomatrixserverlib.SplitID(sigil, id)
	if err != nil {
		return u, s, err
	}
	if !c.IsLocalServerName(s) {
		return u, s, fmt.Errorf("server name %q not known", s)
	}
	return u, s, nil
}

func (c *Global) VirtualHost(serverName spec.ServerName) *VirtualHost {
	for _, v := range c.VirtualHosts {
		if v.ServerName == serverName {
			return v
		}
	}
	return nil
}

func (c *Global) VirtualHostForHTTPHost(serverName spec.ServerName) *VirtualHost {
	for _, v := range c.VirtualHosts {
		if v.ServerName == serverName {
			return v
		}
		for _, h := range v.MatchHTTPHosts {
			if h == serverName {
				return v
			}
		}
	}
	return nil
}

func (c *Global) SigningIdentityFor(serverName spec.ServerName) (*fclient.SigningIdentity, error) {
	for _, id := range c.SigningIdentities() {
		if id.ServerName == serverName {
			return id, nil
		}
	}
	return nil, fmt.Errorf("no signing identity for %q", serverName)
}

func (c *Global) SigningIdentities() []*fclient.SigningIdentity {
	identities := make([]*fclient.SigningIdentity, 0, len(c.VirtualHosts)+1)
	identities = append(identities, &c.SigningIdentity)
	for _, v := range c.VirtualHosts {
		identities = append(identities, &v.SigningIdentity)
	}
	return identities
}

type VirtualHost struct {
	// Signing identity contains the server name, private key and key ID of
	// the virtual host.
	fclient.SigningIdentity `yaml:",inline"`

	// Path to the private key. If not specified, the default global private key
	// will be used instead.
	PrivateKeyPath Path `yaml:"private_key"`

	// How long a remote server can cache our server key for before requesting it again.
	// Increasing this number will reduce the number of requests made by remote servers
	// for our key, but increases the period a compromised key will be considered valid
	// by remote servers.
	// Defaults to 24 hours.
	KeyValidityPeriod time.Duration `yaml:"key_validity_period"`

	// Match these HTTP Host headers on the `/key/v2/server` endpoint, this needs
	// to match all delegated names, likely including the port number too if
	// the well-known delegation includes that also.
	MatchHTTPHosts []spec.ServerName `yaml:"match_http_hosts"`

	// Is registration enabled on this virtual host?
	AllowRegistration bool `yaml:"allow_registration"`

	// Is guest registration enabled on this virtual host?
	AllowGuests bool `yaml:"allow_guests"`
}

func (v *VirtualHost) Verify(configErrs *ConfigErrors) {
	checkNotEmpty(configErrs, "virtual_host.*.server_name", string(v.ServerName))
}

// RegistrationAllowed returns two bools, the first states whether registration
// is allowed for this virtual host and the second states whether guests are
// allowed for this virtual host.
func (v *VirtualHost) RegistrationAllowed() (bool, bool) {
	if v == nil {
		return false, false
	}
	return v.AllowRegistration, v.AllowGuests
}

type OldVerifyKeys struct {
	// Path to the private key.
	PrivateKeyPath Path `yaml:"private_key"`

	// The private key itself.
	PrivateKey ed25519.PrivateKey `yaml:"-"`

	// The public key, in case only that part is known.
	PublicKey spec.Base64Bytes `yaml:"public_key"`

	// The key ID of the private key.
	KeyID gomatrixserverlib.KeyID `yaml:"key_id"`

	// When the private key was designed as "expired", as a UNIX timestamp
	// in millisecond precision.
	ExpiredAt spec.Timestamp `yaml:"expired_at"`
}

// The configuration to use for Prometheus metrics
type Metrics struct {
	// Whether or not the metrics are enabled
	Enabled bool `yaml:"enabled"`
	// Use BasicAuth for Authorization
	BasicAuth struct {
		// Authorization via Static Username & Password
		// Hardcoded Username and Password
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"basic_auth"`
}

func (c *Metrics) Defaults(opts DefaultOpts) {
	c.Enabled = false
	c.BasicAuth.Username = "metrics"
	c.BasicAuth.Password = "metrics"

}

func (c *Metrics) Verify(configErrs *ConfigErrors) {
}

// ServerNotices defines the configuration used for sending server notices
type ServerNotices struct {
	Enabled bool `yaml:"enabled"`
	// The localpart to be used when sending notices
	LocalPart string `yaml:"local_part"`
	// The displayname to be used when sending notices
	DisplayName string `yaml:"display_name"`
	// The avatar of this user
	AvatarURL string `yaml:"avatar_url"`
	// The roomname to be used when creating messages
	RoomName string `yaml:"room_name"`
}

func (c *ServerNotices) Defaults(opts DefaultOpts) {
	c.Enabled = true
	c.LocalPart = "_server"
	c.DisplayName = "Server Alert"
	c.RoomName = "Server Alert"
	c.AvatarURL = ""
}

func (c *ServerNotices) Verify(errors *ConfigErrors) {}

type CacheOptions struct {
	// The connection string,
	ConnectionString DataSource `env:"CACHE_URI" yaml:"connection_string"`

	EstimatedMaxSize DataUnit      `yaml:"max_size_estimated"`
	MaxAge           time.Duration `yaml:"max_age"`
	EnablePrometheus bool          `yaml:"enable_prometheus"`
}

func (c *CacheOptions) LoadEnv() error {

	err := frame.ConfigFillFromEnv(c)
	if err != nil {
		c.ConnectionString = DataSource(os.Getenv("CACHE_URI"))
	}

	if !c.ConnectionString.IsRedis() {
		log.WithField("cache_uri", c.ConnectionString).Warn("Invalid cache uri in the config")
	}
	return nil
}

func (c *CacheOptions) Defaults(opts DefaultOpts) {
	connectionUriStr := string(opts.CacheConnectionStr)
	if connectionUriStr != "" {
		c.ConnectionString = DataSource(connectionUriStr)
	}

	c.ConnectionString = opts.CacheConnectionStr
	c.EstimatedMaxSize = 1024 * 1024 * 1024 // 1GB
	c.MaxAge = time.Hour
}

func (c *CacheOptions) Verify(configErrs *ConfigErrors) {
	checkPositive(configErrs, "max_size_estimated", int64(c.EstimatedMaxSize))

	checkNotEmpty(configErrs, "global.cache.connection_string", string(c.ConnectionString))
}

// ReportStats configures opt-in phone-home statistics reporting.
type ReportStats struct {
	// Enabled configures phone-home statistics of the server
	Enabled bool `yaml:"enabled"`

	// Endpoint the endpoint to report stats to
	Endpoint string `yaml:"endpoint"`
}

func (c *ReportStats) Defaults() {
	c.Enabled = false
	c.Endpoint = "https://panopticon.matrix.org/push"
}

func (c *ReportStats) Verify(configErrs *ConfigErrors) {
	// We prefer to hit panopticon (https://github.com/matrix-org/panopticon) directly over
	// the "old" matrix.org endpoint.
	if c.Endpoint == "https://matrix.org/report-usage-stats/push" {
		c.Endpoint = "https://panopticon.matrix.org/push"
	}
	if c.Enabled {
		checkNotEmpty(configErrs, "global.report_stats.endpoint", c.Endpoint)
	}
}

// The configuration to use for Sentry error reporting
type Sentry struct {
	Enabled bool `yaml:"enabled"`
	// The DSN to connect to e.g "https://examplePublicKey@o0.ingest.sentry.io/0"
	// See https://docs.sentry.io/platforms/go/configuration/options/
	DSN string `yaml:"dsn"`
	// The environment e.g "production"
	// See https://docs.sentry.io/platforms/go/configuration/environments/
	Environment string `yaml:"environment"`
}

func (c *Sentry) Defaults() {
	c.Enabled = false
}

func (c *Sentry) Verify(configErrs *ConfigErrors) {
}

type DatabaseOptions struct {
	// The connection string, file:filename.db or postgres://server....
	ConnectionString DataSource `env:"DATABASE_URI"  yaml:"connection_string"`
	// Maximum open connections to the DB (0 = use default, negative means unlimited)
	MaxOpenConnections int `yaml:"max_open_conns"`
	// Maximum idle connections to the DB (0 = use default, negative means unlimited)
	MaxIdleConnections int `yaml:"max_idle_conns"`
	// maximum amount of time (in seconds) a connection may be reused (<= 0 means unlimited)
	ConnMaxLifetimeSeconds int `yaml:"conn_max_lifetime"`
}

func (c *DatabaseOptions) LoadEnv() error {

	err := frame.ConfigFillFromEnv(c)
	if err != nil {
		c.ConnectionString = DataSource(os.Getenv("DATABASE_URI"))
	}

	if !c.ConnectionString.IsPostgres() {
		log.WithField("db_uri", c.ConnectionString).Warn("Invalid database uri in the config")
	}
	return nil
}

func (c *DatabaseOptions) Defaults(opts DefaultOpts) {

	databaseUriStr := string(opts.DatabaseConnectionStr)
	if databaseUriStr != "" {
		c.ConnectionString = DataSource(databaseUriStr)
	}

	c.MaxOpenConnections = 90
	c.MaxIdleConnections = 2
	c.ConnMaxLifetimeSeconds = -1
}

func (c *DatabaseOptions) Verify(configErrs *ConfigErrors) {

	if c.ConnectionString != "" {
		return
	}

	checkNotEmpty(configErrs, "global.database.connection_string", string(c.ConnectionString))

}

// MaxIdleConns returns maximum idle connections to the DB
func (c *DatabaseOptions) MaxIdleConns() int {
	return c.MaxIdleConnections
}

// MaxOpenConns returns maximum open connections to the DB
func (c *DatabaseOptions) MaxOpenConns() int {
	return c.MaxOpenConnections
}

// ConnMaxLifetime returns maximum amount of time a connection may be reused
func (c *DatabaseOptions) ConnMaxLifetime() time.Duration {
	return time.Duration(c.ConnMaxLifetimeSeconds) * time.Second
}

type DNSCacheOptions struct {
	// Whether the DNS cache is enabled or not
	Enabled bool `yaml:"enabled"`
	// How many entries to store in the DNS cache at a given time
	CacheSize int `yaml:"cache_size"`
	// How long a cache entry should be considered valid for
	CacheLifetime time.Duration `yaml:"cache_lifetime"`
}

func (c *DNSCacheOptions) Defaults() {
	c.Enabled = false
	c.CacheSize = 256
	c.CacheLifetime = time.Minute * 5
}

func (c *DNSCacheOptions) Verify(configErrs *ConfigErrors) {
	checkPositive(configErrs, "cache_size", int64(c.CacheSize))
	checkPositive(configErrs, "cache_lifetime", int64(c.CacheLifetime))
}

// PresenceOptions defines possible configurations for presence events.
type PresenceOptions struct {
	// Whether inbound presence events are allowed
	EnableInbound bool `yaml:"enable_inbound"`
	// Whether outbound presence events are allowed
	EnableOutbound bool `yaml:"enable_outbound"`
}

type DataUnit int64

func (d *DataUnit) UnmarshalText(text []byte) error {
	var magnitude float64
	s := strings.ToLower(string(text))
	switch {
	case strings.HasSuffix(s, "tb"):
		s, magnitude = s[:len(s)-2], 1024*1024*1024*1024
	case strings.HasSuffix(s, "gb"):
		s, magnitude = s[:len(s)-2], 1024*1024*1024
	case strings.HasSuffix(s, "mb"):
		s, magnitude = s[:len(s)-2], 1024*1024
	case strings.HasSuffix(s, "kb"):
		s, magnitude = s[:len(s)-2], 1024
	default:
		magnitude = 1
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*d = DataUnit(v * magnitude)
	return nil
}

// DistributedAPI The configuration to enable the use of distributed stores of data
type DistributedAPI struct {
	Enabled             bool   `yaml:"enabled"`
	ProfileServiceUri   string `yaml:"profile_service_uri"`
	PartitionServiceUri string `yaml:"partition_service_uri"`
}

func (d *DistributedAPI) Defaults() {
	d.Enabled = false
}

func (d *DistributedAPI) Verify(configErrs *ConfigErrors) {
}
