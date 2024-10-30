package config

import (
	"golang.org/x/crypto/bcrypt"
)

type UserAPI struct {
	Matrix *Global `yaml:"-"`

	// The cost when hashing passwords.
	BCryptCost int `yaml:"bcrypt_cost"`

	// The length of time an OpenID token is condidered valid in milliseconds
	OpenIDTokenLifetimeMS int64 `yaml:"openid_token_lifetime_ms"`

	// Disable TLS validation on HTTPS calls to push gatways. NOT RECOMMENDED!
	PushGatewayDisableTLSValidation bool `yaml:"push_gateway_disable_tls_validation"`

	// The Account database stores the login details and account information
	// for local users. It is accessed by the UserAPI.
	AccountDatabase DatabaseOptions `yaml:"account_database,omitempty"`

	// Users who register on this homeserver will automatically
	// be joined to the rooms listed under this option.
	AutoJoinRooms []string `yaml:"auto_join_rooms"`

	// The number of workers to start for the DeviceListUpdater. Defaults to 8.
	// This only needs updating if the "InputDeviceListUpdate" stream keeps growing indefinitely.
	WorkerCount int `yaml:"worker_count"`

	JWTLogin JWTLogin `yaml:"jwt_login"`
}

const DefaultOpenIDTokenLifetimeMS = 3600000 // 60 minutes

func (c *UserAPI) Defaults(opts DefaultOpts) {
	c.BCryptCost = bcrypt.DefaultCost
	c.OpenIDTokenLifetimeMS = DefaultOpenIDTokenLifetimeMS
	c.WorkerCount = 8
	if opts.Generate {
		if !opts.SingleDatabase {
			c.AccountDatabase.ConnectionString = "file:userapi_accounts.db"
		}
	}
}

func (c *UserAPI) Verify(configErrs *ConfigErrors) {
	c.JWTLogin.Verify(configErrs)
	checkPositive(configErrs, "user_api.openid_token_lifetime_ms", c.OpenIDTokenLifetimeMS)
	if c.Matrix.DatabaseOptions.ConnectionString == "" {
		checkNotEmpty(configErrs, "user_api.account_database.connection_string", string(c.AccountDatabase.ConnectionString))
	}
}

type JWTLogin struct {
	Issuer                string `yaml:"issuer"`
	Audience              string `yaml:"audience"`
	Oauth2WellKnownJwkUri string `yaml:"oauth2_well_known_jwk_uri"`
}

func (c *JWTLogin) Verify(configErrs *ConfigErrors) {
	checkNotEmpty(configErrs, "client_api.jwt_login.issuer", c.Issuer)
	checkNotEmpty(configErrs, "client_api.jwt_login.audience", c.Audience)
}
