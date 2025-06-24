package config

import (
	"fmt"
	"time"

	"github.com/antinvestor/matrix/setup/constants"
)

type ClientAPI struct {
	Global  *Global  `yaml:"-"`
	Derived *Derived `yaml:"-"` // TODO: Nuke Derived from orbit

	// If set disables new users from registering (except via shared
	// secrets)
	RegistrationDisabled bool `yaml:"registration_disabled"`

	// If set, requires users to submit a token during registration.
	// Tokens can be managed using admin API.
	RegistrationRequiresToken bool `yaml:"registration_requires_token"`

	// Enable registration without captcha verification or shared secret.
	// This option is populated by the -really-enable-open-registration
	// command line parameter as it is not recommended.
	OpenRegistrationWithoutVerificationEnabled bool `yaml:"-"`

	// If set, allows registration by anyone who also has the shared
	// secret, even if registration is otherwise disabled.
	RegistrationSharedSecret string `yaml:"registration_shared_secret"`
	// If set, prevents guest accounts from being created. Only takes
	// effect if registration is enabled, otherwise guests registration
	// is forbidden either way.
	GuestsDisabled bool `yaml:"guests_disabled"`

	// Boolean stating whether catpcha registration is enabled
	// and required
	RecaptchaEnabled bool `yaml:"enable_registration_captcha"`
	// Recaptcha api.js Url, for compatible with hcaptcha.com, etc.
	RecaptchaApiJsUrl string `yaml:"recaptcha_api_js_url"`
	// Recaptcha div class for sitekey, for compatible with hcaptcha.com, etc.
	RecaptchaSitekeyClass string `yaml:"recaptcha_sitekey_class"`
	// Recaptcha form field, for compatible with hcaptcha.com, etc.
	RecaptchaFormField string `yaml:"recaptcha_form_field"`
	// This Home Server's ReCAPTCHA public key.
	RecaptchaPublicKey string `yaml:"recaptcha_public_key"`
	// This Home Server's ReCAPTCHA private key.
	RecaptchaPrivateKey string `yaml:"recaptcha_private_key"`
	// Secret used to bypass the captcha registration entirely
	RecaptchaBypassSecret string `yaml:"recaptcha_bypass_secret"`
	// HTTP API endpoint used to verify whether the captcha response
	// was successful
	RecaptchaSiteVerifyAPI string `yaml:"recaptcha_siteverify_api"`

	LoginSSO LoginSSO `yaml:"login_sso"`

	// TURN options
	TURN TURN `yaml:"turn"`

	// Rate-limiting options
	RateLimiting RateLimiting `yaml:"rate_limiting"`

	MSCs *MSCs `yaml:"-"`

	Queues ClientQueues `yaml:"queues"`
}

func (c *ClientAPI) Defaults(opts DefaultOpts) {
	c.RegistrationSharedSecret = ""
	c.RegistrationRequiresToken = false
	c.RecaptchaPublicKey = ""
	c.RecaptchaPrivateKey = ""
	c.RecaptchaEnabled = false
	c.RecaptchaBypassSecret = ""
	c.RecaptchaSiteVerifyAPI = ""
	c.RegistrationDisabled = true
	c.OpenRegistrationWithoutVerificationEnabled = false
	c.RateLimiting.Defaults()
	c.Queues.Defaults(opts)
}

func (c *ClientAPI) Verify(configErrs *Errors) {
	c.TURN.Verify(configErrs)
	c.RateLimiting.Verify(configErrs)
	if c.RecaptchaEnabled {
		if c.RecaptchaSiteVerifyAPI == "" {
			c.RecaptchaSiteVerifyAPI = "https://www.google.com/recaptcha/api/siteverify"
		}
		if c.RecaptchaApiJsUrl == "" {
			c.RecaptchaApiJsUrl = "https://www.google.com/recaptcha/api.js"
		}
		if c.RecaptchaFormField == "" {
			c.RecaptchaFormField = "g-recaptcha-response"
		}
		if c.RecaptchaSitekeyClass == "" {
			c.RecaptchaSitekeyClass = "g-recaptcha"
		}
		checkNotEmpty(configErrs, "client_api.recaptcha_public_key", c.RecaptchaPublicKey)
		checkNotEmpty(configErrs, "client_api.recaptcha_private_key", c.RecaptchaPrivateKey)
		checkNotEmpty(configErrs, "client_api.recaptcha_siteverify_api", c.RecaptchaSiteVerifyAPI)
		checkNotEmpty(configErrs, "client_api.recaptcha_sitekey_class", c.RecaptchaSitekeyClass)
	}
	// Ensure there is any spam counter measure when enabling registration
	if !c.RegistrationDisabled && !c.OpenRegistrationWithoutVerificationEnabled && !c.RecaptchaEnabled {
		configErrs.Add(
			"You have tried to enable open registration without any secondary verification methods " +
				"(such as reCAPTCHA). By enabling open registration, you are SIGNIFICANTLY " +
				"increasing the risk that your server will be used to send spam or abuse, and may result in " +
				"your server being banned from some rooms. If you are ABSOLUTELY CERTAIN you want to do this, " +
				"start Matrix with the -really-enable-open-registration command line flag. Otherwise, you " +
				"should set the registration_disabled option in your Matrix config.",
		)
	}
}

type LoginSSO struct {

	// CallbackURL is the absolute URL where a user agent can reach
	// the Matrix `/_matrix/v3/login/sso/callback` endpoint. This is
	// used to create LoginSSO redirect URLs passed to identity
	// providers. If this is empty, a default is inferred from request
	// headers. When Matrix is running behind a proxy, this may not
	// always be the right information.
	CallbackURL string `yaml:"callback_url"`

	// Providers list the identity providers this server is capable of confirming an
	// identity with.
	Providers []IdentityProvider `yaml:"providers"`

	// DefaultProviderID is the provider to use when the client doesn't indicate one.
	// This is legacy support. If empty, the first provider listed is used.
	DefaultProviderID string `yaml:"default_provider"`
}

func (sso *LoginSSO) Verify(configErrs *Errors) {
	var foundDefaultProvider bool
	seenPIDs := make(map[string]bool, len(sso.Providers))
	for _, p := range sso.Providers {
		p = p.WithDefaults()
		p.verifyNormalized(configErrs)
		if p.ID == sso.DefaultProviderID {
			foundDefaultProvider = true
		}
		if seenPIDs[p.ID] {
			configErrs.Add(fmt.Sprintf("duplicate identity provider for config key %q: %s", "client_api.sso.providers", p.ID))
		}
		seenPIDs[p.ID] = true
	}
	if sso.DefaultProviderID != "" && !foundDefaultProvider {
		configErrs.Add(fmt.Sprintf("identity provider ID not found for config key %q: %s", "client_api.sso.default_provider", sso.DefaultProviderID))
	}

	if len(sso.Providers) == 0 {
		configErrs.Add(fmt.Sprintf("empty list for config key %q", "client_api.sso.providers"))
	}

}

type IdentityProvider struct {

	// ID is the unique identifier of this IdP. If empty, the brand will be used.
	ID string `yaml:"id"`

	// Name is a human-friendly name of the provider. If empty, a default based on
	// the brand will be used.
	Name string `yaml:"name"`

	ClientID     string `yaml:"client_id"`
	ClientSecret string `yaml:"client_secret"`
	DiscoveryURL string `yaml:"discovery_url"`

	JWTLogin JWTLogin `yaml:"jwt_login"`
}

func (idp *IdentityProvider) WithDefaults() IdentityProvider {
	p := *idp

	return p
}

func (idp *IdentityProvider) Verify(configErrs *Errors) {
	p := idp.WithDefaults()
	p.verifyNormalized(configErrs)
}

func (idp *IdentityProvider) verifyNormalized(configErrs *Errors) {
	checkNotEmpty(configErrs, "client_api.sso.providers.id", idp.ID)
	checkNotEmpty(configErrs, "client_api.sso.providers.name", idp.Name)

	checkNotEmpty(configErrs, "client_api.sso.providers.client_id", idp.ClientID)
	checkNotEmpty(configErrs, "client_api.sso.providers.client_secret", idp.ClientSecret)
	checkNotEmpty(configErrs, "client_api.sso.providers.discovery_url", idp.DiscoveryURL)
}

type JWTLogin struct {
	Audience string `yaml:"audience"`
}

type TURN struct {
	// TODO Guest Support
	// Whether or not guests can request TURN credentials
	// AllowGuests bool `yaml:"turn_allow_guests"`
	// How long the authorization should last
	UserLifetime string `yaml:"turn_user_lifetime"`
	// The list of TURN URIs to pass to clients
	URIs []string `yaml:"turn_uris"`

	// Authorization via Shared Secret
	// The shared secret from coturn
	SharedSecret string `yaml:"turn_shared_secret"`

	// Authorization via Static Username & Password
	// Hardcoded Username and Password
	Username string `yaml:"turn_username"`
	Password string `yaml:"turn_password"`
}

func (c *TURN) Verify(configErrs *Errors) {
	value := c.UserLifetime
	if value != "" {
		if _, err := time.ParseDuration(value); err != nil {
			configErrs.Add(fmt.Sprintf("invalid duration for config key %q: %s", "client_api.turn.turn_user_lifetime", value))
		}
	}
}

type RateLimiting struct {
	// Is rate limiting enabled or disabled?
	Enabled bool `yaml:"enabled"`

	// How many "slots" a user can occupy sending requests to a rate-limited
	// endpoint before we apply rate-limiting
	Threshold int64 `yaml:"threshold"`

	// The cooloff period in milliseconds after a request before the "slot"
	// is freed again
	CooloffMS int64 `yaml:"cooloff_ms"`

	// A list of users that are exempt from rate limiting, i.e. if you want
	// to run Mjolnir or other bots.
	ExemptUserIDs []string `yaml:"exempt_user_ids"`
}

func (r *RateLimiting) Verify(configErrs *Errors) {
	if r.Enabled {
		checkPositive(configErrs, "client_api.rate_limiting.threshold", r.Threshold)
		checkPositive(configErrs, "client_api.rate_limiting.cooloff_ms", r.CooloffMS)
	}
}

func (r *RateLimiting) Defaults() {
	r.Enabled = true
	r.Threshold = 5
	r.CooloffMS = 500
}

type ClientQueues struct {
	InputFulltextReindex QueueOptions `yaml:"input_fulltext_reindex"`
}

func (q *ClientQueues) Defaults(opts DefaultOpts) {
	q.InputFulltextReindex = opts.defaultQ(constants.InputFulltextReindex)
}

func (q *ClientQueues) Verify(configErrs *Errors) {
	checkNotEmpty(configErrs, "client_api.queues.input_fulltext_reindex", string(q.InputFulltextReindex.DS))
}
