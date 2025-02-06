// Copyright 2022 The Matrix.org Foundation C.I.C.
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

package auth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
)

// oidcDiscoveryMaxStaleness indicates how stale the Discovery
// information is allowed to be. This will very rarely change, so
// we're just making sure even a Matrix that isn't restarting often
// is picking this up eventually.
const oidcDiscoveryMaxStaleness = 24 * time.Hour

// An oidcIdentityProvider wraps OAuth2 with OpenID Connect Discovery.
//
// The LoginSSO identifier is the "sub." A suggested UserID is grabbed from
// "preferred_username", though this isn't commonly provided.
//
// See https://openid.net/specs/openid-connect-core-1_0.html and https://openid.net/specs/openid-connect-discovery-1_0.html.
type oidcIdentityProvider struct {
	cfg *config.IdentityProvider
	hc  *http.Client

	userInfoURL string

	scopes []string

	resetOauth2Config bool
	oauth2Config      oauth2.Config

	responseMimeType string

	disc *oidcDiscovery
	exp  time.Time
	mu   sync.Mutex
}

func newSSOIdentityProvider(cfg *config.IdentityProvider, hc *http.Client) SSOIdentityProvider {

	return &oidcIdentityProvider{
		cfg: cfg,
		hc:  hc,

		scopes:           []string{"openid", "profile", "offline", "contact"},
		responseMimeType: "application/json",
	}
}

func (p *oidcIdentityProvider) AuthorizationURL(ctx context.Context, callbackURL, nonce, codeVerifier string) (string, error) {
	_, err := p.reload(ctx)
	if err != nil {
		return "", err
	}

	// Generate PKCE code verifier and challenge
	codeChallenge := p.generateCodeChallenge(codeVerifier)

	oauth2Config := p.oauth2Config
	oauth2Config.RedirectURL = callbackURL

	// Step 1: Redirect user to provider's authorization URL
	authURL := oauth2Config.AuthCodeURL(nonce,
		oauth2.AccessTypeOffline,
		oauth2.SetAuthURLParam("code_challenge", codeChallenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	)

	return authURL, nil
}

func (p *oidcIdentityProvider) ProcessCallback(ctx context.Context, callbackURL, nonce, codeVerifier string, query url.Values) (*CallbackResult, error) {

	logger := util.GetLogger(ctx)

	disc, err := p.reload(ctx)
	if err != nil {
		return nil, err
	}

	code := query.Get("code")
	if code == "" {
		return nil, spec.MissingParam("code parameter missing")
	}

	state := query.Get("state")
	if state == "" {
		return nil, spec.MissingParam("state parameter missing")
	}
	if state != nonce {
		return nil, spec.MissingParam("state parameter not matching nonce")
	}

	if errStr := query.Get("error"); errStr != "" {
		if euri := query.Get("error_uri"); euri != "" {
			return &CallbackResult{RedirectURL: euri}, nil
		}

		desc := query.Get("error_description")
		if desc == "" {
			desc = errStr
		}
		switch errStr {
		case "unauthorized_client", "access_denied": // nolint:misspell
			return nil, spec.Forbidden("LoginSSO said no: " + desc)
		default:
			return nil, fmt.Errorf("LoginSSO failed: %v", errStr)
		}
	}

	p.oauth2Config.RedirectURL = callbackURL
	codeVerifierOpt := oauth2.SetAuthURLParam("code_verifier", codeVerifier)

	// Exchange authorization code for a token
	token, err := p.oauth2Config.Exchange(ctx, code, codeVerifierOpt)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange token: %v", err)
	}

	logger.WithField("access token", token.AccessToken).WithField("refresh token", token.RefreshToken).Info("obtained token from authentication service")

	subject, displayName, suggestedLocalpart, err := p.getUserInfo(ctx, token.AccessToken)
	if err != nil {
		return nil, err
	}

	if subject == "" {
		return nil, fmt.Errorf("no subject from LoginSSO provider")
	}

	res := &CallbackResult{
		Identifier: UserIdentifier{
			Issuer:  disc.Issuer,
			Subject: subject,
		},
		DisplayName:     displayName,
		SuggestedUserID: suggestedLocalpart,
		Token:           token,
	}

	return res, nil
}

func (p *oidcIdentityProvider) reload(ctx context.Context) (*oidcDiscovery, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	if p.exp.Before(now) || p.disc == nil {
		disc, err := oidcDiscover(ctx, p.cfg.DiscoveryURL)
		if err != nil {
			if p.disc != nil {
				// Prefers returning a stale entry.
				return p.disc, nil
			}
			return nil, err
		}

		p.exp = now.Add(oidcDiscoveryMaxStaleness)
		p.userInfoURL = disc.UserinfoEndpoint
		p.disc = disc
		p.resetOauth2Config = true
	}

	if p.resetOauth2Config {
		// OAuth2 config with client_secret_post support
		p.oauth2Config = oauth2.Config{
			ClientID:     p.cfg.ClientID,
			ClientSecret: p.cfg.ClientSecret,
			Scopes:       []string{strings.Join(p.scopes, " ")},
			Endpoint: oauth2.Endpoint{
				AuthURL:   p.disc.AuthorizationEndpoint,
				TokenURL:  p.disc.TokenEndpoint,
				AuthStyle: oauth2.AuthStyleInParams, // Forces client_secret_post
			},
		}
		p.resetOauth2Config = false
	}

	return p.disc, nil
}

func (p *oidcIdentityProvider) generateCodeChallenge(verifier string) string {
	hash := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(hash[:])
}

func (p *oidcIdentityProvider) getUserInfo(ctx context.Context, accessToken string) (subject, displayName, suggestedLocalpart string, _ error) {
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, p.userInfoURL, nil)
	if err != nil {
		return "", "", "", err
	}
	hreq.Header.Set("Authorization", "Bearer "+accessToken)
	hreq.Header.Set("Accept", p.responseMimeType)

	hresp, err := httpDo(ctx, p.hc, hreq)
	if err != nil {
		return "", "", "", fmt.Errorf("user info: %w", err)
	}
	defer hresp.Body.Close() // nolint:errcheck

	body, err := io.ReadAll(hresp.Body)
	if err != nil {
		return "", "", "", err
	}

	var profileInfo map[string]any
	err = json.Unmarshal(body, &profileInfo)

	if err != nil {
		return "", "", "", fmt.Errorf("unmarshal user info response body: %w", err)
	}

	subject = profileInfo["sub"].(string)
	displayName = profileInfo["name"].(string)

	contacts, ok := profileInfo["contacts"].([]any)
	if ok {
		if len(contacts) == 0 {
			return "", "", "", fmt.Errorf("no contacts in user info response body")
		}

		firstContact := contacts[0].(map[string]any)
		suggestedLocalpart = firstContact["detail"].(string)
	}

	return
}

func httpDo(ctx context.Context, hc *http.Client, req *http.Request) (*http.Response, error) {
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close() // nolint:errcheck

		contentType := resp.Header.Get("Content-Type")
		switch {
		case strings.HasPrefix(contentType, "text/plain"):
			bs, err := io.ReadAll(resp.Body)
			if err == nil {
				if len(bs) > 80 {
					bs = bs[:80]
				}
				util.GetLogger(ctx).WithField("url", req.URL.String()).WithField("status", resp.StatusCode).Warnf("OAuth2 HTTP request failed: %s", string(bs))
			}
		case strings.HasPrefix(contentType, "application/json"):
			// https://openid.net/specs/openid-connect-core-1_0.html#TokenErrorResponse
			var body oauth2Error
			if err := json.NewDecoder(resp.Body).Decode(&body); err == nil {
				util.GetLogger(ctx).WithField("url", req.URL.String()).WithField("status", resp.StatusCode).Warnf("OAuth2 HTTP request failed: %+v", &body)
			}
			if body.Error != "" {
				return nil, fmt.Errorf("OAuth2 request %q failed: %s (%s)", req.URL.String(), resp.Status, body.Error)
			}
		}

		if hdr := resp.Header.Get("WWW-Authenticate"); hdr != "" {
			// https://openid.net/specs/openid-connect-core-1_0.html#UserInfoError
			if len(hdr) > 80 {
				hdr = hdr[:80]
			}
			return nil, fmt.Errorf("OAuth2 request %q failed: %s (%s)", req.URL.String(), resp.Status, hdr)
		}

		return nil, fmt.Errorf("OAuth2 HTTP request %q failed: %s", req.URL.String(), resp.Status)
	}

	return resp, nil
}

type oauth2Error struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
	ErrorURI         string `json:"error_uri"`
}

type oidcDiscovery struct {
	Issuer                string   `json:"issuer"`
	AuthorizationEndpoint string   `json:"authorization_endpoint"`
	TokenEndpoint         string   `json:"token_endpoint"`
	UserinfoEndpoint      string   `json:"userinfo_endpoint"`
	ScopesSupported       []string `json:"scopes_supported"`
}

func oidcDiscover(ctx context.Context, url string) (*oidcDiscovery, error) {
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	hreq.Header.Set("Accept", "application/jrd+json,application/json;q=0.9")

	hresp, err := http.DefaultClient.Do(hreq)
	if err != nil {
		return nil, err
	}
	defer hresp.Body.Close() // nolint:errcheck

	if hresp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("OIDC discovery request %q failed: %d %s", url, hresp.StatusCode, hresp.Status)
	}

	var disc oidcDiscovery
	if err := json.NewDecoder(hresp.Body).Decode(&disc); err != nil {
		return nil, fmt.Errorf("decoding OIDC discovery response from %q: %w", url, err)
	}

	if !validWebURL(disc.Issuer) {
		return nil, fmt.Errorf("issuer identifier is invalid in %q", url)
	}
	if !validWebURL(disc.AuthorizationEndpoint) {
		return nil, fmt.Errorf("authorization endpoint is invalid in %q", url)
	}
	if !validWebURL(disc.TokenEndpoint) {
		return nil, fmt.Errorf("token endpoint is invalid in %q", url)
	}
	if !validWebURL(disc.UserinfoEndpoint) {
		return nil, fmt.Errorf("userinfo endpoint is invalid in %q", url)
	}

	if disc.ScopesSupported != nil {
		if !stringSliceContains(disc.ScopesSupported, "openid") {
			return nil, fmt.Errorf("scope 'openid' is missing in %q", url)
		}
	}

	return &disc, nil
}

func validWebURL(s string) bool {
	if s == "" {
		return false
	}

	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	return u.Scheme != "" && u.Host != ""
}

func stringSliceContains(ss []string, s string) bool {
	for _, s2 := range ss {
		if s2 == s {
			return true
		}
	}
	return false
}
