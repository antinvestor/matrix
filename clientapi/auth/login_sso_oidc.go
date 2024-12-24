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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/tidwall/gjson"

	"github.com/antinvestor/matrix/setup/config"
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

	authorizationURL string
	accessTokenURL   string
	userInfoURL      string

	scopes              []string
	responseMimeType    string
	subPath             string
	displayNamePath     string
	suggestedUserIDPath string

	disc *oidcDiscovery
	exp  time.Time
	mu   sync.Mutex
}

func newSSOIdentityProvider(cfg *config.IdentityProvider, hc *http.Client) *oidcIdentityProvider {
	return &oidcIdentityProvider{
		cfg: cfg,
		hc:  hc,

		scopes:              []string{"openid", "profile", "email"},
		responseMimeType:    "application/json",
		subPath:             "sub",
		displayNamePath:     "name",
		suggestedUserIDPath: "preferred_username",
	}
}

func (p *oidcIdentityProvider) AuthorizationURL(ctx context.Context, callbackURL, nonce string) (string, error) {
	_, err := p.reload(ctx)
	if err != nil {
		return "", err
	}

	u, err := resolveURL(p.authorizationURL, url.Values{
		"client_id":     []string{p.cfg.ClientID},
		"response_type": []string{"code"},
		"redirect_uri":  []string{callbackURL},
		"scope":         []string{strings.Join(p.scopes, " ")},
		"state":         []string{nonce},
	})
	if err != nil {
		return "", err
	}
	return u.String(), nil

}

func (p *oidcIdentityProvider) ProcessCallback(ctx context.Context, callbackURL, nonce string, query url.Values) (*CallbackResult, error) {
	disc, err := p.reload(ctx)
	if err != nil {
		return nil, err
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

	code := query.Get("code")
	if code == "" {
		return nil, spec.MissingParam("code parameter missing")
	}

	at, err := p.getAccessToken(ctx, callbackURL, code)
	if err != nil {
		return nil, err
	}

	subject, displayName, suggestedLocalpart, err := p.getUserInfo(ctx, at)
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
		p.authorizationURL = disc.AuthorizationEndpoint
		p.accessTokenURL = disc.TokenEndpoint
		p.userInfoURL = disc.UserinfoEndpoint
		p.disc = disc
	}

	return p.disc, nil
}

func (p *oidcIdentityProvider) getAccessToken(ctx context.Context, callbackURL, code string) (string, error) {
	body := url.Values{
		"grant_type":    []string{"authorization_code"},
		"code":          []string{code},
		"redirect_uri":  []string{callbackURL},
		"client_id":     []string{p.cfg.ClientID},
		"client_secret": []string{p.cfg.ClientSecret},
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.accessTokenURL, strings.NewReader(body.Encode()))
	if err != nil {
		return "", err
	}
	hreq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	hreq.Header.Set("Accept", p.responseMimeType)

	hresp, err := httpDo(ctx, p.hc, hreq)
	if err != nil {
		return "", fmt.Errorf("access token: %w", err)
	}
	defer hresp.Body.Close() // nolint:errcheck

	var resp oauth2TokenResponse
	if err := json.NewDecoder(hresp.Body).Decode(&resp); err != nil {
		return "", err
	}

	if strings.ToLower(resp.TokenType) != "bearer" {
		return "", fmt.Errorf("expected bearer token, got type %q", resp.TokenType)
	}

	return resp.AccessToken, nil
}

type oauth2TokenResponse struct {
	TokenType   string `json:"token_type"`
	AccessToken string `json:"access_token"`
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

	if res := gjson.GetBytes(body, p.subPath); !res.Exists() {
		return "", "", "", fmt.Errorf("no %q in user info response body", p.subPath)
	} else {
		subject = res.String()
	}
	if subject == "" {
		return "", "", "", fmt.Errorf("empty subject in user info")
	}

	if p.suggestedUserIDPath != "" {
		suggestedLocalpart = gjson.GetBytes(body, p.suggestedUserIDPath).String()
	}

	if p.displayNamePath != "" {
		displayName = gjson.GetBytes(body, p.displayNamePath).String()
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

func resolveURL(urlString string, defaultQuery url.Values) (*url.URL, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}

	if defaultQuery != nil {
		q := u.Query()
		for k, vs := range defaultQuery {
			if q.Get(k) == "" {
				q[k] = vs
			}
		}
		u.RawQuery = q.Encode()
	}

	return u, nil
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