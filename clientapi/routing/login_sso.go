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

package routing

import (
	"context"
	"encoding/base64"
	"fmt"
	"golang.org/x/exp/slog"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/antinvestor/matrix/clientapi/auth"
	"github.com/matrix-org/gomatrixserverlib/spec"

	"github.com/antinvestor/matrix/setup/config"
	uapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

// SSORedirect implements /login/sso/redirect
// https://spec.matrix.org/v1.2/client-server-api/#redirecting-to-the-authentication-server
func SSORedirect(
	req *http.Request,
	idpID string,
	auth ssoAuthenticator,
	cfg *config.LoginSSO,
) util.JSONResponse {
	ctx := req.Context()

	if auth == nil {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("authentication method disabled"),
		}
	}

	redirectURL := req.URL.Query().Get("redirectUrl")
	if redirectURL == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("redirectUrl parameter missing"),
		}
	}
	if ru, err := url.Parse(redirectURL); err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("Invalid redirectURL: " + err.Error()),
		}
	} else if ru.Scheme == "" || ru.Host == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("Invalid redirectURL: " + redirectURL),
		}
	}

	if idpID == "" {
		idpID = cfg.DefaultProviderID
		if idpID == "" && len(cfg.Providers) > 0 {
			idpID = cfg.Providers[0].ID
		}
	}

	callbackURL, err := buildCallbackURLFromOther(cfg, req, "/login/sso/redirect")
	if err != nil {
		util.GetLogger(ctx).With(slog.Any("error", err)).Error("Failed to build callback URL")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: err,
		}
	}

	callbackURL = callbackURL.ResolveReference(&url.URL{
		RawQuery: url.Values{"partition_id": []string{idpID}}.Encode(),
	})
	nonce := formatNonce(redirectURL)

	codeVerifier := genCodeVerifier()

	u, err := auth.AuthorizationURL(ctx, idpID, callbackURL.String(), nonce, codeVerifier)
	if err != nil {
		util.GetLogger(ctx).With(slog.Any("error", err)).Error("Failed to get LoginSSO authorization URL")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: err,
		}
	}

	util.GetLogger(ctx).Info("LoginSSO redirect to %s.", u)

	resp := util.RedirectResponse(u)
	nonceCookie := &http.Cookie{
		Name:     "sso_nonce",
		Value:    nonce,
		Path:     path.Dir(callbackURL.Path),
		Expires:  time.Now().Add(10 * time.Minute),
		Secure:   callbackURL.Scheme == "https",
		SameSite: http.SameSiteNoneMode,
	}
	if !nonceCookie.Secure {
		// SameSite=None requires Secure, so we might as well remove
		// it. See https://blog.chromium.org/2019/10/developers-get-ready-for-new.html.
		nonceCookie.SameSite = http.SameSiteDefaultMode
	}

	codeVerifierCookie := &http.Cookie{
		Name:     "sso_code_verifier",
		Value:    codeVerifier,
		Path:     path.Dir(callbackURL.Path),
		Expires:  time.Now().Add(10 * time.Minute),
		Secure:   callbackURL.Scheme == "https",
		SameSite: http.SameSiteNoneMode,
	}
	if !codeVerifierCookie.Secure {
		// SameSite=None requires Secure, so we might as well remove
		// it. See https://blog.chromium.org/2019/10/developers-get-ready-for-new.html.
		codeVerifierCookie.SameSite = http.SameSiteDefaultMode
	}

	resp.Headers["Set-Cookie"] = []*http.Cookie{nonceCookie, codeVerifierCookie}
	return resp
}

// buildCallbackURLFromOther builds a callback URL from another LoginSSO
// request and configuration.
func buildCallbackURLFromOther(cfg *config.LoginSSO, req *http.Request, expectedPath string) (*url.URL, error) {
	u := &url.URL{
		Scheme: "https",
		Host:   req.Host,
		Path:   req.URL.Path,
	}
	if req.TLS == nil {
		u.Scheme = "http"
	}

	// Find the v3mux base, handling both `redirect` and
	// `redirect/{idp}` and not hard-coding the Matrix version.
	i := strings.Index(u.Path, expectedPath)
	if i < 0 {
		return nil, fmt.Errorf("cannot find %q to replace in URL %q", expectedPath, u.Path)
	}
	u.Path = u.Path[:i] + "/login/sso/callback"

	cu, err := url.Parse(cfg.CallbackURL)
	if err != nil {
		return nil, err
	}
	return u.ResolveReference(cu), nil
}

// SSOCallback implements /login/sso/callback.
// https://spec.matrix.org/v1.2/client-server-api/#handling-the-callback-from-the-authentication-server
func SSOCallback(
	req *http.Request,
	userAPI userAPIForSSO,
	auth ssoAuthenticator,
	cfg *config.LoginSSO,
	serverName spec.ServerName,
) util.JSONResponse {
	if auth == nil {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("authentication method disabled"),
		}
	}

	ctx := req.Context()

	query := req.URL.Query()
	idpID := query.Get("partition_id")
	if idpID == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("provider parameter missing"),
		}
	}

	nonce, err := req.Cookie("sso_nonce")
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("no nonce cookie: " + err.Error()),
		}
	}
	finalRedirectURL, err := parseNonce(nonce.Value)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: err,
		}
	}

	codeVerifier, err := req.Cookie("sso_code_verifier")
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("no code verifier cookie: " + err.Error()),
		}
	}

	callbackURL, err := buildCallbackURLFromOther(cfg, req, "/login/sso/callback")
	if err != nil {
		util.GetLogger(ctx).With(slog.Any("error", err)).Error("Failed to build callback URL")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: err,
		}
	}

	callbackURL = callbackURL.ResolveReference(&url.URL{
		RawQuery: url.Values{"partition_id": []string{idpID}}.Encode(),
	})
	result, err := auth.ProcessCallback(ctx, idpID, callbackURL.String(), nonce.Value, codeVerifier.Value, query)
	if err != nil {
		util.GetLogger(ctx).With(slog.Any("error", err)).Error("Failed to process callback")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: err,
		}
	}
	util.GetLogger(ctx).With("result", result).Info("LoginSSO callback done")

	if result.Identifier.Subject == "" || result.Identifier.Issuer == "" {
		// Not authenticated yet.
		return util.RedirectResponse(result.RedirectURL)
	}

	account, err := verifyUserExits(ctx, serverName, userAPI, result.Identifier)
	if err != nil {
		util.GetLogger(ctx).With(slog.Any("error", err)).With("ssoIdentifier", result.Identifier).Error("failed to find user")
		return util.JSONResponse{
			Code: http.StatusUnauthorized,
			JSON: spec.Forbidden("ID not associated with a local account"),
		}
	}

	token, err := createLoginToken(ctx, userAPI, account.UserID)
	if err != nil {
		util.GetLogger(ctx).With(slog.Any("error", err)).Error("PerformLoginTokenCreation failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	util.GetLogger(ctx).With("account", account).With("ssoIdentifier", result.Identifier).Info("LoginSSO created token")

	rquery := finalRedirectURL.Query()
	rquery.Set("loginToken", token.Token)
	resp := util.RedirectResponse(finalRedirectURL.ResolveReference(&url.URL{RawQuery: rquery.Encode()}).String())
	resp.Headers["Set-Cookie"] = []*http.Cookie{{
		Name:   "sso_nonce",
		Value:  "",
		MaxAge: -1,
		Secure: true,
	}, {
		Name:   "sso_code_verifier",
		Value:  "",
		MaxAge: -1,
		Secure: true,
	}}
	return resp
}

type ssoAuthenticator interface {
	AuthorizationURL(ctx context.Context, providerID, callbackURL, nonce, codeVerifier string) (string, error)
	ProcessCallback(ctx context.Context, providerID, callbackURL, nonce, codeVerifier string, query url.Values) (*auth.CallbackResult, error)
}

type userAPIForSSO interface {
	uapi.LoginTokenInternalAPI

	PerformEnsureSSOAccountExists(ctx context.Context, req *uapi.QuerySSOAccountRequest, res *uapi.QuerySSOAccountResponse) error
}

// formatNonce creates a random nonce that also contains the URL.
func formatNonce(redirectURL string) string {
	return util.RandomString(16) + "." + base64.RawURLEncoding.EncodeToString([]byte(redirectURL))
}

func genCodeVerifier() string {
	return util.RandomString(128)
}

// parseNonce extracts the embedded URL from the nonce. The nonce
// should have been validated to be the original before calling this
// function. The URL is not integrity protected.
func parseNonce(s string) (redirectURL *url.URL, _ error) {
	if s == "" {
		return nil, spec.MissingParam("empty LoginSSO nonce cookie")
	}

	ss := strings.Split(s, ".")
	if len(ss) < 2 {
		return nil, spec.InvalidParam("malformed LoginSSO nonce cookie")
	}

	urlbs, err := base64.RawURLEncoding.DecodeString(ss[1])
	if err != nil {
		return nil, spec.InvalidParam("invalid redirect URL in LoginSSO nonce cookie")
	}
	u, err := url.Parse(string(urlbs))
	if err != nil {
		return nil, spec.InvalidParam("invalid redirect URL in LoginSSO nonce cookie: " + err.Error())
	}

	return u, nil
}

// verifyUserExits resolves an sso.UserIdentifier to a local
// part using the User API. Returns empty if there is no associated
// user.
func verifyUserExits(ctx context.Context, serverName spec.ServerName, userAPI userAPIForSSO, id auth.UserIdentifier) (account *uapi.Account, _ error) {
	req := &uapi.QuerySSOAccountRequest{
		ServerName: serverName,
		Issuer:     id.Issuer,
		Subject:    id.Subject,
	}
	var res uapi.QuerySSOAccountResponse
	if err := userAPI.PerformEnsureSSOAccountExists(ctx, req, &res); err != nil {
		return nil, err
	}

	if res.AccountCreated {
		amtRegUsers.Inc()
	}

	return res.Account, nil
}

// createLoginToken produces a new login token, valid for the given
// user.
func createLoginToken(ctx context.Context, userAPI userAPIForSSO, userID string) (*uapi.LoginTokenMetadata, error) {
	req := uapi.PerformLoginTokenCreationRequest{Data: uapi.LoginTokenData{UserID: userID}}
	var resp uapi.PerformLoginTokenCreationResponse
	if err := userAPI.PerformLoginTokenCreation(ctx, &req, &resp); err != nil {
		return nil, err
	}
	return &resp.Metadata, nil
}
