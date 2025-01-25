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
	"fmt"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/oauth2"

	"github.com/antinvestor/matrix/setup/config"
)

// maxHTTPTimeout is an upper bound on an HTTP request to an LoginSSO
// backend. The individual request context deadlines are also
// honoured.
const maxHTTPTimeout = 10 * time.Second

// An Authenticator keeps a set of identity providers and dispatches
// calls to one of them, based on configured ID.
type Authenticator struct {
	providers map[string]ssoIdentityProvider
}

func NewAuthenticator(cfg *config.LoginSSO) *Authenticator {
	hc := &http.Client{
		Timeout: maxHTTPTimeout,
		Transport: &http.Transport{
			DisableKeepAlives: true,
			Proxy:             http.ProxyFromEnvironment,
		},
	}

	a := &Authenticator{
		providers: make(map[string]ssoIdentityProvider, len(cfg.Providers)),
	}
	for _, pcfg := range cfg.Providers {
		pcfg = pcfg.WithDefaults()

		a.providers[pcfg.ID] = newSSOIdentityProvider(&pcfg, hc)
	}

	return a
}

func (auth *Authenticator) AuthorizationURL(ctx context.Context, providerID, callbackURL, nonce, codeVerifier string) (string, error) {
	p := auth.providers[providerID]
	if p == nil {
		return "", fmt.Errorf("unknown identity provider: %s", providerID)
	}
	return p.AuthorizationURL(ctx, callbackURL, nonce, codeVerifier)
}

func (auth *Authenticator) ProcessCallback(ctx context.Context, partitionID, callbackURL, nonce, codeVerifier string, query url.Values) (*CallbackResult, error) {
	p := auth.providers[partitionID]
	if p == nil {
		return nil, fmt.Errorf("unknown partition provider: %s", partitionID)
	}
	return p.ProcessCallback(ctx, callbackURL, nonce, codeVerifier, query)
}

type ssoIdentityProvider interface {
	AuthorizationURL(ctx context.Context, callbackURL, nonce, codeVerifier string) (string, error)
	ProcessCallback(ctx context.Context, callbackURL, nonce, codeVerifier string, query url.Values) (*CallbackResult, error)
}

type CallbackResult struct {
	RedirectURL     string
	Identifier      UserIdentifier
	DisplayName     string
	SuggestedUserID string
	Token           *oauth2.Token
}

type UserIdentifier struct {
	Issuer, Subject string
}
