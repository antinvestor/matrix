// Copyright 2022 The Global.org Foundation C.I.C.
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
	"sync"
	"time"

	partitionv1 "github.com/antinvestor/apis/go/partition/v1"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame"
	"golang.org/x/oauth2"
)

// maxHTTPTimeout is an upper bound on an HTTP request to an LoginSSO
// backend. The individual request context deadlines are also
// honoured.
const maxHTTPTimeout = 10 * time.Second

const partitionPropertyClientIDKey = "client_id"
const partitionPropertyClientSecretKey = "client_secret"
const partitionPropertyDiscoveryUriKey = "client_discovery_uri"

// An Authenticator keeps a set of identity providers and dispatches
// calls to one of them, based on configured ID.
type Authenticator struct {
	cfg          *config.LoginSSO
	partitionCli *partitionv1.PartitionClient
	providers    sync.Map
}

func NewAuthenticator(cfg *config.LoginSSO, partitionCli *partitionv1.PartitionClient) *Authenticator {

	a := &Authenticator{
		cfg:          cfg,
		partitionCli: partitionCli,
		providers:    sync.Map{},
	}

	hc := &http.Client{
		Timeout: maxHTTPTimeout,
		Transport: &http.Transport{
			DisableKeepAlives: true,
			Proxy:             http.ProxyFromEnvironment,
		},
	}

	for _, pcfg := range cfg.Providers {
		pcfg = pcfg.WithDefaults()
		provider := newSSOIdentityProvider(&pcfg, hc)
		a.providers.Store(pcfg.ID, provider)
	}

	return a
}

func (auth *Authenticator) GetProvider(ctx context.Context, providerID string) (SSOIdentityProvider, error) {

	if providerID == "" {
		providerID = auth.cfg.DefaultProviderID
	}

	storedVal, ok := auth.providers.Load(providerID)
	if ok {
		return storedVal.(SSOIdentityProvider), nil
	}

	if auth.partitionCli == nil {
		return nil, fmt.Errorf("no provider resolved")
	}

	resp, err := auth.partitionCli.GetPartition(ctx, providerID)
	if err != nil {
		return nil, err
	}

	var partitionProperties frame.JSONMap = resp.GetProperties().AsMap()
	partitionClientID := partitionProperties.GetString(partitionPropertyClientIDKey)
	if partitionClientID == "" {
		return nil, fmt.Errorf("no client_id in partition properties :%v", partitionProperties)
	}

	partitionClientSecret := partitionProperties.GetString(partitionPropertyClientSecretKey)
	if partitionClientSecret == "" {
		return nil, fmt.Errorf("no client_secret in partition properties :%v", partitionProperties)
	}

	partitionDiscoveryUri := partitionProperties.GetString(partitionPropertyDiscoveryUriKey)
	if partitionDiscoveryUri == "" {
		return nil, fmt.Errorf("no discovery uri in partition properties :%v", partitionProperties)
	}

	idp := config.IdentityProvider{
		ID:           providerID,
		Name:         resp.GetName(),
		ClientID:     partitionClientID,
		ClientSecret: partitionClientSecret,
		DiscoveryURL: partitionDiscoveryUri,
		JWTLogin: config.JWTLogin{
			Audience: "service_matrix",
		},
	}

	hc := &http.Client{
		Timeout: maxHTTPTimeout,
		Transport: &http.Transport{
			DisableKeepAlives: true,
			Proxy:             http.ProxyFromEnvironment,
		},
	}

	provider := newSSOIdentityProvider(&idp, hc)
	auth.providers.Store(idp.ID, provider)
	return provider, nil

}

func (auth *Authenticator) AuthorizationURL(ctx context.Context, partitionID, callbackURL, nonce, codeVerifier string) (string, error) {
	p, err := auth.GetProvider(ctx, partitionID)
	if err != nil {
		return "", err
	}
	return p.AuthorizationURL(ctx, callbackURL, nonce, codeVerifier)
}

func (auth *Authenticator) ProcessCallback(ctx context.Context, partitionID, callbackURL, nonce, codeVerifier string, query url.Values) (*CallbackResult, error) {
	p, err := auth.GetProvider(ctx, partitionID)
	if err != nil {
		return nil, err
	}
	return p.ProcessCallback(ctx, callbackURL, nonce, codeVerifier, query)
}

type SSOIdentityProvider interface {
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
