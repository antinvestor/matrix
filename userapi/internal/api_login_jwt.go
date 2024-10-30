// Copyright 2021 The Matrix.org Foundation C.I.C.
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

package internal

import (
	"context"
	"crypto/rsa"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"strings"

	"github.com/antinvestor/matrix/userapi/api"
	"github.com/golang-jwt/jwt/v5"
	"github.com/matrix-org/util"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// Prometheus metrics
	jwtAutoCreateUsers = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "matrix",
			Subsystem: "client_api",
			Name:      "jwt_created_user_total",
			Help:      "Total number of users via jwt",
		},
	)
)

type Jwks struct {
	Keys []JSONWebKeys `json:"keys"`
}

type JSONWebKeys struct {
	Kty string   `json:"kty"`
	Kid string   `json:"kid"`
	Use string   `json:"use"`
	N   string   `json:"n"`
	E   string   `json:"e"`
	X5c []string `json:"x5c"`
}

// QueryLoginJWT returns the data associated with a valid login token
func (a *UserInternalAPI) QueryLoginJWT(ctx context.Context, req *api.QueryLoginJWTRequest, res *api.QueryLoginJWTResponse) error {

	options := []jwt.ParserOption{
		jwt.WithAudience(a.Config.JWTLogin.Audience),
		jwt.WithIssuer(a.Config.JWTLogin.Issuer)}

	claims := &api.JWTClaims{}

	token, err := jwt.ParseWithClaims(req.Token, claims, a.getPemCert, options...)
	if err != nil {
		return err
	}

	if !token.Valid {
		return errors.New("supplied jwt token was invalid")
	}

	localPart, err := token.Claims.GetSubject()
	if err != nil {
		return err
	}

	serverName := a.Config.Matrix.ServerName

	var account *api.Account
	account, err = a.DB.GetAccountByLocalpart(ctx, localPart, serverName)
	if err != nil {
		res.Data = nil
		if !errors.Is(err, sql.ErrNoRows) {

			return err
		}

		util.GetLogger(ctx).WithField("local part", localPart).Info("No account exists with the profile id")

		var accRes api.PerformAccountCreationResponse
		err = a.PerformAccountCreation(ctx, &api.PerformAccountCreationRequest{
			Localpart:   localPart,
			ServerName:  serverName,
			AccountType: api.AccountTypeUser,
			OnConflict:  api.ConflictAbort,
		}, &accRes)

		if err != nil {
			return err
		}

		// Increment prometheus counter for created users
		jwtAutoCreateUsers.Inc()

		account = accRes.Account
	}
	res.Data = &api.LoginTokenData{
		UserID: account.UserID,
	}

	return nil
}

func (a *UserInternalAPI) getPemCert(token *jwt.Token) (any, error) {

	oauth2WellKnownJwkUrl := a.Config.JWTLogin.Oauth2WellKnownJwkUri

	if oauth2WellKnownJwkUrl == "" {
		return nil, errors.New("web key URL is invalid")
	}

	jwkKeyString, ok := a.JWTCertMap[oauth2WellKnownJwkUrl]
	if !ok {
		resp, err := http.Get(oauth2WellKnownJwkUrl)

		if err != nil {
			return nil, err
		}
		defer func(Body io.ReadCloser) {
			_ = Body.Close()
		}(resp.Body)
		var jwkKeyBytes []byte
		jwkKeyBytes, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		jwkKeyString = string(jwkKeyBytes)
		a.JWTCertMap[oauth2WellKnownJwkUrl] = jwkKeyString
	}

	var jwks = Jwks{}
	err := json.NewDecoder(strings.NewReader(jwkKeyString)).Decode(&jwks)
	if err != nil {
		return nil, err
	}

	for k, val := range jwks.Keys {
		if token.Header["kid"] == jwks.Keys[k].Kid {
			var exponent []byte
			if exponent, err = base64.RawURLEncoding.DecodeString(val.E); err != nil {
				return nil, err
			}

			// Decode the modulus from Base64.
			var modulus []byte
			if modulus, err = base64.RawURLEncoding.DecodeString(val.N); err != nil {
				return nil, err
			}

			// Create the RSA public key.
			publicKey := &rsa.PublicKey{}

			// Turn the exponent into an integer.
			//
			// According to RFC 7517, these numbers are in big-endian format.
			// https://tools.ietf.org/html/rfc7517#appendix-A.1
			publicKey.E = int(big.NewInt(0).SetBytes(exponent).Uint64())

			// Turn the modulus into a *big.Int.
			publicKey.N = big.NewInt(0).SetBytes(modulus)

			return publicKey, nil
		}
	}

	return nil, errors.New("unable to find appropriate key")
}
