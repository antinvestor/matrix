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

package threepid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/setup/config"
)

// EmailAssociationRequest represents the request defined at https://matrix.org/docs/spec/client_server/r0.2.0.html#post-matrix-client-r0-register-email-requesttoken
type EmailAssociationRequest struct {
	IDServer    string `json:"id_server"`
	Secret      string `json:"client_secret"`
	Email       string `json:"email"`
	SendAttempt int    `json:"send_attempt"`
}

// EmailAssociationCheckRequest represents the request defined at https://matrix.org/docs/spec/client_server/r0.2.0.html#post-matrix-client-r0-account-3pid
type EmailAssociationCheckRequest struct {
	Creds Credentials `json:"three_pid_creds"`
	Bind  bool        `json:"bind"`
}

// Credentials represents the "ThreePidCredentials" structure defined at https://matrix.org/docs/spec/client_server/r0.2.0.html#post-matrix-client-r0-account-3pid
type Credentials struct {
	SID      string `json:"sid"`
	IDServer string `json:"id_server"`
	Secret   string `json:"client_secret"`
}

type SID struct {
	SID string `json:"sid"`
}

// CreateSession creates a session on an identity server.
// Returns the session's ID.
// Returns an error if there was a problem sending the request or decoding the
// response, or if the identity server responded with a non-OK status.
func CreateSession(
	ctx context.Context, req EmailAssociationRequest, cfg *config.ClientAPI, client *fclient.Client,
) (string, error) {
	if err := isTrusted(req.IDServer, cfg); err != nil {
		return "", err
	}

	// Create a session on the ID server
	postURL := fmt.Sprintf("https://%s/_matrix/identity/api/v1/validate/email/requestToken", req.IDServer)

	data := url.Values{}
	data.Add("client_secret", req.Secret)
	data.Add("email", req.Email)
	data.Add("send_attempt", strconv.Itoa(req.SendAttempt))

	request, err := http.NewRequest(http.MethodPost, postURL, strings.NewReader(data.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.DoHTTPRequest(ctx, request)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close() // nolint: errcheck

	// Error if the status isn't OK
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("could not create a session on the server %s", req.IDServer)
	}

	// Extract the SID from the response and return it
	var sid SID
	err = json.NewDecoder(resp.Body).Decode(&sid)

	return sid.SID, err
}

type GetValidatedResponse struct {
	Medium      string `json:"medium"`
	ValidatedAt int64  `json:"validated_at"`
	Address     string `json:"address"`
	ErrCode     string `json:"errcode"`
	Error       string `json:"error"`
}

// CheckAssociation checks the status of an ongoing association validation on an
// identity server.
// Returns a boolean set to true if the association has been validated, false if not.
// If the association has been validated, also returns the related third-party
// identifier and its medium.
// Returns an error if there was a problem sending the request or decoding the
// response, or if the identity server responded with a non-OK status.
func CheckAssociation(
	ctx context.Context, creds Credentials, cfg *config.ClientAPI,
	client *fclient.Client,
) (bool, string, string, error) {
	if err := isTrusted(creds.IDServer, cfg); err != nil {
		return false, "", "", err
	}

	requestURL := fmt.Sprintf("https://%s/_matrix/identity/api/v1/3pid/getValidated3pid?sid=%s&client_secret=%s", creds.IDServer, creds.SID, creds.Secret)
	req, err := http.NewRequest(http.MethodGet, requestURL, nil)
	if err != nil {
		return false, "", "", err
	}
	resp, err := client.DoHTTPRequest(ctx, req)
	if err != nil {
		return false, "", "", err
	}

	var respBody GetValidatedResponse
	if err = json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		return false, "", "", err
	}

	if respBody.ErrCode == string(spec.ErrorSessionNotValidated) {
		return false, "", "", nil
	} else if len(respBody.ErrCode) > 0 {
		return false, "", "", errors.New(respBody.Error)
	}

	return true, respBody.Address, respBody.Medium, nil
}

// PublishAssociation publishes a validated association between a third-party
// identifier and a Global ID.
// Returns an error if there was a problem sending the request or decoding the
// response, or if the identity server responded with a non-OK status.
func PublishAssociation(ctx context.Context, creds Credentials, userID string, cfg *config.ClientAPI, client *fclient.Client) error {
	if err := isTrusted(creds.IDServer, cfg); err != nil {
		return err
	}

	postURL := fmt.Sprintf("https://%s/_matrix/identity/api/v1/3pid/bind", creds.IDServer)

	data := url.Values{}
	data.Add("sid", creds.SID)
	data.Add("client_secret", creds.Secret)
	data.Add("mxid", userID)

	request, err := http.NewRequest(http.MethodPost, postURL, strings.NewReader(data.Encode()))
	if err != nil {
		return err
	}
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.DoHTTPRequest(ctx, request)
	if err != nil {
		return err
	}

	// Error if the status isn't OK
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("could not publish the association on the server %s", creds.IDServer)
	}

	return nil
}

// isTrusted checks if a given identity server is part of the list of trusted
// identity servers in the configuration file.
// Returns an error if the server isn't trusted.
func isTrusted(idServer string, cfg *config.ClientAPI) error {
	for _, server := range cfg.Global.TrustedIDServers {
		if idServer == server {
			return nil
		}
	}
	return ErrNotTrusted{}
}
