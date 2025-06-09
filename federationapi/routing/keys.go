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

package routing

import (
	"encoding/json"
	"github.com/pitabwire/frame"
	"net/http"
	"time"

	"github.com/pitabwire/util"
	"golang.org/x/crypto/ed25519"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	clienthttputil "github.com/antinvestor/matrix/clientapi/httputil"
	federationAPI "github.com/antinvestor/matrix/federationapi/api"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/userapi/api"
)

type queryKeysRequest struct {
	DeviceKeys map[string][]string `json:"device_keys"`
}

// QueryDeviceKeys returns device keys for users on this server.
// https://matrix.org/docs/spec/server_server/latest#post-matrix-federation-v1-user-keys-query
func QueryDeviceKeys(
	httpReq *http.Request, request *fclient.FederationRequest, keyAPI api.FederationKeyAPI, thisServer spec.ServerName,
) util.JSONResponse {
	var qkr queryKeysRequest
	err := json.Unmarshal(request.Content(), &qkr)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The request body could not be decoded into valid JSON. " + err.Error()),
		}
	}
	// make sure we only query users on our domain
	for userID := range qkr.DeviceKeys {
		_, serverName, err := gomatrixserverlib.SplitID('@', userID)
		if err != nil {
			delete(qkr.DeviceKeys, userID)
			continue // ignore invalid users
		}
		if serverName != thisServer {
			delete(qkr.DeviceKeys, userID)
			continue
		}
	}

	var queryRes api.QueryKeysResponse
	keyAPI.QueryKeys(httpReq.Context(), &api.QueryKeysRequest{
		UserToDevices: qkr.DeviceKeys,
	}, &queryRes)
	if queryRes.Error != nil {
		util.GetLogger(httpReq.Context()).WithError(queryRes.Error).Error("Failed to QueryKeys")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return util.JSONResponse{
		Code: 200,
		JSON: struct {
			DeviceKeys      interface{} `json:"device_keys"`
			MasterKeys      interface{} `json:"master_keys"`
			SelfSigningKeys interface{} `json:"self_signing_keys"`
		}{
			queryRes.DeviceKeys,
			queryRes.MasterKeys,
			queryRes.SelfSigningKeys,
		},
	}
}

type claimOTKsRequest struct {
	OneTimeKeys map[string]map[string]string `json:"one_time_keys"`
}

// ClaimOneTimeKeys claims OTKs for users on this server.
// https://matrix.org/docs/spec/server_server/latest#post-matrix-federation-v1-user-keys-claim
func ClaimOneTimeKeys(
	httpReq *http.Request, request *fclient.FederationRequest, keyAPI api.FederationKeyAPI, thisServer spec.ServerName,
) util.JSONResponse {
	var cor claimOTKsRequest
	err := json.Unmarshal(request.Content(), &cor)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The request body could not be decoded into valid JSON. " + err.Error()),
		}
	}
	// make sure we only claim users on our domain
	for userID := range cor.OneTimeKeys {
		_, serverName, err := gomatrixserverlib.SplitID('@', userID)
		if err != nil {
			delete(cor.OneTimeKeys, userID)
			continue // ignore invalid users
		}
		if serverName != thisServer {
			delete(cor.OneTimeKeys, userID)
			continue
		}
	}

	var claimRes api.PerformClaimKeysResponse
	keyAPI.PerformClaimKeys(httpReq.Context(), &api.PerformClaimKeysRequest{
		OneTimeKeys: cor.OneTimeKeys,
	}, &claimRes)
	if claimRes.Error != nil {
		util.GetLogger(httpReq.Context()).WithError(claimRes.Error).Error("Failed to PerformClaimKeys")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return util.JSONResponse{
		Code: 200,
		JSON: struct {
			OneTimeKeys interface{} `json:"one_time_keys"`
		}{claimRes.OneTimeKeys},
	}
}

// LocalKeys returns the local keys for the server.
// See https://matrix.org/docs/spec/server_server/unstable.html#publishing-keys
func LocalKeys(cfg *config.FederationAPI, serverName spec.ServerName) util.JSONResponse {
	keys, err := localKeys(cfg, serverName)
	if err != nil {
		return util.MessageResponse(http.StatusNotFound, err.Error())
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: keys}
}

func localKeys(cfg *config.FederationAPI, serverName spec.ServerName) (*gomatrixserverlib.ServerKeys, error) {
	var keys gomatrixserverlib.ServerKeys
	var identity *fclient.SigningIdentity
	var err error
	if virtualHost := cfg.Global.VirtualHostForHTTPHost(serverName); virtualHost == nil {
		if identity, err = cfg.Global.SigningIdentityFor(cfg.Global.ServerName); err != nil {
			return nil, err
		}
		publicKey := cfg.Global.PrivateKey.Public().(ed25519.PublicKey)
		keys.ServerName = cfg.Global.ServerName
		keys.ValidUntilTS = spec.AsTimestamp(time.Now().Add(cfg.Global.KeyValidityPeriod))
		keys.VerifyKeys = map[gomatrixserverlib.KeyID]gomatrixserverlib.VerifyKey{
			cfg.Global.KeyID: {
				Key: spec.Base64Bytes(publicKey),
			},
		}
		keys.OldVerifyKeys = map[gomatrixserverlib.KeyID]gomatrixserverlib.OldVerifyKey{}
		for _, oldVerifyKey := range cfg.Global.OldVerifyKeys {
			keys.OldVerifyKeys[oldVerifyKey.KeyID] = gomatrixserverlib.OldVerifyKey{
				VerifyKey: gomatrixserverlib.VerifyKey{
					Key: oldVerifyKey.PublicKey,
				},
				ExpiredTS: oldVerifyKey.ExpiredAt,
			}
		}
	} else {
		if identity, err = cfg.Global.SigningIdentityFor(virtualHost.ServerName); err != nil {
			return nil, err
		}
		publicKey := virtualHost.PrivateKey.Public().(ed25519.PublicKey)
		keys.ServerName = virtualHost.ServerName
		keys.ValidUntilTS = spec.AsTimestamp(time.Now().Add(virtualHost.KeyValidityPeriod))
		keys.VerifyKeys = map[gomatrixserverlib.KeyID]gomatrixserverlib.VerifyKey{
			virtualHost.KeyID: {
				Key: spec.Base64Bytes(publicKey),
			},
		}
		// TODO: Virtual hosts probably want to be able to specify old signing
		// keys too, just in case
	}

	toSign, err := json.Marshal(keys.ServerKeyFields)
	if err != nil {
		return nil, err
	}

	keys.Raw, err = gomatrixserverlib.SignJSON(
		string(identity.ServerName), identity.KeyID, identity.PrivateKey, toSign,
	)
	return &keys, err
}

type NotaryKeysResponse struct {
	ServerKeys []json.RawMessage `json:"server_keys"`
}

func NotaryKeys(
	httpReq *http.Request, cfg *config.FederationAPI,
	fsAPI federationAPI.FederationInternalAPI,
	req *gomatrixserverlib.PublicKeyNotaryLookupRequest,
) util.JSONResponse {

	ctx := httpReq.Context()

	serverName := spec.ServerName(httpReq.Host) // TODO: this is not ideal
	if !cfg.Global.IsLocalServerName(serverName) {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Server name not known"),
		}
	}

	if req == nil {
		req = &gomatrixserverlib.PublicKeyNotaryLookupRequest{}
		if reqErr := clienthttputil.UnmarshalJSONRequest(httpReq, &req); reqErr != nil {
			return *reqErr
		}
	}

	response := NotaryKeysResponse{
		ServerKeys: []json.RawMessage{},
	}

	for serverName, kidToCriteria := range req.ServerKeys {
		var keyList []gomatrixserverlib.ServerKeys
		if serverName == cfg.Global.ServerName {
			if k, err := localKeys(cfg, serverName); err == nil {
				keyList = append(keyList, *k)
			} else {
				return util.ErrorResponse(err)
			}
		} else {
			var resp federationAPI.QueryServerKeysResponse
			err := fsAPI.QueryServerKeys(httpReq.Context(), &federationAPI.QueryServerKeysRequest{
				ServerName:      serverName,
				KeyIDToCriteria: kidToCriteria,
			}, &resp)
			if err != nil {
				return util.ErrorResponse(err)
			}
			keyList = append(keyList, resp.ServerKeys...)
		}
		if len(keyList) == 0 {
			continue
		}

		for _, keys := range keyList {
			j, err := json.Marshal(keys)
			if err != nil {
				frame.Log(ctx).WithError(err).Error("Failed to marshal %q response", serverName)
				return util.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}

			js, err := gomatrixserverlib.SignJSON(
				string(cfg.Global.ServerName), cfg.Global.KeyID, cfg.Global.PrivateKey, j,
			)
			if err != nil {
				frame.Log(ctx).WithError(err).Error("Failed to sign %q response", serverName)
				return util.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}

			response.ServerKeys = append(response.ServerKeys, js)
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: response,
	}
}
