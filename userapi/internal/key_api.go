// Copyright 2025 Ant Investor Ltd.
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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func (a *UserInternalAPI) QueryKeyChanges(ctx context.Context, req *api.QueryKeyChangesRequest, res *api.QueryKeyChangesResponse) error {
	userIDs, latest, err := a.KeyDatabase.KeyChanges(ctx, req.Offset, req.ToOffset)
	if err != nil {
		res.Error = &api.KeyError{
			Err: err.Error(),
		}
		return nil
	}
	res.Offset = latest
	res.UserIDs = userIDs
	return nil
}

func (a *UserInternalAPI) PerformUploadKeys(ctx context.Context, req *api.PerformUploadKeysRequest, res *api.PerformUploadKeysResponse) error {
	res.KeyErrors = make(map[string]map[string]*api.KeyError)
	if len(req.DeviceKeys) > 0 {
		a.uploadLocalDeviceKeys(ctx, req, res)
	}
	if len(req.OneTimeKeys) > 0 {
		a.uploadOneTimeKeys(ctx, req, res)
	}
	otks, err := a.KeyDatabase.OneTimeKeysCount(ctx, req.UserID, req.DeviceID)
	if err != nil {
		return err
	}
	res.OneTimeKeyCounts = []api.OneTimeKeysCount{*otks}
	return nil
}

func (a *UserInternalAPI) PerformClaimKeys(ctx context.Context, req *api.PerformClaimKeysRequest, res *api.PerformClaimKeysResponse) {
	res.OneTimeKeys = make(map[string]map[string]map[string]json.RawMessage)
	res.Failures = make(map[string]interface{})
	// wrap request map in a top-level by-domain map
	domainToDeviceKeys := make(map[string]map[string]map[string]string)
	for userID, val := range req.OneTimeKeys {
		_, serverName, err := gomatrixserverlib.SplitID('@', userID)
		if err != nil {
			continue // ignore invalid users
		}
		nested, ok := domainToDeviceKeys[string(serverName)]
		if !ok {
			nested = make(map[string]map[string]string)
		}
		nested[userID] = val
		domainToDeviceKeys[string(serverName)] = nested
	}
	for domain, local := range domainToDeviceKeys {
		if !a.Config.Global.IsLocalServerName(spec.ServerName(domain)) {
			continue
		}
		// claim local keys
		keys, err := a.KeyDatabase.ClaimKeys(ctx, local)
		if err != nil {
			res.Error = &api.KeyError{
				Err: fmt.Sprintf("failed to ClaimKeys locally: %s", err),
			}
		}
		util.Log(ctx).WithField("keys_claimed", len(keys)).WithField("num_users", len(local)).Info("Claimed local keys")
		for _, key := range keys {
			_, ok := res.OneTimeKeys[key.UserID]
			if !ok {
				res.OneTimeKeys[key.UserID] = make(map[string]map[string]json.RawMessage)
			}
			_, ok = res.OneTimeKeys[key.UserID][key.DeviceID]
			if !ok {
				res.OneTimeKeys[key.UserID][key.DeviceID] = make(map[string]json.RawMessage)
			}
			for keyID, keyJSON := range key.KeyJSON {
				res.OneTimeKeys[key.UserID][key.DeviceID][keyID] = keyJSON
			}
		}
		delete(domainToDeviceKeys, domain)
	}
	if len(domainToDeviceKeys) > 0 {
		a.claimRemoteKeys(ctx, req.Timeout, res, domainToDeviceKeys)
	}
}

func (a *UserInternalAPI) claimRemoteKeys(
	ctx context.Context, timeout time.Duration, res *api.PerformClaimKeysResponse, domainToDeviceKeys map[string]map[string]map[string]string,
) {
	var wg sync.WaitGroup // Wait for fan-out goroutines to finish
	var mu sync.Mutex     // Protects the response struct
	var claimed int       // Number of keys claimed in total
	var failures int      // Number of servers we failed to ask

	util.Log(ctx).WithField("num_servers", len(domainToDeviceKeys)).Info("Claiming remote keys")
	wg.Add(len(domainToDeviceKeys))

	for d, k := range domainToDeviceKeys {
		go func(domain string, keysToClaim map[string]map[string]string) {
			fedCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			defer wg.Done()

			claimKeyRes, err := a.FedClient.ClaimKeys(fedCtx, a.Config.Global.ServerName, spec.ServerName(domain), keysToClaim)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				util.Log(ctx).WithError(err).WithField("server", domain).Error("ClaimKeys failed")
				res.Failures[domain] = map[string]interface{}{
					"message": err.Error(),
				}
				failures++
				return
			}

			for userID, deviceIDToKeys := range claimKeyRes.OneTimeKeys {
				res.OneTimeKeys[userID] = make(map[string]map[string]json.RawMessage)
				for deviceID, keys := range deviceIDToKeys {
					res.OneTimeKeys[userID][deviceID] = keys
					claimed += len(keys)
				}
			}
		}(d, k)
	}

	wg.Wait()
	util.Log(ctx).WithField("num_keys", claimed).WithField("num_servers", len(domainToDeviceKeys)).WithField("num_failures", failures).Info("Claimed remote keys")
}

func (a *UserInternalAPI) PerformDeleteKeys(ctx context.Context, req *api.PerformDeleteKeysRequest, res *api.PerformDeleteKeysResponse) error {
	if err := a.KeyDatabase.DeleteDeviceKeys(ctx, req.UserID, req.KeyIDs); err != nil {
		res.Error = &api.KeyError{
			Err: fmt.Sprintf("Failed to delete device keys: %s", err),
		}
	}
	return nil
}

func (a *UserInternalAPI) QueryOneTimeKeys(ctx context.Context, req *api.QueryOneTimeKeysRequest, res *api.QueryOneTimeKeysResponse) error {
	count, err := a.KeyDatabase.OneTimeKeysCount(ctx, req.UserID, req.DeviceID)
	if err != nil {
		res.Error = &api.KeyError{
			Err: fmt.Sprintf("Failed to query OTK counts: %s", err),
		}
		return nil
	}
	res.Count = *count
	return nil
}

func (a *UserInternalAPI) QueryDeviceMessages(ctx context.Context, req *api.QueryDeviceMessagesRequest, res *api.QueryDeviceMessagesResponse) error {
	msgs, err := a.KeyDatabase.DeviceKeysForUser(ctx, req.UserID, nil, false)
	if err != nil {
		res.Error = &api.KeyError{
			Err: fmt.Sprintf("failed to query Cm for device keys: %s", err),
		}
		return nil
	}
	maxStreamID := int64(0)
	// remove deleted devices
	var result []api.DeviceMessage
	for _, m := range msgs {
		if m.StreamID > maxStreamID {
			maxStreamID = m.StreamID
		}
		if len(m.KeyJSON) == 0 {
			continue
		}
		result = append(result, m)
	}
	res.Devices = result
	res.StreamID = maxStreamID
	return nil
}

// PerformMarkAsStaleIfNeeded marks the users device list as stale, if the given deviceID is not present
// in our database.
func (a *UserInternalAPI) PerformMarkAsStaleIfNeeded(ctx context.Context, req *api.PerformMarkAsStaleRequest, res *struct{}) error {
	knownDevices, err := a.KeyDatabase.DeviceKeysForUser(ctx, req.UserID, []string{}, true)
	if err != nil {
		return err
	}
	if len(knownDevices) == 0 {
		return nil // fmt.Errorf("unknown user %s", req.UserID)
	}

	for i := range knownDevices {
		if knownDevices[i].DeviceID == req.DeviceID {
			return nil // we already know about this device
		}
	}

	return a.Updater.ManualUpdate(ctx, req.Domain, req.UserID)
}

// nolint:gocyclo
func (a *UserInternalAPI) QueryKeys(ctx context.Context, req *api.QueryKeysRequest, res *api.QueryKeysResponse) {
	var respMu sync.Mutex
	res.DeviceKeys = make(map[string]map[string]json.RawMessage)
	res.MasterKeys = make(map[string]fclient.CrossSigningKey)
	res.SelfSigningKeys = make(map[string]fclient.CrossSigningKey)
	res.UserSigningKeys = make(map[string]fclient.CrossSigningKey)
	res.Failures = make(map[string]interface{})

	// make a map from domain to device keys
	domainToDeviceKeys := make(map[string]map[string][]string)
	domainToCrossSigningKeys := make(map[string]map[string]struct{})
	for userID, deviceIDs := range req.UserToDevices {
		_, serverName, err := gomatrixserverlib.SplitID('@', userID)
		if err != nil {
			continue // ignore invalid users
		}
		domain := string(serverName)
		// query local devices
		if a.Config.Global.IsLocalServerName(serverName) {
			deviceKeys, err := a.KeyDatabase.DeviceKeysForUser(ctx, userID, deviceIDs, false)
			if err != nil {
				res.Error = &api.KeyError{
					Err: fmt.Sprintf("failed to query local device keys: %s", err),
				}
				return
			}

			// pull out display names after we have the keys so we handle wildcards correctly
			var dids []string
			for _, dk := range deviceKeys {
				dids = append(dids, dk.DeviceID)
			}
			var queryRes api.QueryDeviceInfosResponse
			err = a.QueryDeviceInfos(ctx, &api.QueryDeviceInfosRequest{
				DeviceIDs: dids,
			}, &queryRes)
			if err != nil {
				util.Log(ctx).Warn("Failed to QueryDeviceInfos for device IDs, display names will be missing")
			}

			if res.DeviceKeys[userID] == nil {
				res.DeviceKeys[userID] = make(map[string]json.RawMessage)
			}
			for _, dk := range deviceKeys {
				if len(dk.KeyJSON) == 0 {
					continue // don't include blank keys
				}
				// inject display name if known (either locally or remotely)
				displayName := dk.DisplayName
				if queryRes.DeviceInfo[dk.DeviceID].DisplayName != "" {
					displayName = queryRes.DeviceInfo[dk.DeviceID].DisplayName
				}
				dk.KeyJSON, _ = sjson.SetBytes(dk.KeyJSON, "unsigned", struct {
					DisplayName string `json:"device_display_name,omitempty"`
				}{displayName})
				res.DeviceKeys[userID][dk.DeviceID] = dk.KeyJSON
			}
		} else {
			domainToDeviceKeys[domain] = make(map[string][]string)
			domainToDeviceKeys[domain][userID] = append(domainToDeviceKeys[domain][userID], deviceIDs...)
		}
		// work out if our cross-signing request for this user was
		// satisfied, if not add them to the list of things to fetch
		if _, ok := res.MasterKeys[userID]; !ok {
			if _, ok := domainToCrossSigningKeys[domain]; !ok {
				domainToCrossSigningKeys[domain] = make(map[string]struct{})
			}
			domainToCrossSigningKeys[domain][userID] = struct{}{}
		}
		if _, ok := res.SelfSigningKeys[userID]; !ok {
			if _, ok := domainToCrossSigningKeys[domain]; !ok {
				domainToCrossSigningKeys[domain] = make(map[string]struct{})
			}
			domainToCrossSigningKeys[domain][userID] = struct{}{}
		}
	}

	// attempt to satisfy key queries from the local database first as we should get device updates pushed to us
	domainToDeviceKeys = a.remoteKeysFromDatabase(ctx, res, &respMu, domainToDeviceKeys)
	if len(domainToDeviceKeys) > 0 || len(domainToCrossSigningKeys) > 0 {
		// perform key queries for remote devices
		a.queryRemoteKeys(ctx, req.Timeout, res, domainToDeviceKeys, domainToCrossSigningKeys)
	}

	// Now that we've done the potentially expensive work of asking the federation,
	// try filling the cross-signing keys from the database that we know about.
	a.crossSigningKeysFromDatabase(ctx, req, res)

	// Finally, append signatures that we know about
	// TODO: This is horrible because we need to round-trip the signature from
	// JSON, add the signatures and marshal it again, for some reason?

	for targetUserID, masterKey := range res.MasterKeys {
		if masterKey.Signatures == nil {
			masterKey.Signatures = map[string]map[gomatrixserverlib.KeyID]spec.Base64Bytes{}
		}
		for targetKeyID := range masterKey.Keys {
			sigMap, err := a.KeyDatabase.CrossSigningSigsForTarget(ctx, req.UserID, targetUserID, targetKeyID)
			if err != nil {
				// Stop executing the function if the context was canceled/the deadline was exceeded,
				// as we can't continue without a valid context.
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				util.Log(ctx).WithError(err).Error("a.KeyDatabase.CrossSigningSigsForTarget failed")
				continue
			}
			if len(sigMap) == 0 {
				continue
			}
			for sourceUserID, forSourceUser := range sigMap {
				for sourceKeyID, sourceSig := range forSourceUser {
					if _, ok := masterKey.Signatures[sourceUserID]; !ok {
						masterKey.Signatures[sourceUserID] = map[gomatrixserverlib.KeyID]spec.Base64Bytes{}
					}
					masterKey.Signatures[sourceUserID][sourceKeyID] = sourceSig
				}
			}
		}
	}

	for targetUserID, forUserID := range res.DeviceKeys {
		for targetKeyID, key := range forUserID {
			sigMap, err := a.KeyDatabase.CrossSigningSigsForTarget(ctx, req.UserID, targetUserID, gomatrixserverlib.KeyID(targetKeyID))
			if err != nil {
				// Stop executing the function if the context was canceled/the deadline was exceeded,
				// as we can't continue without a valid context.
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				util.Log(ctx).WithError(err).Error("a.KeyDatabase.CrossSigningSigsForTarget failed")
				continue
			}
			if len(sigMap) == 0 {
				continue
			}
			var deviceKey fclient.DeviceKeys
			if err = json.Unmarshal(key, &deviceKey); err != nil {
				continue
			}
			if deviceKey.Signatures == nil {
				deviceKey.Signatures = map[string]map[gomatrixserverlib.KeyID]spec.Base64Bytes{}
			}
			for sourceUserID, forSourceUser := range sigMap {
				for sourceKeyID, sourceSig := range forSourceUser {
					if _, ok := deviceKey.Signatures[sourceUserID]; !ok {
						deviceKey.Signatures[sourceUserID] = map[gomatrixserverlib.KeyID]spec.Base64Bytes{}
					}
					deviceKey.Signatures[sourceUserID][sourceKeyID] = sourceSig
				}
			}
			if js, err := json.Marshal(deviceKey); err == nil {
				res.DeviceKeys[targetUserID][targetKeyID] = js
			}
		}
	}
}

func (a *UserInternalAPI) remoteKeysFromDatabase(
	ctx context.Context, res *api.QueryKeysResponse, respMu *sync.Mutex, domainToDeviceKeys map[string]map[string][]string,
) map[string]map[string][]string {
	fetchRemote := make(map[string]map[string][]string)
	for domain, userToDeviceMap := range domainToDeviceKeys {
		for userID, deviceIDs := range userToDeviceMap {
			// we can't safely return keys from the db when all devices are requested as we don't
			// know if one has just been added.
			if len(deviceIDs) > 0 {
				err := a.populateResponseWithDeviceKeysFromDatabase(ctx, res, respMu, userID, deviceIDs)
				if err == nil {
					continue
				}
				util.Log(ctx).WithError(err).Error("populateResponseWithDeviceKeysFromDatabase")
			}
			// fetch device lists from remote
			if _, ok := fetchRemote[domain]; !ok {
				fetchRemote[domain] = make(map[string][]string)
			}
			fetchRemote[domain][userID] = append(fetchRemote[domain][userID], deviceIDs...)

		}
	}
	return fetchRemote
}

func (a *UserInternalAPI) queryRemoteKeys(
	ctx context.Context, timeout time.Duration, res *api.QueryKeysResponse,
	domainToDeviceKeys map[string]map[string][]string, domainToCrossSigningKeys map[string]map[string]struct{},
) {
	resultCh := make(chan *fclient.RespQueryKeys, len(domainToDeviceKeys))
	// allows us to wait until all federation servers have been poked
	var wg sync.WaitGroup
	// mutex for writing directly to res (e.g failures)
	var respMu sync.Mutex

	domains := map[string]struct{}{}
	for domain := range domainToDeviceKeys {
		if a.Config.Global.IsLocalServerName(spec.ServerName(domain)) {
			continue
		}
		domains[domain] = struct{}{}
	}
	for domain := range domainToCrossSigningKeys {
		if a.Config.Global.IsLocalServerName(spec.ServerName(domain)) {
			continue
		}
		domains[domain] = struct{}{}
	}
	wg.Add(len(domains))

	// fan out
	for domain := range domains {
		go a.queryRemoteKeysOnServer(
			ctx, domain, domainToDeviceKeys[domain], domainToCrossSigningKeys[domain],
			&wg, &respMu, timeout, resultCh, res,
		)
	}

	// Close the result channel when the goroutines have quit so the for .. range exits
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	processResult := func(result *fclient.RespQueryKeys) {
		respMu.Lock()
		defer respMu.Unlock()
		for userID, nest := range result.DeviceKeys {
			res.DeviceKeys[userID] = make(map[string]json.RawMessage)
			for deviceID, deviceKey := range nest {
				keyJSON, err := json.Marshal(deviceKey)
				if err != nil {
					continue
				}
				res.DeviceKeys[userID][deviceID] = keyJSON
			}
		}

		for userID, body := range result.MasterKeys {
			res.MasterKeys[userID] = body
		}

		for userID, body := range result.SelfSigningKeys {
			res.SelfSigningKeys[userID] = body
		}

		// TODO: do we want to persist these somewhere now
		// that we have fetched them?
	}

	for result := range resultCh {
		processResult(result)
	}
}

func (a *UserInternalAPI) queryRemoteKeysOnServer(
	ctx context.Context, serverName string, devKeys map[string][]string, crossSigningKeys map[string]struct{},
	wg *sync.WaitGroup, respMu *sync.Mutex, timeout time.Duration, resultCh chan<- *fclient.RespQueryKeys,
	res *api.QueryKeysResponse,
) {
	defer wg.Done()
	fedCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		fedCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	// for users who we do not have any knowledge about, try to start doing device list updates for them
	// by hitting /users/devices - otherwise fallback to /keys/query which has nicer bulk properties but
	// lack a stream ID.
	userIDsForAllDevices := map[string]struct{}{}
	for userID, deviceIDs := range devKeys {
		if len(deviceIDs) == 0 {
			userIDsForAllDevices[userID] = struct{}{}
		}
	}
	// for cross-signing keys, it's probably easier just to hit /keys/query if we aren't already doing
	// a device list update, so we'll populate those back into the /keys/query list if not
	for userID := range crossSigningKeys {
		if devKeys == nil {
			devKeys = map[string][]string{}
		}
		if _, ok := userIDsForAllDevices[userID]; !ok {
			devKeys[userID] = []string{}
		}
	}
	for userID := range userIDsForAllDevices {
		err := a.Updater.ManualUpdate(ctx, spec.ServerName(serverName), userID)
		if err != nil {
			util.Log(ctx).
				WithError(err).
				WithField("user_id", userID).
				WithField("server", serverName).
				Error("Failed to manually update device lists for user")
			// try to do it via /keys/query
			devKeys[userID] = []string{}
			continue
		}
		// refresh entries from Cm: unlike remoteKeysFromDatabase we know we previously had no device info for this
		// user so the fact that we're populating all devices here isn't a problem so long as we have devices.
		err = a.populateResponseWithDeviceKeysFromDatabase(ctx, res, respMu, userID, nil)
		if err != nil {
			util.Log(ctx).
				WithError(err).
				WithField("user_id", userID).
				WithField("server", serverName).
				Error("Failed to manually update device lists for user")
			// try to do it via /keys/query
			devKeys[userID] = []string{}
			continue
		}
	}
	if len(devKeys) == 0 {
		return
	}
	queryKeysResp, err := a.FedClient.QueryKeys(fedCtx, a.Config.Global.ServerName, spec.ServerName(serverName), devKeys)
	if err == nil {
		resultCh <- &queryKeysResp
		return
	}
	respMu.Lock()
	res.Failures[serverName] = map[string]interface{}{
		"message": err.Error(),
	}
	respMu.Unlock()

	// last ditch, use the cache only. This is good for when clients hit /keys/query and the remote server
	// is down, better to return something than nothing at all. Clients can know about the failure by
	// inspecting the failures map though so they can know it's a cached response.
	for userID, dkeys := range devKeys {
		// drop the error as it's already a failure at this point
		_ = a.populateResponseWithDeviceKeysFromDatabase(ctx, res, respMu, userID, dkeys)
	}

	// Sytest expects no failures, if we still could retrieve keys, e.g. from local cache
	respMu.Lock()
	if len(res.DeviceKeys) > 0 {
		delete(res.Failures, serverName)
	}
	respMu.Unlock()
}

func (a *UserInternalAPI) populateResponseWithDeviceKeysFromDatabase(
	ctx context.Context, res *api.QueryKeysResponse, respMu *sync.Mutex, userID string, deviceIDs []string,
) error {
	keys, err := a.KeyDatabase.DeviceKeysForUser(ctx, userID, deviceIDs, false)
	// if we can't query the db or there are fewer keys than requested, fetch from remote.
	if err != nil {
		return fmt.Errorf("DeviceKeysForUser %s %v failed: %w", userID, deviceIDs, err)
	}
	if len(keys) < len(deviceIDs) {
		return fmt.Errorf("DeviceKeysForUser %s returned fewer devices than requested, falling back to remote", userID)
	}
	if len(deviceIDs) == 0 && len(keys) == 0 {
		return fmt.Errorf("DeviceKeysForUser %s returned no keys but wanted all keys, falling back to remote", userID)
	}
	respMu.Lock()
	if res.DeviceKeys[userID] == nil {
		res.DeviceKeys[userID] = make(map[string]json.RawMessage)
	}
	respMu.Unlock()

	for _, key := range keys {
		if len(key.KeyJSON) == 0 {
			continue // ignore deleted keys
		}
		// inject the display name
		key.KeyJSON, _ = sjson.SetBytes(key.KeyJSON, "unsigned", struct {
			DisplayName string `json:"device_display_name,omitempty"`
		}{key.DisplayName})
		respMu.Lock()
		res.DeviceKeys[userID][key.DeviceID] = key.KeyJSON
		respMu.Unlock()
	}
	return nil
}

func (a *UserInternalAPI) uploadLocalDeviceKeys(ctx context.Context, req *api.PerformUploadKeysRequest, res *api.PerformUploadKeysResponse) {
	// get a list of devices from the user API that actually exist, as
	// we won't store keys for devices that don't exist
	uapidevices := &api.QueryDevicesResponse{}
	if err := a.QueryDevices(ctx, &api.QueryDevicesRequest{UserID: req.UserID}, uapidevices); err != nil {
		res.Error = &api.KeyError{
			Err: err.Error(),
		}
		return
	}
	if !uapidevices.UserExists {
		res.Error = &api.KeyError{
			Err: fmt.Sprintf("user %q does not exist", req.UserID),
		}
		return
	}
	existingDeviceMap := make(map[string]struct{}, len(uapidevices.Devices))
	for _, key := range uapidevices.Devices {
		existingDeviceMap[key.ID] = struct{}{}
	}

	// Get all of the user existing device keys so we can check for changes.
	existingKeys, err := a.KeyDatabase.DeviceKeysForUser(ctx, req.UserID, nil, true)
	if err != nil {
		res.Error = &api.KeyError{
			Err: fmt.Sprintf("failed to query existing device keys: %s", err.Error()),
		}
		return
	}

	// Work out whether we have device keys in the keyserver for devices that
	// no longer exist in the user API. This is mostly an exercise to ensure
	// that we keep some integrity between the two.
	var toClean []gomatrixserverlib.KeyID
	for _, k := range existingKeys {
		if _, ok := existingDeviceMap[k.DeviceID]; !ok {
			toClean = append(toClean, gomatrixserverlib.KeyID(k.DeviceID))
		}
	}

	if len(toClean) > 0 {
		if err = a.KeyDatabase.DeleteDeviceKeys(ctx, req.UserID, toClean); err != nil {
			util.Log(ctx).WithField("user_id", req.UserID).WithError(err).WithField("count", len(toClean)).Error("Failed to clean up stale keyserver device key entries")
		} else {
			util.Log(ctx).WithField("user_id", req.UserID).WithField("count", len(toClean)).Debug("Cleaned up stale keyserver device key entries")
		}
	}

	var keysToStore []api.DeviceMessage

	if req.OnlyDisplayNameUpdates {
		for _, existingKey := range existingKeys {
			for _, newKey := range req.DeviceKeys {
				switch {
				case existingKey.UserID != newKey.UserID:
					continue
				case existingKey.DeviceID != newKey.DeviceID:
					continue
				case existingKey.DisplayName != newKey.DisplayName:
					existingKey.DisplayName = newKey.DisplayName
				}
			}
			keysToStore = append(keysToStore, existingKey)
		}
	} else {
		// assert that the user ID / device ID are not lying for each key
		for _, key := range req.DeviceKeys {
			var serverName spec.ServerName
			_, serverName, err = gomatrixserverlib.SplitID('@', key.UserID)
			if err != nil {
				continue // ignore invalid users
			}
			if !a.Config.Global.IsLocalServerName(serverName) {
				continue // ignore remote users
			}
			if len(key.KeyJSON) == 0 {
				keysToStore = append(keysToStore, key.WithStreamID(0))
				continue // deleted keys don't need sanity checking
			}
			// check that the device in question actually exists in the user
			// API before we try and store a key for it
			if _, ok := existingDeviceMap[key.DeviceID]; !ok {
				continue
			}
			gotUserID := gjson.GetBytes(key.KeyJSON, "user_id").Str
			gotDeviceID := gjson.GetBytes(key.KeyJSON, "device_id").Str
			if gotUserID == key.UserID && gotDeviceID == key.DeviceID {
				keysToStore = append(keysToStore, key.WithStreamID(0))
				continue
			}

			res.KeyError(key.UserID, key.DeviceID, &api.KeyError{
				Err: fmt.Sprintf(
					"user_id or device_id mismatch: users: %s - %s, devices: %s - %s",
					gotUserID, key.UserID, gotDeviceID, key.DeviceID,
				),
			})
		}
	}

	// store the device keys and emit changes
	err = a.KeyDatabase.StoreLocalDeviceKeys(ctx, keysToStore)
	if err != nil {
		res.Error = &api.KeyError{
			Err: fmt.Sprintf("failed to store device keys: %s", err.Error()),
		}
		return
	}

	// If the request does _not_ come right after registering an account
	// inform downstream components. However, we're fine with just creating the
	// database entries above in other cases.
	if !req.FromRegistration {
		err = emitDeviceKeyChanges(ctx, a.KeyChangeProducer, existingKeys, keysToStore, req.OnlyDisplayNameUpdates)
		if err != nil {
			util.Log(ctx).WithError(err).Error("Failed to emitDeviceKeyChanges")
		}
	}
}

func (a *UserInternalAPI) uploadOneTimeKeys(ctx context.Context, req *api.PerformUploadKeysRequest, res *api.PerformUploadKeysResponse) {
	if req.UserID == "" {
		res.Error = &api.KeyError{
			Err: "user ID  missing",
		}
	}
	if req.DeviceID != "" && len(req.OneTimeKeys) == 0 {
		counts, err := a.KeyDatabase.OneTimeKeysCount(ctx, req.UserID, req.DeviceID)
		if err != nil {
			res.Error = &api.KeyError{
				Err: fmt.Sprintf("a.KeyDatabase.OneTimeKeysCount: %s", err),
			}
		}
		if counts != nil {
			res.OneTimeKeyCounts = append(res.OneTimeKeyCounts, *counts)
		}
		return
	}
	for _, key := range req.OneTimeKeys {
		// grab existing keys based on (user/device/algorithm/key ID)
		keyIDsWithAlgorithms := make([]string, len(key.KeyJSON))
		i := 0
		for keyIDWithAlgo := range key.KeyJSON {
			keyIDsWithAlgorithms[i] = keyIDWithAlgo
			i++
		}
		existingKeys, err := a.KeyDatabase.ExistingOneTimeKeys(ctx, req.UserID, req.DeviceID, keyIDsWithAlgorithms)
		if err != nil {
			res.KeyError(req.UserID, req.DeviceID, &api.KeyError{
				Err: "failed to query existing one-time keys: " + err.Error(),
			})
			continue
		}
		for keyIDWithAlgo := range existingKeys {
			// if keys exist and the JSON doesn't match, error out as the key already exists
			if !bytes.Equal(existingKeys[keyIDWithAlgo], key.KeyJSON[keyIDWithAlgo]) {
				res.KeyError(req.UserID, req.DeviceID, &api.KeyError{
					Err: fmt.Sprintf("%s device %s: algorithm / key ID %s one-time key already exists", req.UserID, req.DeviceID, keyIDWithAlgo),
				})
				continue
			}
		}
		// store one-time keys
		counts, err := a.KeyDatabase.StoreOneTimeKeys(ctx, key)
		if err != nil {
			res.KeyError(req.UserID, req.DeviceID, &api.KeyError{
				Err: fmt.Sprintf("%s device %s : failed to store one-time keys: %s", req.UserID, req.DeviceID, err.Error()),
			})
			continue
		}
		// collect counts
		res.OneTimeKeyCounts = append(res.OneTimeKeyCounts, *counts)
	}

}

func emitDeviceKeyChanges(ctx context.Context, producer KeyChangeProducer, existing, new []api.DeviceMessage, onlyUpdateDisplayName bool) error {
	// if we only want to update the display names, we can skip the checks below
	if onlyUpdateDisplayName {
		return producer.ProduceKeyChanges(ctx, new)
	}
	// find keys in new that are not in existing
	var keysAdded []api.DeviceMessage
	for _, newKey := range new {
		exists := false
		for _, existingKey := range existing {
			// Do not treat the absence of keys as equal, or else we will not emit key changes
			// when users delete devices which never had a key to begin with as both KeyJSONs are nil.
			if existingKey.DeviceKeysEqual(&newKey) {
				exists = true
				break
			}
		}
		if !exists {
			keysAdded = append(keysAdded, newKey)
		}
	}
	return producer.ProduceKeyChanges(ctx, keysAdded)
}
