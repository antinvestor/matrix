package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/pitabwire/frame"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/api"
)

// QueryJoinedHostServerNamesInRoom implements api.FederationInternalAPI
func (r *FederationInternalAPI) QueryJoinedHostServerNamesInRoom(
	ctx context.Context,
	request *api.QueryJoinedHostServerNamesInRoomRequest,
	response *api.QueryJoinedHostServerNamesInRoomResponse,
) (err error) {
	joinedHosts, err := r.db.GetJoinedHostsForRooms(ctx, []string{request.RoomID}, request.ExcludeSelf, request.ExcludeBlacklisted)
	if err != nil {
		return
	}
	response.ServerNames = joinedHosts

	return
}

func (r *FederationInternalAPI) fetchServerKeysDirectly(ctx context.Context, serverName spec.ServerName) (*gomatrixserverlib.ServerKeys, error) {
	iCtx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	ires, err := r.doRequestIfNotBackingOffOrBlacklisted(iCtx, serverName, func() (interface{}, error) {
		return r.federation.GetServerKeys(iCtx, serverName)
	})
	if err != nil {
		return nil, err
	}
	sks := ires.(gomatrixserverlib.ServerKeys)
	return &sks, nil
}

func (r *FederationInternalAPI) fetchServerKeysFromCache(
	ctx context.Context, req *api.QueryServerKeysRequest,
) ([]gomatrixserverlib.ServerKeys, error) {
	var results []gomatrixserverlib.ServerKeys

	// We got a request for _all_ server keys, return them.
	if len(req.KeyIDToCriteria) == 0 {
		serverKeysResponses, _ := r.db.GetNotaryKeys(ctx, req.ServerName, []gomatrixserverlib.KeyID{})
		if len(serverKeysResponses) == 0 {
			return nil, fmt.Errorf("failed to find server key response for server %s", req.ServerName)
		}
		return serverKeysResponses, nil
	}
	for keyID, criteria := range req.KeyIDToCriteria {
		serverKeysResponses, _ := r.db.GetNotaryKeys(ctx, req.ServerName, []gomatrixserverlib.KeyID{keyID})
		if len(serverKeysResponses) == 0 {
			return nil, fmt.Errorf("failed to find server key response for key ID %s", keyID)
		}
		// we should only get 1 result as we only gave 1 key ID
		sk := serverKeysResponses[0]
		frame.Log(ctx).WithField("key_id", keyID).WithField("minimum_valid_until_ts", criteria.MinimumValidUntilTS).WithField("server_keys_valid_until_ts", sk.ValidUntilTS).Info("fetched server keys from cache")
		if criteria.MinimumValidUntilTS != 0 {
			// check if it's still valid. if they have the same value that's also valid
			if sk.ValidUntilTS < criteria.MinimumValidUntilTS {
				return nil, fmt.Errorf(
					"found server response for key ID %s but it is no longer valid, min: %v valid_until: %v",
					keyID, criteria.MinimumValidUntilTS, sk.ValidUntilTS,
				)
			}
		}
		results = append(results, sk)
	}
	return results, nil
}

func (r *FederationInternalAPI) QueryServerKeys(
	ctx context.Context, req *api.QueryServerKeysRequest, res *api.QueryServerKeysResponse,
) error {
	// attempt to satisfy the entire request from the cache first
	results, err := r.fetchServerKeysFromCache(ctx, req)
	if err == nil {
		// satisfied entirely from cache, return it
		res.ServerKeys = results
		return nil
	}
	frame.Log(ctx).WithField("server", req.ServerName).WithError(err).Warn("notary: failed to satisfy keys request entirely from cache, hitting direct")

	serverKeys, err := r.fetchServerKeysDirectly(ctx, req.ServerName)
	if err != nil {
		// try to load as much as we can from the cache in a best effort basis
		frame.Log(ctx).WithField("server", req.ServerName).WithError(err).Warn("notary: failed to ask server for keys, returning best effort keys")
		serverKeysResponses, dbErr := r.db.GetNotaryKeys(ctx, req.ServerName, req.KeyIDs())
		if dbErr != nil {
			return fmt.Errorf("notary: server returned %s, and db returned %s", err, dbErr)
		}
		res.ServerKeys = serverKeysResponses
		return nil
	}
	// cache it!
	if err = r.db.UpdateNotaryKeys(ctx, req.ServerName, *serverKeys); err != nil {
		// non-fatal, still return the response
		frame.Log(ctx).WithError(err).Warn("failed to UpdateNotaryKeys")
	}
	res.ServerKeys = []gomatrixserverlib.ServerKeys{*serverKeys}
	return nil
}
