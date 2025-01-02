package caching

import (
	"context"
	"fmt"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
)

// ServerKeyCache contains the subset of functions needed for
// a server key cache.
type ServerKeyCache interface {
	// request -> timestamp is emulating gomatrixserverlib.FetchKeys:
	// https://github.com/antinvestor/gomatrixserverlib/blob/f69539c86ea55d1e2cc76fd8e944e2d82d30397c/keyring.go#L95
	// The timestamp should be the timestamp of the event that is being
	// verified. We will not return keys from the cache that are not valid
	// at this timestamp.
	GetServerKey(ctx context.Context, request gomatrixserverlib.PublicKeyLookupRequest, timestamp spec.Timestamp) (response gomatrixserverlib.PublicKeyLookupResult, ok bool)

	// request -> result is emulating gomatrixserverlib.StoreKeys:
	// https://github.com/antinvestor/gomatrixserverlib/blob/f69539c86ea55d1e2cc76fd8e944e2d82d30397c/keyring.go#L112
	StoreServerKey(ctx context.Context, request gomatrixserverlib.PublicKeyLookupRequest, response gomatrixserverlib.PublicKeyLookupResult) error
}

func (c Caches) GetServerKey(
	ctx context.Context,
	request gomatrixserverlib.PublicKeyLookupRequest,
	timestamp spec.Timestamp,
) (gomatrixserverlib.PublicKeyLookupResult, bool) {
	key := fmt.Sprintf("%s/%s", request.ServerName, request.KeyID)
	val, found := c.ServerKeys.Get(ctx, key)
	if found && !val.WasValidAt(timestamp, gomatrixserverlib.StrictValiditySignatureCheck) {
		// The key wasn't valid at the requested timestamp so don't
		// return it. The caller will have to work out what to do.
		_ = c.ServerKeys.Unset(ctx, key)
		return gomatrixserverlib.PublicKeyLookupResult{}, false
	}
	return val, found
}

func (c Caches) StoreServerKey(
	ctx context.Context,
	request gomatrixserverlib.PublicKeyLookupRequest,
	response gomatrixserverlib.PublicKeyLookupResult,
) error {
	key := fmt.Sprintf("%s/%s", request.ServerName, request.KeyID)
	return c.ServerKeys.Set(ctx, key, response)
}
