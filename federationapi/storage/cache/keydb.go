package cache

import (
	"context"
	"errors"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/cacheutil"
)

// A Database implements gomatrixserverlib.KeyDatabase and is used to store
// the public keys for other matrix servers.
type KeyDatabase struct {
	inner gomatrixserverlib.KeyDatabase
	cache cacheutil.ServerKeyCache
}

func NewKeyDatabase(inner gomatrixserverlib.KeyDatabase, cache cacheutil.ServerKeyCache) (*KeyDatabase, error) {
	if inner == nil {
		return nil, errors.New("inner database can't be nil")
	}
	if cache == nil {
		return nil, errors.New("cache can't be nil")
	}
	return &KeyDatabase{
		inner: inner,
		cache: cache,
	}, nil
}

// FetcherName implements KeyFetcher
func (d KeyDatabase) FetcherName() string {
	return "InMemoryKeyCache"
}

// FetchKeys implements gomatrixserverlib.KeyDatabase
func (d *KeyDatabase) FetchKeys(
	ctx context.Context,
	requests map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp,
) (map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult, error) {
	results := make(map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult)
	for req, ts := range requests {
		if res, cached := d.cache.GetServerKey(ctx, req, ts); cached {
			results[req] = res
			delete(requests, req)
		}
	}
	// Don't bother hitting the Cm if we got everything from cache.
	if len(requests) == 0 {
		return results, nil
	}
	fromDB, err := d.inner.FetchKeys(ctx, requests)
	if err != nil {
		return results, err
	}
	for req, res := range fromDB {
		results[req] = res
		_ = d.cache.StoreServerKey(ctx, req, res)
	}
	return results, nil
}

// StoreKeys implements gomatrixserverlib.KeyDatabase
func (d *KeyDatabase) StoreKeys(
	ctx context.Context,
	keyMap map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult,
) error {
	for req, res := range keyMap {
		_ = d.cache.StoreServerKey(ctx, req, res)
	}
	return d.inner.StoreKeys(ctx, keyMap)
}
