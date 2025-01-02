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

package caching

import (
	"context"
	"errors"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
)

const (
	roomVersionsCache byte = iota + 1
	serverKeysCache
	roomNIDsCache
	roomIDsCache
	roomEventsCache
	federationPDUsCache
	federationEDUsCache
	spaceSummaryRoomsCache
	lazyLoadingCache
	eventStateKeyCache
	eventTypeCache
	eventTypeNIDCache
	eventStateKeyNIDCache
)

const (
	DisableMetrics = false
	EnableMetrics  = true
)

// Caches contains a set of references to caches. They may be
// different implementations as long as they satisfy the Cache
// interface.
type Caches struct {
	RoomVersions            Cache[string, gomatrixserverlib.RoomVersion]           // room ID -> room version
	ServerKeys              Cache[string, gomatrixserverlib.PublicKeyLookupResult] // server name -> server keys
	RoomServerRoomNIDs      Cache[string, types.RoomNID]                           // room ID -> room NID
	RoomServerRoomIDs       Cache[types.RoomNID, string]                           // room NID -> room ID
	RoomServerEvents        Cache[int64, *types.HeaderedEvent]                     // event NID -> event
	RoomServerStateKeys     Cache[types.EventStateKeyNID, string]                  // eventStateKey NID -> event state key
	RoomServerStateKeyNIDs  Cache[string, types.EventStateKeyNID]                  // event state key -> eventStateKey NID
	RoomServerEventTypeNIDs Cache[string, types.EventTypeNID]                      // eventType -> eventType NID
	RoomServerEventTypes    Cache[types.EventTypeNID, string]                      // eventType NID -> eventType
	FederationPDUs          Cache[int64, *types.HeaderedEvent]                     // queue NID -> PDU
	FederationEDUs          Cache[int64, *gomatrixserverlib.EDU]                   // queue NID -> EDU
	RoomHierarchies         Cache[string, fclient.RoomHierarchyResponse]           // room ID -> space response
	LazyLoading             Cache[lazyLoadingCacheKey, string]                     // composite key -> event ID
}

// Cache is the interface that an implementation must satisfy.
type Cache[K keyable, T any] interface {
	Get(ctx context.Context, key K) (value T, ok bool)
	Set(ctx context.Context, key K, value T) error
	Unset(ctx context.Context, key K) error
}

type keyable interface {
	// from https://github.com/dgraph-io/ristretto/blob/8e850b710d6df0383c375ec6a7beae4ce48fc8d5/z/z.go#L34
	~uint64 | ~string | []byte | byte | ~int | ~int32 | ~uint32 | ~int64 | lazyLoadingCacheKey
}

func maxAgeOfHalfHour(b time.Duration) time.Duration {
	a := time.Hour / 2
	if a < b {
		return a
	}
	return b
}

func NewCache(cfg *config.CacheOptions) (*Caches, error) {

	if cfg.ConnectionString == "" {
		return nil, errors.New("no url to cache specified")
	}

	if !cfg.ConnectionString.IsRedis() {
		return nil, errors.New("only redis is currently supported as cache")
	}

	return NewRedisCache(string(cfg.ConnectionString), cfg.MaxAge)
}
