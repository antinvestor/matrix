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

package cacheutil

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/pitabwire/util"
	"github.com/valkey-io/valkey-go"
)

// ValkeyCachePartition Define the ValkeyCachePartition type to replace Ristretto
type ValkeyCachePartition[K comparable, V any] struct {
	client  valkey.Client
	Prefix  byte
	Mutable bool
	MaxAge  time.Duration
}

// NewValkeyCache Main Redis-based caching setup
func NewValkeyCache(redisAddr string, maxAge time.Duration) (*Caches, error) {

	opts, err := valkey.ParseURL(redisAddr)
	if err != nil {
		return nil, err
	}

	client, err := valkey.NewClient(opts)
	if err != nil {
		return nil, err
	}

	return &Caches{
		RoomVersions:            &ValkeyCachePartition[string, gomatrixserverlib.RoomVersion]{client, roomVersionsCache, false, maxAge},
		ServerKeys:              &ValkeyCachePartition[string, gomatrixserverlib.PublicKeyLookupResult]{client, serverKeysCache, true, maxAge},
		RoomServerRoomLocks:     &ValkeyCachePartition[string, int64]{client, roomLocksCache, false, maxAge},
		RoomServerRoomNIDs:      &ValkeyCachePartition[string, types.RoomNID]{client, roomNIDsCache, false, maxAge},
		RoomServerRoomIDs:       &ValkeyCachePartition[types.RoomNID, string]{client, roomIDsCache, false, maxAge},
		RoomServerEvents:        &ValkeyCachePartition[int64, *types.HeaderedEvent]{client, roomEventsCache, true, maxAge},
		RoomServerStateKeys:     &ValkeyCachePartition[types.EventStateKeyNID, string]{client, eventStateKeyCache, false, maxAge},
		RoomServerStateKeyNIDs:  &ValkeyCachePartition[string, types.EventStateKeyNID]{client, eventStateKeyNIDCache, false, maxAge},
		RoomServerEventTypeNIDs: &ValkeyCachePartition[string, types.EventTypeNID]{client, eventTypeCache, false, maxAge},
		RoomServerEventTypes:    &ValkeyCachePartition[types.EventTypeNID, string]{client, eventTypeNIDCache, false, maxAge},
		FederationPDUs:          &ValkeyCachePartition[int64, *types.HeaderedEvent]{client, federationPDUsCache, true, maxAgeOfHalfHour(maxAge)},
		FederationEDUs:          &ValkeyCachePartition[int64, *gomatrixserverlib.EDU]{client, federationEDUsCache, true, maxAgeOfHalfHour(maxAge)},
		RoomHierarchies:         &ValkeyCachePartition[string, fclient.RoomHierarchyResponse]{client, spaceSummaryRoomsCache, true, maxAge},
		LazyLoading:             &ValkeyCachePartition[lazyLoadingCacheKey, string]{client, lazyLoadingCache, true, maxAge},
	}, nil
}

func (c *ValkeyCachePartition[K, V]) getPrefixedKey(key K) string {
	return fmt.Sprintf("%c%v", c.Prefix, key)
}

// Set value in Redis with JSON serialisation
func (c *ValkeyCachePartition[K, V]) Set(ctx context.Context, key K, value V) error {
	bkey := c.getPrefixedKey(key)
	val, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	cmd := c.client.B().Set().Key(bkey).Value(string(val)).Ex(c.MaxAge).Build()
	return c.client.Do(ctx, cmd).Error()
}

// Get value from Redis with JSON deserialisation
func (c *ValkeyCachePartition[K, V]) Get(ctx context.Context, key K) (V, bool) {
	bkey := c.getPrefixedKey(key)

	cmd := c.client.B().Get().Key(bkey).Build()
	result, err := c.client.Do(ctx, cmd).ToString()
	if err != nil {

		if !valkey.IsValkeyNil(err) {
			util.Log(ctx).WithError(err).Error("Failed to get value")
		}

		var empty V
		return empty, false
	}
	var value V
	err = json.Unmarshal([]byte(result), &value)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal value: %v", err))
	}
	return value, true
}

// Unset key from Redis
func (c *ValkeyCachePartition[K, V]) Unset(ctx context.Context, key K) error {
	bkey := c.getPrefixedKey(key)

	cmd := c.client.B().Del().Key(bkey).Build()
	return c.client.Do(ctx, cmd).Error()
}

func (c *ValkeyCachePartition[K, V]) TryLock(ctx context.Context, key K, ttl time.Duration) (bool, error) {
	bkey := c.getPrefixedKey(key)

	cmd := c.client.B().Set().Key(bkey).Value(time.Now().String()).Nx().Ex(ttl).Build()
	return c.client.Do(ctx, cmd).ToBool()
}

func (c *ValkeyCachePartition[K, V]) ExtendLock(ctx context.Context, key K, ttl time.Duration) (bool, error) {
	bkey := c.getPrefixedKey(key)

	cmd := c.client.B().Expire().Key(bkey).Seconds(int64(ttl.Seconds())).Build()
	return c.client.Do(ctx, cmd).ToBool()
}
func (c *ValkeyCachePartition[K, V]) Unlock(ctx context.Context, key K) error {
	return c.Unset(ctx, key)
}
