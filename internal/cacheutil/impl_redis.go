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
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/redis/go-redis/v9"
)

// RedisCachePartition Define the RedisCachePartition type to replace Ristretto
type RedisCachePartition[K comparable, V any] struct {
	client  *redis.Client
	Prefix  byte
	Mutable bool
	MaxAge  time.Duration
}

// NewRedisCache Main Redis-based caching setup
func NewRedisCache(redisAddr string, maxAge time.Duration) (*Caches, error) {

	opts, err := redis.ParseURL(redisAddr)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opts)
	return &Caches{
		RoomVersions:            &RedisCachePartition[string, gomatrixserverlib.RoomVersion]{client, roomVersionsCache, false, maxAge},
		ServerKeys:              &RedisCachePartition[string, gomatrixserverlib.PublicKeyLookupResult]{client, serverKeysCache, true, maxAge},
		RoomServerRoomLocks:     &RedisCachePartition[string, int64]{client, roomLocksCache, false, maxAge},
		RoomServerRoomNIDs:      &RedisCachePartition[string, types.RoomNID]{client, roomNIDsCache, false, maxAge},
		RoomServerRoomIDs:       &RedisCachePartition[types.RoomNID, string]{client, roomIDsCache, false, maxAge},
		RoomServerEvents:        &RedisCachePartition[int64, *types.HeaderedEvent]{client, roomEventsCache, true, maxAge},
		RoomServerStateKeys:     &RedisCachePartition[types.EventStateKeyNID, string]{client, eventStateKeyCache, false, maxAge},
		RoomServerStateKeyNIDs:  &RedisCachePartition[string, types.EventStateKeyNID]{client, eventStateKeyNIDCache, false, maxAge},
		RoomServerEventTypeNIDs: &RedisCachePartition[string, types.EventTypeNID]{client, eventTypeCache, false, maxAge},
		RoomServerEventTypes:    &RedisCachePartition[types.EventTypeNID, string]{client, eventTypeNIDCache, false, maxAge},
		FederationPDUs:          &RedisCachePartition[int64, *types.HeaderedEvent]{client, federationPDUsCache, true, maxAgeOfHalfHour(maxAge)},
		FederationEDUs:          &RedisCachePartition[int64, *gomatrixserverlib.EDU]{client, federationEDUsCache, true, maxAgeOfHalfHour(maxAge)},
		RoomHierarchies:         &RedisCachePartition[string, fclient.RoomHierarchyResponse]{client, spaceSummaryRoomsCache, true, maxAge},
		LazyLoading:             &RedisCachePartition[lazyLoadingCacheKey, string]{client, lazyLoadingCache, true, maxAge},
	}, nil
}

func (c *RedisCachePartition[K, V]) getPrefixedKey(key K) string {
	return fmt.Sprintf("%c%v", c.Prefix, key)
}

// Set value in Redis with JSON serialisation
func (c *RedisCachePartition[K, V]) Set(ctx context.Context, key K, value V) error {
	bkey := c.getPrefixedKey(key)
	val, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}
	return c.client.Set(ctx, bkey, val, c.MaxAge).Err()
}

// Get value from Redis with JSON deserialisation
func (c *RedisCachePartition[K, V]) Get(ctx context.Context, key K) (V, bool) {
	bkey := c.getPrefixedKey(key)
	result, err := c.client.Get(ctx, bkey).Result()
	if err != nil {

		if !errors.Is(err, redis.Nil) {
			logrus.WithError(err).Error("Failed to get value")
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
func (c *RedisCachePartition[K, V]) Unset(ctx context.Context, key K) error {
	bkey := c.getPrefixedKey(key)
	return c.client.Del(ctx, bkey).Err()
}

func (c *RedisCachePartition[K, V]) TryLock(ctx context.Context, key K, ttl time.Duration) (bool, error) {
	bkey := c.getPrefixedKey(key)
	return c.client.SetNX(ctx, bkey, time.Now().Unix(), ttl).Result()
}
func (c *RedisCachePartition[K, V]) ExtendLock(ctx context.Context, key K, ttl time.Duration) (bool, error) {
	bkey := c.getPrefixedKey(key)
	return c.client.Expire(ctx, bkey, ttl).Result()
}
func (c *RedisCachePartition[K, V]) Unlock(ctx context.Context, key K) error {
	bkey := c.getPrefixedKey(key)
	_, err := c.client.Del(ctx, bkey).Result()
	if err != nil {
		return err
	}
	return nil
}
