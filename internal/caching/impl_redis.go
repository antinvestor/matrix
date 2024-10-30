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
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// Define the RedisCachePartition type to replace Ristretto
type RedisCachePartition[K comparable, V any] struct {
	client  *redis.Client
	Prefix  byte
	Mutable bool
	MaxAge  time.Duration
}

// Main Redis-based caching setup
func NewRedisCache(redisAddr string, maxAge time.Duration) *Caches {
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	return &Caches{
		RoomVersions:            &RedisCachePartition[string, gomatrixserverlib.RoomVersion]{client, roomVersionsCache, false, maxAge},
		ServerKeys:              &RedisCachePartition[string, gomatrixserverlib.PublicKeyLookupResult]{client, serverKeysCache, true, maxAge},
		RoomServerRoomNIDs:      &RedisCachePartition[string, types.RoomNID]{client, roomNIDsCache, false, maxAge},
		RoomServerRoomIDs:       &RedisCachePartition[types.RoomNID, string]{client, roomIDsCache, false, maxAge},
		RoomServerEvents:        &RedisCachePartition[int64, *types.HeaderedEvent]{client, roomEventsCache, true, maxAge},
		RoomServerStateKeys:     &RedisCachePartition[types.EventStateKeyNID, string]{client, eventStateKeyCache, false, maxAge},
		RoomServerStateKeyNIDs:  &RedisCachePartition[string, types.EventStateKeyNID]{client, eventStateKeyNIDCache, false, maxAge},
		RoomServerEventTypeNIDs: &RedisCachePartition[string, types.EventTypeNID]{client, eventTypeCache, false, maxAge},
		RoomServerEventTypes:    &RedisCachePartition[types.EventTypeNID, string]{client, eventTypeNIDCache, false, maxAge},
		FederationPDUs:          &RedisCachePartition[int64, *types.HeaderedEvent]{client, federationPDUsCache, true, lesserOf(time.Hour/2, maxAge)},
		FederationEDUs:          &RedisCachePartition[int64, *gomatrixserverlib.EDU]{client, federationEDUsCache, true, lesserOf(time.Hour/2, maxAge)},
		RoomHierarchies:         &RedisCachePartition[string, fclient.RoomHierarchyResponse]{client, spaceSummaryRoomsCache, true, maxAge},
		LazyLoading:             &RedisCachePartition[lazyLoadingCacheKey, string]{client, lazyLoadingCache, true, maxAge},
	}
}

// Set value in Redis with JSON serialization
func (c *RedisCachePartition[K, V]) Set(ctx context.Context, key K, value V) error {
	bkey := fmt.Sprintf("%c%v", c.Prefix, key)
	val, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}
	return c.client.Set(ctx, bkey, val, c.MaxAge).Err()
}

// Get value from Redis with JSON deserialization
func (c *RedisCachePartition[K, V]) Get(ctx context.Context, key K) (V, bool) {
	bkey := fmt.Sprintf("%c%v", c.Prefix, key)
	result, err := c.client.Get(ctx, bkey).Result()
	if errors.Is(err, redis.Nil) {
		var empty V
		return empty, false
	} else if err != nil {
		panic(err) // handle as needed
	}
	var value V
	if err := json.Unmarshal([]byte(result), &value); err != nil {
		panic(fmt.Sprintf("failed to unmarshal value: %v", err))
	}
	return value, true
}

// Delete key from Redis
func (c *RedisCachePartition[K, V]) Unset(ctx context.Context, key K) error {
	bkey := fmt.Sprintf("%c%v", c.Prefix, key)
	return c.client.Del(ctx, bkey).Err()
}
