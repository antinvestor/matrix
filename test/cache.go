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

package test

import (
	"context"
	"github.com/redis/go-redis/v9"
	"os"
)

func clearCache(_ context.Context, redisUriStr string) error {

	_, err := redis.ParseURL(redisUriStr)
	if err != nil {
		return err
	}

	//client := redis.NewClient(opts)
	//return client.FlushDB(ctx).Err()
	return nil
}

// PrepareRedisConnectionString Prepare a redis connection string for testing.
// Returns the connection string to use and a close function which must be called when the test finishes.
// Calling this function twice will return the same database, which will have data from previous tests
// unless close() is called.
func PrepareRedisConnectionString(ctx context.Context) (connStr string, close func(), err error) {

	redisUriStr := os.Getenv("REDIS_URI")
	if redisUriStr == "" {
		redisUriStr = "redis://localhost:6379/0?protocol=3"
	}

	return redisUriStr, func() {
		_ = clearCache(ctx, redisUriStr)
	}, nil
}
