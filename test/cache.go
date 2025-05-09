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
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/redis/go-redis/v9"
)

//const RedisImage = "redis:7"
//
//func setupRedis(ctx context.Context) (*tcRedis.RedisContainer, error) {
//	return tcRedis.Run(ctx, RedisImage)
//}
//
//// testContainerRedisDataSource Prepare a redis connection string for testing.
//// Returns the connection string to use and a close function which must be called when the test finishes.
//// Calling this function twice will return the same database, which will have data from previous tests
//// unless close() is called.
//func testContainerRedisDataSource(ctx context.Context) (dsConnection config.DataSource, close func(), err error) {
//
//	container, err := setupRedis(ctx)
//	if err != nil {
//		return "", nil, err
//	}
//
//	connStr, err := container.ConnectionString(ctx)
//	if err != nil {
//		return "", nil, err
//	}
//
//	return config.DataSource(connStr), func() {
//		err = testcontainers.TerminateContainer(container)
//		if err != nil {
//			logrus.WithError(err).Error("failed to terminate container")
//		}
//	}, nil
//}

func clearCache(ctx context.Context, redisUriStr string) error {

	opts, err := redis.ParseURL(redisUriStr)
	if err != nil {
		return err
	}

	client := redis.NewClient(opts)
	return client.FlushDB(ctx).Err()
}

// PrepareCacheConnection Prepare a redis connection string for testing.
// Returns the connection string to use and a close function which must be called when the test finishes.
// Calling this function twice will return the same database, which will have data from previous tests
// unless close() is called.
func PrepareCacheConnection(ctx context.Context, testOpts DependancyOption) (connStr config.DataSource, close func(ctx context.Context), err error) {

	if testOpts.Cache() != DefaultCache {
		return "", func(ctx context.Context) {}, fmt.Errorf(" %s is unsupported, only redis is the usable cache", testOpts.Cache())
	}

	redisUriStr := os.Getenv("TESTING_CACHE_URI")
	if redisUriStr == "" {
		redisUriStr = "redis://matrix:s3cr3t@127.0.0.1:6379"
	}

	parsedUri, err := url.Parse(redisUriStr)
	if err != nil {
		return "", func(ctx context.Context) {}, err
	}

	newDb := rand.IntN(10000)

	parsedUri.Path = fmt.Sprintf("/%d", newDb)
	redisUriStr = parsedUri.String()

	return config.DataSource(redisUriStr), func(ctx context.Context) {
		_ = clearCache(ctx, redisUriStr)
	}, nil
}
