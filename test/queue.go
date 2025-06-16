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

package test

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/antinvestor/matrix/setup/config"
)

// const NatsImage = "nats:2.10"
//
// func setupNats(ctx context.Context) (*tcNats.NATSContainer, error) {
//	return tcNats.Run(ctx, NatsImage)
// }

// PrepareQueueConnection Prepare a nats connection string for testing.
// Returns the connection string to use and a close function which must be called when the test finishes.
// Calling this function twice will return the same connection, which will have data from previous tests
// unless close() is called.
// func PrepareQueueConnection(ctx context.Context) (dsConnection config.DataSource, close func(), err error) {
//
//	container, err := setupNats(ctx)
//	if err != nil {
//		return "", nil, err
//	}
//
//	connStr, err := container.DS(ctx)
//	if err != nil {
//		return "", nil, err
//	}
//
//	return config.DataSource(connStr), func() {
//
//		err = testcontainers.TerminateContainer(container)
//		if err != nil {
//			util.Log(ctx).WithError(err).Error("failed to terminate container")
//		}
//
//	}, nil
// }

// PrepareQueueConnection Prepare a nats connection string for testing.
// Returns the connection string to use and a close function which must be called when the test finishes.
// Calling this function twice will return the same database, which will have data from previous tests
// unless close() is called.
func PrepareQueueConnection(_ context.Context, randomnesPrefix string, testOpts DependancyOption) (connStr config.DataSource, close func(ctx context.Context), err error) {

	if testOpts.Queue() != DefaultQueue {
		return "", func(ctx context.Context) {}, fmt.Errorf(" %s is unsupported, only nats is the supported queue for now", testOpts.Queue())
	}

	natsUriStr := os.Getenv("TESTING_QUEUE_URI")
	if natsUriStr == "" {
		natsUriStr = "nats://localhost:4222"
	}

	parsedNatsUri, err := url.Parse(natsUriStr)
	if err != nil {
		return "", func(ctx context.Context) {}, err
	}

	natsUriStr = parsedNatsUri.String()

	return config.DataSource(natsUriStr), func(ctx context.Context) {
	}, nil
}
