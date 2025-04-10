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
	"net/url"
	"os"

	"github.com/antinvestor/matrix/setup/config"
)

//const NatsImage = "nats:2.10"
//
//func setupNats(ctx context.Context) (*tcNats.NATSContainer, error) {
//	return tcNats.Run(ctx, NatsImage)
//}

// PrepareNatsDataSourceConnection Prepare a nats connection string for testing.
// Returns the connection string to use and a close function which must be called when the test finishes.
// Calling this function twice will return the same connection, which will have data from previous tests
// unless close() is called.
//func PrepareNatsDataSourceConnection(ctx context.Context) (dsConnection config.DataSource, close func(), err error) {
//
//	container, err := setupNats(ctx)
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
//
//		err = testcontainers.TerminateContainer(container)
//		if err != nil {
//			logrus.WithError(err).Error("failed to terminate container")
//		}
//
//	}, nil
//}

// PrepareNatsDataSourceConnection Prepare a nats connection string for testing.
// Returns the connection string to use and a close function which must be called when the test finishes.
// Calling this function twice will return the same database, which will have data from previous tests
// unless close() is called.
func PrepareNatsDataSourceConnection(_ context.Context) (connStr config.DataSource, close func(), err error) {

	natsUriStr := os.Getenv("TESTING_QUEUE_URI")
	if natsUriStr == "" {
		natsUriStr = "nats://localhost:4222"
	}

	parsedNatsUri, err := url.Parse(natsUriStr)
	if err != nil {
		return "", func() {}, err
	}

	natsUriStr = parsedNatsUri.String()

	return config.DataSource(natsUriStr), func() {
	}, nil
}
