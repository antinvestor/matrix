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

package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/queue"
	"github.com/antinvestor/matrix/federationapi/statistics"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

const (
	FailuresUntilAssumedOffline = 3
	FailuresUntilBlacklist      = 8
)

func (t *testFedClient) QueryKeys(ctx context.Context, origin, s spec.ServerName, keys map[string][]string) (fclient.RespQueryKeys, error) {
	t.queryKeysCalled = true
	if t.shouldFail {
		return fclient.RespQueryKeys{}, fmt.Errorf("Failure")
	}
	return fclient.RespQueryKeys{}, nil
}

func (t *testFedClient) ClaimKeys(ctx context.Context, origin, s spec.ServerName, oneTimeKeys map[string]map[string]string) (fclient.RespClaimKeys, error) {
	t.claimKeysCalled = true
	if t.shouldFail {
		return fclient.RespClaimKeys{}, fmt.Errorf("Failure")
	}
	return fclient.RespClaimKeys{}, nil
}

func TestFederationClientQueryKeys(t *testing.T) {
	ctx, svc, cfg := testrig.Init(t, testOpts)
	defer svc.Stop(ctx)
	testDB := test.NewInMemoryFederationDatabase()

	cfg := config.FederationAPI{
		Global: &config.Global{
			SigningIdentity: fclient.SigningIdentity{
				ServerName: "server",
			},
		},
	}
	fedClient := &testFedClient{}
	stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
	queues := queue.NewOutgoingQueues(
		ctx, testDB,
		false,
		cfg.Global.ServerName, fedClient, &stats,
		nil,
	)
	fedapi := FederationInternalAPI{
		db:         testDB,
		cfg:        &cfg,
		statistics: &stats,
		federation: fedClient,
		queues:     queues,
	}
	_, err := fedapi.QueryKeys(ctx, "origin", "server", nil)
	assert.Nil(t, err)
	assert.True(t, fedClient.queryKeysCalled)
}

func TestFederationClientQueryKeysBlacklisted(t *testing.T) {

	ctx, svc, cfg := testrig.Init(t, testOpts)
	defer svc.Stop(ctx)
	testDB := test.NewInMemoryFederationDatabase()
	err := testDB.AddServerToBlacklist(ctx, "server")
	assert.Nil(t, err)

	cfg := config.FederationAPI{
		Global: &config.Global{
			SigningIdentity: fclient.SigningIdentity{
				ServerName: "server",
			},
		},
	}
	fedClient := &testFedClient{}
	stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
	queues := queue.NewOutgoingQueues(
		ctx, testDB,
		false,
		cfg.Global.ServerName, fedClient, &stats,
		nil,
	)
	fedapi := FederationInternalAPI{
		db:         testDB,
		cfg:        &cfg,
		statistics: &stats,
		federation: fedClient,
		queues:     queues,
	}
	_, err = fedapi.QueryKeys(ctx, "origin", "server", nil)
	assert.NotNil(t, err)
	assert.False(t, fedClient.queryKeysCalled)
}

func TestFederationClientQueryKeysFailure(t *testing.T) {
	ctx, svc, cfg := testrig.Init(t, testOpts)
	defer svc.Stop(ctx)
	testDB := test.NewInMemoryFederationDatabase()

	cfg := config.FederationAPI{
		Global: &config.Global{
			SigningIdentity: fclient.SigningIdentity{
				ServerName: "server",
			},
		},
	}
	fedClient := &testFedClient{shouldFail: true}
	stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
	queues := queue.NewOutgoingQueues(
		ctx, testDB,
		false,
		cfg.Global.ServerName, fedClient, &stats,
		nil,
	)
	fedapi := FederationInternalAPI{
		db:         testDB,
		cfg:        &cfg,
		statistics: &stats,
		federation: fedClient,
		queues:     queues,
	}
	_, err := fedapi.QueryKeys(ctx, "origin", "server", nil)
	assert.NotNil(t, err)
	assert.True(t, fedClient.queryKeysCalled)
}

func TestFederationClientClaimKeys(t *testing.T) {

	ctx, svc, cfg := testrig.Init(t, testOpts)
	defer svc.Stop(ctx)
	testDB := test.NewInMemoryFederationDatabase()

	cfg := config.FederationAPI{
		Global: &config.Global{
			SigningIdentity: fclient.SigningIdentity{
				ServerName: "server",
			},
		},
	}
	fedClient := &testFedClient{}
	stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
	queues := queue.NewOutgoingQueues(
		ctx, testDB,
		false,
		cfg.Global.ServerName, fedClient, &stats,
		nil,
	)
	fedapi := FederationInternalAPI{
		db:         testDB,
		cfg:        &cfg,
		statistics: &stats,
		federation: fedClient,
		queues:     queues,
	}
	_, err := fedapi.ClaimKeys(ctx, "origin", "server", nil)
	assert.Nil(t, err)
	assert.True(t, fedClient.claimKeysCalled)
}

func TestFederationClientClaimKeysBlacklisted(t *testing.T) {
	ctx, svc, cfg := testrig.Init(t, testOpts)
	defer svc.Stop(ctx)
	testDB := test.NewInMemoryFederationDatabase()
	err := testDB.AddServerToBlacklist(ctx, "server")
	assert.Nil(t, err)

	cfg := config.FederationAPI{
		Global: &config.Global{
			SigningIdentity: fclient.SigningIdentity{
				ServerName: "server",
			},
		},
	}
	fedClient := &testFedClient{}
	stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
	queues := queue.NewOutgoingQueues(
		ctx, testDB,
		false,
		cfg.Global.ServerName, fedClient, &stats,
		nil,
	)
	fedapi := FederationInternalAPI{
		db:         testDB,
		cfg:        &cfg,
		statistics: &stats,
		federation: fedClient,
		queues:     queues,
	}
	_, err = fedapi.ClaimKeys(ctx, "origin", "server", nil)
	assert.NotNil(t, err)
	assert.False(t, fedClient.claimKeysCalled)
}
