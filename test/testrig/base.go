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

package testrig

import (
	"fmt"
	"testing"
	"time"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/process"
	"github.com/antinvestor/matrix/test"
)

func CreateConfig(t *testing.T, dbType test.DBType) (*config.Dendrite, *process.ProcessContext, func()) {
	var cfg config.Dendrite
	cfg.Defaults(config.DefaultOpts{
		Generate:       false,
		SingleDatabase: true,
	})
	cfg.Global.JetStream.InMemory = true
	cfg.FederationAPI.KeyPerspectives = nil
	processContext := process.NewProcessContext()

	ctx := processContext.Context()
	cacheConnStr, closeCache, err := test.PrepareRedisConnectionString(ctx)
	if err != nil {
		t.Fatalf("Could not create redis container %s", err)
	}

	cfg.Global.Cache = config.CacheOptions{
		MaxAge:           time.Minute * 5,
		EstimatedMaxSize: 8 * 1024 * 1024,
		ConnectionString: cacheConnStr,
		EnablePrometheus: false,
	}

	if dbType != test.DBTypePostgres {
		t.Fatalf("unknown db type: %v", dbType)
	}

	cfg.Global.Defaults(config.DefaultOpts{ // autogen a signing key
		Generate:       true,
		SingleDatabase: true,
	})
	cfg.MediaAPI.Defaults(config.DefaultOpts{ // autogen a media path
		Generate:       true,
		SingleDatabase: true,
	})
	cfg.SyncAPI.Fulltext.Defaults(config.DefaultOpts{ // use in memory fts
		Generate:       true,
		SingleDatabase: true,
	})
	cfg.Global.ServerName = "test"
	// use a distinct prefix else concurrent postgres/sqlite runs will clash since NATS will use
	// the file system event with InMemory=true :(
	cfg.Global.JetStream.TopicPrefix = fmt.Sprintf("Test_%d_", dbType)
	cfg.SyncAPI.Fulltext.InMemory = true

	connStr, closeDb, err := test.PrepareDBConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	cfg.Global.DatabaseOptions = config.DatabaseOptions{
		ConnectionString:       config.DataSource(connStr),
		MaxOpenConnections:     10,
		MaxIdleConnections:     2,
		ConnMaxLifetimeSeconds: 60,
	}
	return &cfg, processContext, func() {
		processContext.ShutdownDendrite()
		processContext.WaitForShutdown()
		closeDb()
		closeCache()
	}
}
