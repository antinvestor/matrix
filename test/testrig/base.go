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

func CreateConfig(t *testing.T, testOpts test.DependancyOption) (*config.Dendrite, *process.ProcessContext, func()) {

	processContext := process.NewProcessContext()
	ctx := processContext.Context()
	defaultOpts, closeDSConns, err := test.PrepareDefaultDSConnections(ctx)
	if err != nil {
		t.Fatalf("Could not create default connections %s", err)
	}

	var cfg config.Dendrite
	cfg.Defaults(defaultOpts)
	cfg.FederationAPI.KeyPerspectives = nil

	cfg.Global.Cache.MaxAge = time.Minute * 5
	cfg.Global.Cache.EstimatedMaxSize = 8 * 1024 * 1024
	cfg.Global.Cache.EnablePrometheus = false

	cfg.Global.DatabaseOptions.MaxOpenConnections = 10
	cfg.Global.DatabaseOptions.ConnMaxLifetimeSeconds = 60

	cfg.Global.ServerName = "test"
	// use a distinct prefix else concurrent postgres runs will clash since NATS will use
	// the file system event with InMemory=true :(
	cfg.Global.JetStream.TopicPrefix = fmt.Sprintf("Test_%s_", testOpts.Database())
	cfg.SyncAPI.Fulltext.InMemory = true

	return &cfg, processContext, func() {
		processContext.ShutdownDendrite()
		processContext.WaitForShutdown()
		closeDSConns()
	}
}
