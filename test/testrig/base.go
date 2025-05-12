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

package testrig

import (
	"context"
	"fmt"
	"github.com/pitabwire/frame"
	"testing"
	"time"

	"github.com/pitabwire/util"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
)

func newService(t *testing.T) (context.Context, *frame.Service) {
	ctx := t.Context()
	opts := []frame.Option{frame.NoopDriver()}
	return frame.NewServiceWithContext(ctx, "Test Srv", opts...)
}

func newServiceWithoutT() (context.Context, *frame.Service) {
	opts := []frame.Option{frame.NoopDriver()}
	return frame.NewService("Test Srv", opts...)
}

func CreateConfig(ctx context.Context, testOpts test.DependancyOption) (*config.Dendrite, func(ctx context.Context), error) {

	defaultOpts, closeDSConns, err := test.PrepareDefaultDSConnections(ctx, testOpts)
	if err != nil {
		return nil, nil, err
	}

	var cfg config.Dendrite
	cfg.Defaults(defaultOpts)
	cfg.FederationAPI.KeyPerspectives = nil

	cfg.Global.Cache.MaxAge = time.Minute * 5
	cfg.Global.Cache.EstimatedMaxSize = 8 * 1024 * 1024
	cfg.Global.Cache.EnablePrometheus = false

	cfg.Global.DatabaseOptions.MaxOpenConnections = 10
	cfg.Global.DatabaseOptions.ConnMaxLifetimeSeconds = 60

	for _, conn := range defaultOpts.DatabaseConnectionStr.ToArray() {
		cfg.Global.DatabasePrimaryURL = append(cfg.Global.DatabasePrimaryURL, string(conn))
	}

	cfg.Global.DatabaseMigrate = "true"
	cfg.Global.DatabaseMigrationPath = "./migrations/0001"

	cfg.Global.ServerName = "test"
	// use a distinct prefix else concurrent postgres runs will clash since NATS will use
	// the file system event with InMemory=true :(
	cfg.Global.JetStream.TopicPrefix = fmt.Sprintf("Test_%s_", util.RandomString(8))
	cfg.SyncAPI.Fulltext.InMemory = true

	return &cfg, func(ctx context.Context) {
		closeDSConns(ctx)
	}, nil
}

func Init(t *testing.T, testOpts ...test.DependancyOption) (context.Context, *frame.Service, *config.Dendrite) {

	opts := test.DependancyOption{}
	if len(testOpts) > 0 {
		opts = testOpts[0]
	}

	ctx, srv := newService(t)
	cfg, clearConfig, err := CreateConfig(ctx, opts)
	if err != nil {
		t.Fatalf("Could not create default connections %s", err)
	}
	srv.AddCleanupMethod(clearConfig)

	srvOpts := []frame.Option{frame.Config(&cfg.Global), frame.Datastore(ctx)}

	srv.Init(srvOpts...)

	return ctx, srv, cfg
}

func InitWithoutT(testOpts ...test.DependancyOption) (context.Context, *frame.Service, *config.Dendrite, error) {

	opts := test.DependancyOption{}
	if len(testOpts) > 0 {
		opts = testOpts[0]
	}

	ctx, srv := newServiceWithoutT()
	cfg, clearConfig, err := CreateConfig(ctx, opts)
	if err != nil {
		return ctx, nil, nil, err
	}

	srv.AddCleanupMethod(clearConfig)

	scfg := cfg.Global

	srvOpts := []frame.Option{frame.Config(&scfg), frame.Datastore(ctx)}

	srv.Init(srvOpts...)

	return ctx, srv, cfg, nil
}
