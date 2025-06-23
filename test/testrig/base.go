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
	"testing"
	"time"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/pitabwire/frame"
)

func newService(t *testing.T) (context.Context, *frame.Service) {
	ctx := t.Context()
	opts := []frame.Option{frame.WithNoopDriver()}
	return frame.NewServiceWithContext(ctx, "Test Srv", opts...)
}

func newServiceWithoutT() (context.Context, *frame.Service) {
	opts := []frame.Option{frame.WithNoopDriver()}
	return frame.NewService("Test Srv", opts...)
}

func CreateConfig(ctx context.Context, testOpts test.DependancyOption) (*config.Matrix, func(ctx context.Context), error) {

	defaultOpts, closeDSConns, err := test.PrepareDefaultDSConnections(ctx, testOpts)
	if err != nil {
		return nil, nil, err
	}

	cfg, err := frame.ConfigFromEnv[config.Matrix]()
	if err != nil {
		return nil, nil, err
	}
	cfg.Defaults(defaultOpts)
	cfg.FederationAPI.KeyPerspectives = nil

	cfg.Global.LogLevel = "Warn"
	cfg.Global.LogShowStackTrace = true

	cfg.Global.Cache.MaxAge = time.Minute * 5
	cfg.Global.Cache.EstimatedMaxSize = 8 * 1024 * 1024
	cfg.Global.Cache.EnablePrometheus = false

	for _, conn := range defaultOpts.DSDatabaseConn.ToArray() {
		cfg.Global.DatabasePrimaryURL = append(cfg.Global.DatabasePrimaryURL, string(conn))
	}

	cfg.Global.DatabaseMigrate = true
	cfg.Global.DatabaseMigrationPath = "./migrations/0001"

	cfg.Global.ServerName = "test"
	// use a distinct prefix else concurrent postgres runs will clash since NATS will use
	// the file system event with InMemory=true :(
	cfg.SyncAPI.Fulltext.InMemory = true

	return &cfg, func(ctx context.Context) {
		closeDSConns(ctx)
	}, nil
}

func Init(t *testing.T, testOpts ...test.DependancyOption) (context.Context, *frame.Service, *config.Matrix) {

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

	srvOpts := []frame.Option{
		frame.WithConfig(&cfg.Global),
		frame.WithDatastore(),
		frame.WithWorkerPoolOptions(
			frame.WithSinglePoolCapacity(40),
			frame.WithConcurrency(1),
			frame.WithPoolCount(1),
		),
	}

	srv.Init(ctx, srvOpts...)

	return ctx, srv, cfg
}

func InitWithoutT(testOpts ...test.DependancyOption) (context.Context, *frame.Service, *config.Matrix, error) {

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

	srvOpts := []frame.Option{frame.WithConfig(&scfg), frame.WithDatastore()}

	srv.Init(ctx, srvOpts...)

	return ctx, srv, cfg, nil
}
