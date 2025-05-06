// Copyright 2023 The Global.org Foundation C.I.C.
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

package sqlutil

import (
	"context"
	"errors"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame" // assumed path; adjust if needed
	"gorm.io/gorm"
)

type Connections struct {
	opts *config.DatabaseOptions

	service *frame.Service // optional
	dbPool  *frame.Pool    // keyed by connection string
	writer  Writer
}

func (c *Connections) DS() *config.DataSource {
	slc := c.opts.ConnectionString.ToArray()
	if len(slc) == 0 {
		return nil
	}
	return &slc[0]
}

func (c *Connections) Migrate() bool {
	return c.dbPool.CanMigrate()
}

func (c *Connections) Connection(ctx context.Context, readOnly bool) *gorm.DB {

	if c.dbPool == nil {
		return nil
	}

	return c.dbPool.DB(ctx, readOnly)
}

func (c *Connections) FromOptions(ctx context.Context, opts *config.DatabaseOptions) (*Connections, error) {
	conn, err := NewConnectionManagerWithOptions(ctx, c.service, opts)

	if err != nil {
		c.service.L(ctx).WithError(err).Error("Failed to create connection manager, reusing current connection manager")
		return c, nil
	}

	if conn.dbPool == nil {
		c.service.L(ctx).Info("Reusing current connection manager, because old has no pool")
		conn = c
	}

	return conn, nil

}

func (c *Connections) MigrateStrings(ctx context.Context, migrations ...frame.MigrationPatch) error {

	err := c.service.MigrateDatastore(ctx, "")
	if err != nil {
		return err
	}

	for _, migration := range migrations {
		err = c.service.SaveMigrationWithPool(ctx, c.dbPool, migration)
		if err != nil {
			return err
		}
	}

	return c.service.MigratePool(ctx, c.dbPool, "")
}

func NewConnectionManager(service *frame.Service) *Connections {

	var dbPool *frame.Pool
	var opts *config.DatabaseOptions

	if service != nil {
		dbPool = service.DBPool()

		cfg := service.Config().(frame.ConfigurationDatabase)
		primaryUrl := cfg.GetDatabasePrimaryHostURL()
		if len(primaryUrl) > 0 {
			opts = &config.DatabaseOptions{ConnectionString: config.DataSource(primaryUrl[0])}
		}
	}

	return &Connections{
		service: service,
		dbPool:  dbPool,
		opts:    opts,
	}
}

// NewConnectionManagerWithOptions ensures a Pool exists for the given options and returns a copy with that as primary
func NewConnectionManagerWithOptions(ctx context.Context, service *frame.Service, opts *config.DatabaseOptions) (*Connections, error) {
	connStr := opts.ConnectionString
	if connStr == "" {
		return nil, errors.New("no database connection string provided")
	}

	connSlice := connStr.ToArray()
	if len(connSlice) == 0 {
		return nil, errors.New("no valid database connection string provided")
	}
	primaryConn := connSlice[0]
	if !primaryConn.IsPostgres() {
		return nil, errors.New("primary database connection should be a postgresql url")
	}

	primaryConnStr := string(primaryConn)

	pool := service.DBPool(primaryConnStr)
	if pool == nil {
		var dbOpts []frame.Option
		for _, conn := range connSlice {
			if conn.IsPostgres() {
				dbOpts = append(dbOpts, frame.DatastoreConnectionWithName(ctx, primaryConnStr, string(conn), false))
			}
		}
		service.Init(dbOpts...)
		pool = service.DBPool(primaryConnStr)
	}

	// Return a copy with new main
	return &Connections{
		service: service,
		dbPool:  pool,
		opts:    opts,
	}, nil
}
