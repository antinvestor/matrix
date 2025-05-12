// Copyright 2023 The Matrix.org Foundation C.I.C.
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
	"database/sql"
	"errors"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame" // assumed path; adjust if needed
	"gorm.io/gorm"
	"sync"
)

type ConnectionManager interface {
	Writer
	DS() *config.DataSource
	Connection(ctx context.Context, readOnly bool) *gorm.DB
	BeginTx(ctx context.Context, opts ...*WriterOption) (context.Context, Transaction, error)
	FromOptions(ctx context.Context, opts *config.DatabaseOptions) (ConnectionManager, error)
	Collect(migrations ...*frame.MigrationPatch) error
	Migrate(ctx context.Context) error
}

type Connections struct {
	opts *config.DatabaseOptions

	service *frame.Service // optional
	dbPool  *frame.Pool    // keyed by connection string

	migrations    []*frame.MigrationPatch
	migrationLock sync.Mutex
}

func (c *Connections) DS() *config.DataSource {
	slc := c.opts.ConnectionString.ToArray()
	if len(slc) == 0 {
		return nil
	}
	return &slc[0]
}

func (c *Connections) Connection(ctx context.Context, readOnly bool) *gorm.DB {

	txn, ok := ctx.Value(ctxKeyTransaction).(*defaultTransaction)
	if ok {
		return txn.txn
	}

	if c.dbPool == nil {
		return nil
	}

	return c.dbPool.DB(ctx, readOnly)
}

func (c *Connections) BeginTx(ctx context.Context, opts ...*WriterOption) (context.Context, Transaction, error) {

	var sqlOpts []*sql.TxOptions
	for _, opt := range opts {
		sqlOpts = append(sqlOpts, opt.SqlOpts...)
	}

	writeDb := c.Connection(ctx, false)
	gormTxn := writeDb.Begin(sqlOpts...)
	txn := newDefaultTransaction(gormTxn)
	ctx = context.WithValue(ctx, ctxKeyTransaction, txn)
	return ctx, txn, gormTxn.Error
}

func (c *Connections) FromOptions(ctx context.Context, opts *config.DatabaseOptions) (ConnectionManager, error) {
	conn, err := NewConnectionManagerWithOptions(ctx, c.service, opts)

	if err != nil {
		c.service.L(ctx).WithError(err).Error("Failed to create connection manager, reusing current connection manager")
		return c, nil
	}

	if conn != nil {
		return conn, nil
	}

	c.service.L(ctx).Info("Reusing active connection manager")
	return c, nil

}

func (c *Connections) Collect(migrations ...*frame.MigrationPatch) error {
	c.migrationLock.Lock()
	defer c.migrationLock.Unlock()

	c.migrations = append(c.migrations, migrations...)
	return nil
}

func (c *Connections) Migrate(ctx context.Context) error {
	c.migrationLock.Lock()
	defer c.migrationLock.Unlock()

	if c.dbPool.CanMigrate() {

		cfg := c.service.Config().(frame.ConfigurationDatabase)
		migrationsDirPath := cfg.GetDatabaseMigrationPath()

		err := c.service.MigratePool(ctx, c.dbPool, migrationsDirPath)
		if err != nil {
			return err
		}

		err = c.service.SaveMigrationWithPool(ctx, c.dbPool, c.migrations...)
		if err != nil {
			return err
		}

		err = c.service.MigratePool(ctx, c.dbPool, "")
		if err != nil {
			return err
		}
	}

	c.migrations = nil

	return nil
}

func (c *Connections) Do(ctx context.Context, f func(ctx context.Context) error, opts ...*WriterOption) error {
	ctx0, txn, err := c.BeginTx(ctx, opts...)
	if err != nil {
		return err
	}

	return WithTransaction(ctx0, txn, func(ctx context.Context) error {
		return f(ctx)
	})
}

func NewConnectionManager(service *frame.Service) ConnectionManager {

	var dbPool *frame.Pool
	var opts *config.DatabaseOptions

	if service != nil {
		dbPool = service.DBPool()

		cfg := service.Config().(frame.ConfigurationDatabase)
		primaryUrl := cfg.GetDatabasePrimaryHostURL()
		if len(primaryUrl) > 0 {
			opts = &config.DatabaseOptions{ConnectionString: config.DataSource(primaryUrl[0])}
		}

		cfg.GetDatabaseMigrationPath()
	}

	return &Connections{
		service: service,
		dbPool:  dbPool,
		opts:    opts,
	}
}

// NewConnectionManagerWithOptions ensures a Pool exists for the given options and returns a copy with that as primary
func NewConnectionManagerWithOptions(ctx context.Context, service *frame.Service, opts *config.DatabaseOptions) (ConnectionManager, error) {
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
