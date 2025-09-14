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
	"database/sql"
	"errors"
	"strings"
	"sync"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame" // assumed path; adjust if needed
	"github.com/pitabwire/util"
	"gorm.io/gorm"
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
	slc := c.opts.DatabaseURI.ToArray()
	if len(slc) == 0 {
		return nil
	}
	return &slc[0]
}

func (c *Connections) Connection(ctx context.Context, readOnly bool) *gorm.DB {
	// Check for a transaction in the stack first
	stack := GetTransactionStack(ctx)
	if !stack.IsEmpty() {
		if txn, ok := stack.Peek().(*defaultTransaction); ok {
			return txn.txn
		}
	}

	// Check for legacy transaction in context (for backward compatibility)
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

	// Get the active transaction stack or create a new one
	stack := GetTransactionStack(ctx)
	ctx = WithTransactionStack(ctx, stack)

	writeDb := c.Connection(ctx, false)
	gormTxn := writeDb.Begin(sqlOpts...)
	txn := newDefaultTransaction(gormTxn)

	// Add to context for backward compatibility
	ctx = context.WithValue(ctx, ctxKeyTransaction, txn)

	return ctx, txn, gormTxn.Error
}

func (c *Connections) FromOptions(ctx context.Context, opts *config.DatabaseOptions) (ConnectionManager, error) {

	log := util.Log(ctx)

	conn, err := NewConnectionManagerWithOptions(ctx, c.service, opts)

	if err != nil {
		log.WithError(err).Error("Failed to create connection manager, reusing current connection manager")
		return c, nil
	}

	if conn != nil {
		return conn, nil
	}

	log.Debug("Reusing active connection manager")
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
			opts = &config.DatabaseOptions{
				Prefix:                 "",
				Reference:              "",
				DatabaseURI:            config.DataSource(strings.Join(primaryUrl, ",")),
				MaxOpenConnections:     cfg.GetMaxOpenConnections(),
				MaxIdleConnections:     cfg.GetMaxIdleConnections(),
				ConnMaxLifetimeSeconds: int(cfg.GetMaxConnectionLifeTimeInSeconds().Seconds()),
			}
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
	connStr := opts.DatabaseURI
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

	pool := service.DBPool(opts.Ref())
	if pool == nil {
		var dbOpts []frame.Option
		for _, conn := range connSlice {
			if conn.IsPostgres() {
				dbOpts = append(dbOpts, frame.WithDatastoreConnectionWithName(opts.Ref(), string(conn), false))
			}
		}
		service.Init(ctx, dbOpts...)
		pool = service.DBPool(opts.Ref())
	}

	// Return a copy with new main
	return &Connections{
		service: service,
		dbPool:  pool,
		opts:    opts,
	}, nil
}
