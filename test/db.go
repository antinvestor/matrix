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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/antinvestor/matrix/setup/config"
)

const (
	DefaultDB    = "postgres"
	DefaultCache = "redis"
	DefaultQueue = "nats"
)

type DependancyOption struct {
	name  string
	db    string
	cache string
	queue string
}

func (opt *DependancyOption) Name() string {
	return opt.name
}
func (opt *DependancyOption) Database() string {
	if opt.db == "" {
		return DefaultDB
	}
	return opt.db
}
func (opt *DependancyOption) Cache() string {
	if opt.cache == "" {
		return DefaultCache
	}
	return opt.cache
}
func (opt *DependancyOption) Queue() string {
	if opt.queue == "" {
		return DefaultQueue
	}
	return opt.queue
}

// ensureDatabaseExists checks if a specific database exists and creates it if it does not.
func ensureDatabaseExists(ctx context.Context, postgresUri *url.URL, newDbName string) (*url.URL, error) {
	connectionString := postgresUri.String()
	cfg, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return postgresUri, err
	}
	cfg.MaxConns = 20 // Increase pool size for concurrency
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return postgresUri, err
	}

	defer pool.Close()

	if err = pool.Ping(ctx); err != nil {
		return postgresUri, err
	}

	// Check if database exists before trying to create it
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s;`, newDbName))
	if err != nil {

		var pgErr *pgconn.PgError
		ok := errors.As(err, &pgErr)
		if !(ok && pgErr.Code == "42P04" || (pgErr.Code == "XX000" && strings.Contains(pgErr.Message, "tuple concurrently updated"))) {
			return postgresUri, err
		}
	}

	dbUserName := postgresUri.User.Username()
	_, err = pool.Exec(ctx, fmt.Sprintf(`GRANT ALL PRIVILEGES ON DATABASE %s TO %s;`, newDbName, dbUserName))
	if err != nil {
		var pgErr *pgconn.PgError
		ok := errors.As(err, &pgErr)
		if !(ok && pgErr.Code == "XX000" && strings.Contains(pgErr.Message, "tuple concurrently updated")) {
			return postgresUri, err
		}
	}

	postgresUri.Path = newDbName
	return postgresUri, nil
}

func clearDatabase(ctx context.Context, connectionString string) error {

	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		return err
	}
	defer pool.Close()

	_, err = pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`)
	if err != nil {
		return err
	}
	return nil
}

func generateNewDBName() (string, error) {
	// we cannot use 'matrix_test' here else 2x concurrently running packages will try to use the same db.
	// instead, hash the current working directory, snaffle the first 16 bytes and append that to matrix_test
	// and use that as the unique db name. We do this because packages are per-directory hence by hashing the
	// working (test) directory we ensure we get a consistent hash and don't hash against concurrent packages.
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256([]byte(wd))
	databaseName := fmt.Sprintf("matrix_tests_%s", hex.EncodeToString(hash[:16]))
	return databaseName, nil
}

// PrepareDatabaseConnection Prepare a postgres connection string for testing.
// Returns the connection string to use and a close function which must be called when the test finishes.
// Calling this function twice will return the same database, which will have data from previous tests
// unless close() is called.
func PrepareDatabaseConnection(ctx context.Context, testOpts DependancyOption) (postgresDataSource config.DataSource, close func(context.Context), err error) {

	if testOpts.Database() != DefaultDB {
		return "", func(ctx context.Context) {}, errors.New("only postgresql is the supported database for now")
	}

	postgresUriStr := os.Getenv("TESTING_DATABASE_URI")
	if postgresUriStr == "" {
		postgresUriStr = "postgres://matrix:s3cr3t@127.0.0.1:5432/matrix?sslmode=disable"
	}
	parsedPostgresUri, err := url.Parse(postgresUriStr)
	if err != nil {
		return "", func(ctx context.Context) {}, err
	}

	newDatabaseName, err := generateNewDBName()
	if err != nil {
		return "", func(ctx context.Context) {}, err
	}

	connectionUri, err := ensureDatabaseExists(ctx, parsedPostgresUri, newDatabaseName)
	if err != nil {
		return "", func(ctx context.Context) {}, err
	}

	postgresUriStr = connectionUri.String()
	return config.DataSource(postgresUriStr), func(ctx context.Context) {
		_ = clearDatabase(ctx, postgresUriStr)
	}, nil
}

// WithAllDatabases Creates subtests with each known DependancyOption
func WithAllDatabases(t *testing.T, testFn func(t *testing.T, db DependancyOption)) {
	options := []DependancyOption{
		{
			name:  "Default",
			db:    DefaultDB,
			cache: DefaultCache,
			queue: DefaultQueue,
		},
	}
	for _, opt := range options {
		t.Run(opt.Name(), func(tt *testing.T) {
			tt.Parallel()
			testFn(tt, opt)
		})
	}
}
