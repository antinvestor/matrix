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
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/antinvestor/matrix/setup/config"

	"github.com/lib/pq"
)

type DependancyOption struct {
	name  string
	db    string
	cache string
	queue string
}

func (opt *DependancyOption) Name() string {
	return "default"
}
func (opt *DependancyOption) Database() string {
	return "postgres"
}
func (opt *DependancyOption) Cache() string {
	return "redis"
}
func (opt *DependancyOption) Queue() string {
	return "nats"
}

// ensureDatabaseExists checks if a specific database exists and creates it if it does not.
func ensureDatabaseExists(_ context.Context, postgresUri *url.URL, newDbName string) (*url.URL, error) {

	var connectionString = postgresUri.String()
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return postgresUri, err
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	if err = db.Ping(); err != nil {
		return postgresUri, err
	}
	_, err = db.Exec(fmt.Sprintf(`CREATE DATABASE %s;`, newDbName))
	if err != nil {
		var pqErr *pq.Error
		ok := errors.As(err, &pqErr)
		if !ok {
			return postgresUri, err
		}
		// we ignore duplicate database error as we expect this
		if pqErr.Code != "42P04" {
			return postgresUri, pqErr
		}
	}

	dbUserName := postgresUri.User.Username()

	_, err = db.Exec(fmt.Sprintf(`GRANT ALL PRIVILEGES ON DATABASE %s TO %s;`, newDbName, dbUserName))
	if err != nil {
		return postgresUri, err
	}

	postgresUri.Path = newDbName

	return postgresUri, nil
}

func clearDatabase(_ context.Context, connectionStr string) error {

	db, err := sql.Open("postgres", connectionStr)
	if err != nil {
		return err
	}

	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	_, err = db.Exec(`DROP SCHEMA public CASCADE; CREATE SCHEMA public;`)
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

// PrepareDatabaseDSConnection Prepare a postgres connection string for testing.
// Returns the connection string to use and a close function which must be called when the test finishes.
// Calling this function twice will return the same database, which will have data from previous tests
// unless close() is called.
func PrepareDatabaseDSConnection(ctx context.Context) (postgresDataSource config.DataSource, close func(), err error) {

	var connectionUri *url.URL
	postgresUriStr := os.Getenv("TESTING_DATABASE_URI")
	if postgresUriStr == "" {
		postgresUriStr = "postgres://matrix:s3cr3t@127.0.0.1:5432/matrix?sslmode=disable"
	}

	parsedPostgresUri, err := url.Parse(postgresUriStr)
	if err != nil {
		return "", func() {}, err
	}

	newDatabaseName, err := generateNewDBName()
	if err != nil {
		return "", func() {}, err
	}

	connectionUri, err = ensureDatabaseExists(ctx, parsedPostgresUri, newDatabaseName)
	if err != nil {
		return "", func() {}, err
	}

	postgresUriStr = connectionUri.String()
	return config.DataSource(postgresUriStr), func() {
		_ = clearDatabase(ctx, postgresUriStr)
	}, nil
}

// WithAllDatabases Creates subtests with each known DependancyOption
func WithAllDatabases(t *testing.T, testFn func(t *testing.T, db DependancyOption)) {
	options := []DependancyOption{
		{
			name:  "Default",
			db:    "postgres",
			cache: "redis",
			queue: "nats",
		},
	}
	for _, opt := range options {
		t.Run(opt.Name(), func(tt *testing.T) {
			tt.Parallel()
			testFn(tt, opt)
		})
	}
}
