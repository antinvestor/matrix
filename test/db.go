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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcPostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const PostgresqlImage = "postgres:17"

type DBType int

var DBTypeSQLite DBType = 1
var DBTypePostgres DBType = 2

func setupPostgres(ctx context.Context, dbName, user, connStr string) (*tcPostgres.PostgresContainer, error) {

	postgresContainer, err := tcPostgres.Run(ctx,
		PostgresqlImage,
		tcPostgres.WithDatabase(dbName),
		tcPostgres.WithUsername(user),
		tcPostgres.WithPassword(connStr),

		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		return nil, err
	}

	return postgresContainer, nil
}

// PrepareDBConnectionString Prepare a sqlite or postgres connection string for testing.
// Returns the connection string to use and a close function which must be called when the test finishes.
// Calling this function twice will return the same database, which will have data from previous tests
// unless close() is called.
func PrepareDBConnectionString(t *testing.T, dbType DBType) (connStr string, close func()) {
	ctx := context.Background()
	if dbType == DBTypeSQLite {
		// this will be made in the t.TempDir, which is unique per test
		dbname := filepath.Join(t.TempDir(), "dendrite_test.db")
		return fmt.Sprintf("file:%s", dbname), func() {
			t.Cleanup(func() {}) // removes the t.TempDir
		}
	}

	// Required vars: user and db
	// We'll try to infer from the local env if they are missing
	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		user = "matrix"
	}
	// optional vars, used in CI
	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		password = "s3cr3t"
	}

	// we cannot use 'dendrite_test' here else 2x concurrently running packages will try to use the same db.
	// instead, hash the current working directory, snaffle the first 16 bytes and append that to dendrite_test
	// and use that as the unique db name. We do this because packages are per-directory hence by hashing the
	// working (test) directory we ensure we get a consistent hash and don't hash against concurrent packages.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("cannot get working directory: %s", err)
	}
	hash := sha256.Sum256([]byte(wd))
	dbName := fmt.Sprintf("dendrite_test_%s", hex.EncodeToString(hash[:16]))

	pgContainer, err := setupPostgres(ctx, dbName, user, password)
	if err != nil {
		t.Fatalf("cannot instantiate postgresql %s", err)
	}

	connStr, err = pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("cannot get postgresql connection %s", err)
	}

	return connStr, func() {
		_ = pgContainer.Terminate(ctx)
	}
}

// WithAllDatabases Creates subtests with each known DBType
func WithAllDatabases(t *testing.T, testFn func(t *testing.T, db DBType)) {
	dbs := map[string]DBType{
		"postgres": DBTypePostgres,
		"sqlite":   DBTypeSQLite,
	}
	for dbName, dbType := range dbs {
		dbt := dbType
		t.Run(dbName, func(tt *testing.T) {
			tt.Parallel()
			testFn(tt, dbt)
		})
	}
}
