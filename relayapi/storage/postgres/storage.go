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

package postgres

import (
	"context"
	"database/sql"
	"github.com/pitabwire/frame"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/relayapi/storage/shared"
)

// Migrations All relayapi migrations for the postgres module
var Migrations = []frame.MigrationPatch{
	{
		Name:        "relayapi_001_create_queue_json_table",
		Patch:       relayQueueJSONSchema,
		RevertPatch: relayQueueJSONSchemaRevert,
	},
	{
		Name:        "relayapi_002_create_queue_table",
		Patch:       relayQueueSchema,
		RevertPatch: relayQueueSchemaRevert,
	},
}

// Database stores information needed by the relayapi
type Database struct {
	shared.Database
	db     *sql.DB
	writer sqlutil.Writer
}

// NewDatabase opens a new database
func NewDatabase(
	ctx context.Context,
	cm *sqlutil.Connections,
	cache caching.FederationCache,
	isLocalServerName func(spec.ServerName) bool,
) (*Database, error) {
	var d Database

	err := cm.MigrateStrings(ctx, Migrations...)
	if err != nil {
		return nil, err
	}

	queue := NewPostgresRelayQueueTable(cm)
	queueJSON := NewPostgresRelayQueueJSONTable(cm)
	d.Database = shared.Database{
		IsLocalServerName: isLocalServerName,
		Cache:             cache,
		RelayQueue:        queue,
		RelayQueueJSON:    queueJSON,
	}
	return &d, nil
}
