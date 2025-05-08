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

package postgres

import (
	"context"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/lib/pq"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/syncapi/storage/tables"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/pitabwire/frame"
)

// Schema for presence table
const presenceSchema = `
CREATE SEQUENCE IF NOT EXISTS syncapi_presence_id;
-- Stores data about presence
CREATE TABLE IF NOT EXISTS syncapi_presence (
	-- The ID
	id BIGINT PRIMARY KEY DEFAULT nextval('syncapi_presence_id'),
	-- The Matrix user ID
	user_id TEXT NOT NULL,
	-- The actual presence
	presence INT NOT NULL,
	-- The status message
	status_msg TEXT,
	-- The last time an action was received by this user
	last_active_ts BIGINT NOT NULL,
	CONSTRAINT presence_presences_unique UNIQUE (user_id)
);
CREATE INDEX IF NOT EXISTS syncapi_presence_user_id ON syncapi_presence(user_id);
`

// Revert schema for presence table
const presenceSchemaRevert = `
DROP INDEX IF EXISTS syncapi_presence_user_id;
DROP TABLE IF EXISTS syncapi_presence;
DROP SEQUENCE IF EXISTS syncapi_presence_id;
`

// SQL query to upsert presence
const upsertPresenceSQL = `
INSERT INTO syncapi_presence AS p
 (user_id, presence, status_msg, last_active_ts)
 VALUES ($1, $2, $3, $4)
 ON CONFLICT (user_id)
 DO UPDATE SET id = nextval('syncapi_presence_id'),
 presence = $2, status_msg = COALESCE($3, p.status_msg), last_active_ts = $4
 RETURNING id
`

// SQL query to upsert presence from sync
const upsertPresenceFromSyncSQL = `
INSERT INTO syncapi_presence AS p
 (user_id, presence, last_active_ts)
 VALUES ($1, $2, $3)
 ON CONFLICT (user_id)
 DO UPDATE SET id = nextval('syncapi_presence_id'),
 presence = $2, last_active_ts = $3
 RETURNING id
`

// SQL query to select presence for users
const selectPresenceForUserSQL = `
SELECT user_id, presence, status_msg, last_active_ts
 FROM syncapi_presence
 WHERE user_id = ANY($1)
`

// SQL query to select max presence ID
const selectMaxPresenceSQL = `
SELECT COALESCE(MAX(id), 0) FROM syncapi_presence
`

// SQL query to select presence after a position
const selectPresenceAfter = `
 SELECT id, user_id, presence, status_msg, last_active_ts
 FROM syncapi_presence
 WHERE id > $1 AND last_active_ts >= $2
 ORDER BY id ASC LIMIT $3
`

// presenceTable implements tables.Presence
type presenceTable struct {
	cm                        *sqlutil.Connections
	upsertPresenceSQL         string
	upsertPresenceFromSyncSQL string
	selectPresenceForUserSQL  string
	selectMaxPresenceSQL      string
	selectPresenceAfter       string
}

// NewPostgresPresenceTable creates a new presence table
func NewPostgresPresenceTable(ctx context.Context, cm *sqlutil.Connections) (tables.Presence, error) {
	t := &presenceTable{
		cm:                        cm,
		upsertPresenceSQL:         upsertPresenceSQL,
		upsertPresenceFromSyncSQL: upsertPresenceFromSyncSQL,
		selectPresenceForUserSQL:  selectPresenceForUserSQL,
		selectMaxPresenceSQL:      selectMaxPresenceSQL,
		selectPresenceAfter:       selectPresenceAfter,
	}

	// Perform the migration
	err := cm.MigrateStrings(ctx, frame.MigrationPatch{
		Name:        "syncapi_presence_table_schema_001",
		Patch:       presenceSchema,
		RevertPatch: presenceSchemaRevert,
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// UpsertPresence creates/updates a presence status.
func (t *presenceTable) UpsertPresence(
	ctx context.Context,
	userID string,
	statusMsg *string,
	presence types.Presence,
	lastActiveTS spec.Timestamp,
	fromSync bool,
) (pos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, false)
	if fromSync {
		row := db.Raw(t.upsertPresenceFromSyncSQL, userID, presence, lastActiveTS).Row()
		err = row.Scan(&pos)
	} else {
		row := db.Raw(t.upsertPresenceSQL, userID, presence, statusMsg, lastActiveTS).Row()
		err = row.Scan(&pos)
	}
	return
}

// GetPresenceForUsers returns the current presence for a list of users.
// If the user doesn't have a presence status yet, it is omitted from the response.
func (t *presenceTable) GetPresenceForUsers(
	ctx context.Context,
	userIDs []string,
) ([]*types.PresenceInternal, error) {
	result := make([]*types.PresenceInternal, 0, len(userIDs))
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectPresenceForUserSQL, pq.Array(userIDs)).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "GetPresenceForUsers: rows.close() failed")

	for rows.Next() {
		presence := &types.PresenceInternal{}
		if err = rows.Scan(&presence.UserID, &presence.Presence, &presence.ClientFields.StatusMsg, &presence.LastActiveTS); err != nil {
			return nil, err
		}
		presence.ClientFields.Presence = presence.Presence.String()
		result = append(result, presence)
	}
	return result, rows.Err()
}

// GetMaxPresenceID returns the maximum presence ID
func (t *presenceTable) GetMaxPresenceID(ctx context.Context) (pos types.StreamPosition, err error) {
	db := t.cm.Connection(ctx, true)
	row := db.Raw(t.selectMaxPresenceSQL).Row()
	err = row.Scan(&pos)
	return
}

// GetPresenceAfter returns the changes presences after a given stream id
func (t *presenceTable) GetPresenceAfter(
	ctx context.Context,
	after types.StreamPosition,
	filter synctypes.EventFilter,
) (presences map[string]*types.PresenceInternal, err error) {
	presences = make(map[string]*types.PresenceInternal)
	db := t.cm.Connection(ctx, true)
	afterTS := spec.AsTimestamp(time.Now().Add(time.Minute * -5))
	rows, err := db.Raw(t.selectPresenceAfter, after, afterTS, filter.Limit).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "GetPresenceAfter: failed to close rows")
	for rows.Next() {
		qryRes := &types.PresenceInternal{}
		if err := rows.Scan(&qryRes.StreamPos, &qryRes.UserID, &qryRes.Presence, &qryRes.ClientFields.StatusMsg, &qryRes.LastActiveTS); err != nil {
			return nil, err
		}
		qryRes.ClientFields.Presence = qryRes.Presence.String()
		presences[qryRes.UserID] = qryRes
	}
	return presences, rows.Err()
}
