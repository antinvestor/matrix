// Copyright 2021 The Global.org Foundation C.I.C.
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
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/antinvestor/matrix/userapi/types"
)

const crossSigningSigsSchema = `
CREATE TABLE IF NOT EXISTS keyserver_cross_signing_sigs (
    origin_user_id TEXT NOT NULL,
	origin_key_id TEXT NOT NULL,
	target_user_id TEXT NOT NULL,
	target_key_id TEXT NOT NULL,
	signature TEXT NOT NULL,
	PRIMARY KEY (origin_user_id, origin_key_id, target_user_id, target_key_id)
);

CREATE INDEX IF NOT EXISTS keyserver_cross_signing_sigs_idx ON keyserver_cross_signing_sigs (origin_user_id, target_user_id, target_key_id);
`

const crossSigningSigsSchemaRevert = "DROP TABLE IF EXISTS keyserver_cross_signing_sigs CASCADE; DROP INDEX IF EXISTS keyserver_cross_signing_sigs_idx;"

// SQL: Select cross-signing signatures for a target
const selectCrossSigningSigsForTargetSQL = "SELECT origin_user_id, origin_key_id, signature FROM keyserver_cross_signing_sigs WHERE (origin_user_id = $1 OR origin_user_id = $2) AND target_user_id = $2 AND target_key_id = $3"

// SQL: Upsert (insert or update) cross-signing signatures for a target
const upsertCrossSigningSigsForTargetSQL = "INSERT INTO keyserver_cross_signing_sigs (origin_user_id, origin_key_id, target_user_id, target_key_id, signature) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (origin_user_id, origin_key_id, target_user_id, target_key_id) DO UPDATE SET signature = $5"

// SQL: Delete cross-signing signatures for a target
const deleteCrossSigningSigsForTargetSQL = "DELETE FROM keyserver_cross_signing_sigs WHERE target_user_id = $1 AND target_key_id = $2"

// crossSigningSigsTable implements tables.CrossSigningSigsTable using GORM and a connection manager.
type crossSigningSigsTable struct {
	cm *sqlutil.Connections

	selectCrossSigningSigsForTargetSQL string
	upsertCrossSigningSigsForTargetSQL string
	deleteCrossSigningSigsForTargetSQL string
}

// NewPostgresCrossSigningSigsTable returns a new CrossSigningSigsTable using the provided connection manager.
func NewPostgresCrossSigningSigsTable(cm *sqlutil.Connections) tables.CrossSigningSigs {
	return &crossSigningSigsTable{
		cm:                                 cm,
		selectCrossSigningSigsForTargetSQL: selectCrossSigningSigsForTargetSQL,
		upsertCrossSigningSigsForTargetSQL: upsertCrossSigningSigsForTargetSQL,
		deleteCrossSigningSigsForTargetSQL: deleteCrossSigningSigsForTargetSQL,
	}
}

// SelectCrossSigningSigsForTarget retrieves cross-signing signatures for a target user and key.
func (t *crossSigningSigsTable) SelectCrossSigningSigsForTarget(ctx context.Context, originUserID, targetUserID string, targetKeyID gomatrixserverlib.KeyID) (types.CrossSigningSigMap, error) {
	db := t.cm.Connection(ctx, true)
	rows, err := db.Raw(t.selectCrossSigningSigsForTargetSQL, originUserID, targetUserID, targetKeyID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "failed to close rows")
	r := types.CrossSigningSigMap{}
	for rows.Next() {
		var userID string
		var keyID gomatrixserverlib.KeyID
		var signature spec.Base64Bytes
		if err := rows.Scan(&userID, &keyID, &signature); err != nil {
			return nil, err
		}
		if _, ok := r[userID]; !ok {
			r[userID] = map[gomatrixserverlib.KeyID]spec.Base64Bytes{}
		}
		r[userID][keyID] = signature
	}
	return r, rows.Err()
}

// UpsertCrossSigningSigsForTarget inserts or updates cross-signing signatures for a target user and key.
func (t *crossSigningSigsTable) UpsertCrossSigningSigsForTarget(ctx context.Context, originUserID string, originKeyID gomatrixserverlib.KeyID, targetUserID string, targetKeyID gomatrixserverlib.KeyID, signature spec.Base64Bytes) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.upsertCrossSigningSigsForTargetSQL, originUserID, originKeyID, targetUserID, targetKeyID, signature)
	return result.Error
}

// DeleteCrossSigningSigsForTarget deletes cross-signing signatures for a target user and key.
func (t *crossSigningSigsTable) DeleteCrossSigningSigsForTarget(ctx context.Context, targetUserID string, targetKeyID gomatrixserverlib.KeyID) error {
	db := t.cm.Connection(ctx, false)
	result := db.Exec(t.deleteCrossSigningSigsForTargetSQL, targetUserID, targetKeyID)
	return result.Error
}
