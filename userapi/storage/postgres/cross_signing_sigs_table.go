// Copyright 2021 The Matrix.org Foundation C.I.C.
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
	"fmt"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/antinvestor/matrix/userapi/types"
)

var crossSigningSigsSchema = `
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

// SQL query constants for cross-signing signatures operations
const (
	selectCrossSigningSigsForTargetSQL = "SELECT origin_user_id, origin_key_id, signature FROM keyserver_cross_signing_sigs" +
		" WHERE (origin_user_id = $1 OR origin_user_id = $2) AND target_user_id = $2 AND target_key_id = $3"

	upsertCrossSigningSigsForTargetSQL = "INSERT INTO keyserver_cross_signing_sigs (origin_user_id, origin_key_id, target_user_id, target_key_id, signature)" +
		" VALUES($1, $2, $3, $4, $5)" +
		" ON CONFLICT (origin_user_id, origin_key_id, target_user_id, target_key_id) DO UPDATE SET signature = $5"

	deleteCrossSigningSigsForTargetSQL = "DELETE FROM keyserver_cross_signing_sigs WHERE target_user_id=$1 AND target_key_id=$2"
)

type crossSigningSigsTable struct {
	cm                                  *sqlutil.Connections
	selectCrossSigningSigsForTargetStmt string
	upsertCrossSigningSigsForTargetStmt string
	deleteCrossSigningSigsForTargetStmt string
}

func NewPostgresCrossSigningSigsTable(ctx context.Context, cm *sqlutil.Connections) (tables.CrossSigningSigs, error) {
	// Initialize schema
	db := cm.Connection(ctx, false)
	if err := db.Exec(crossSigningSigsSchema).Error; err != nil {
		return nil, err
	}

	// Get SQL Cm for migrations
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	m := sqlutil.NewMigrator(sqlDB)
	if err = m.Up(ctx); err != nil {
		return nil, err
	}

	// Initialize table with SQL statements
	t := &crossSigningSigsTable{
		cm:                                  cm,
		selectCrossSigningSigsForTargetStmt: selectCrossSigningSigsForTargetSQL,
		upsertCrossSigningSigsForTargetStmt: upsertCrossSigningSigsForTargetSQL,
		deleteCrossSigningSigsForTargetStmt: deleteCrossSigningSigsForTargetSQL,
	}

	return t, nil
}

func (s *crossSigningSigsTable) SelectCrossSigningSigsForTarget(
	ctx context.Context, originUserID, targetUserID string, targetKeyID gomatrixserverlib.KeyID,
) (r types.CrossSigningSigMap, err error) {
	db := s.cm.Connection(ctx, true)

	rows, err := db.Raw(s.selectCrossSigningSigsForTargetStmt, originUserID, targetUserID, targetKeyID).Rows()
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectCrossSigningSigsForTarget: rows.close() failed")

	r = types.CrossSigningSigMap{}
	for rows.Next() {
		var userID string
		var keyID gomatrixserverlib.KeyID
		var signature spec.Base64Bytes
		if err = rows.Scan(&userID, &keyID, &signature); err != nil {
			return nil, err
		}
		if _, ok := r[userID]; !ok {
			r[userID] = map[gomatrixserverlib.KeyID]spec.Base64Bytes{}
		}
		r[userID][keyID] = signature
	}
	err = rows.Err()
	return
}

func (s *crossSigningSigsTable) UpsertCrossSigningSigsForTarget(
	ctx context.Context,
	originUserID string, originKeyID gomatrixserverlib.KeyID,
	targetUserID string, targetKeyID gomatrixserverlib.KeyID,
	signature spec.Base64Bytes,
) error {
	db := s.cm.Connection(ctx, false)

	err := db.Exec(s.upsertCrossSigningSigsForTargetStmt,
		originUserID, originKeyID, targetUserID, targetKeyID, signature).Error
	if err != nil {
		return fmt.Errorf("failed to upsert cross signing signatures: %w", err)
	}
	return nil
}

func (s *crossSigningSigsTable) DeleteCrossSigningSigsForTarget(
	ctx context.Context,
	targetUserID string, targetKeyID gomatrixserverlib.KeyID,
) error {
	db := s.cm.Connection(ctx, false)

	err := db.Exec(s.deleteCrossSigningSigsForTargetStmt, targetUserID, targetKeyID).Error
	if err != nil {
		return fmt.Errorf("failed to delete cross signing signatures: %w", err)
	}
	return nil
}
