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

package tables_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/relayapi/storage/postgres"
	"github.com/antinvestor/matrix/relayapi/storage/tables"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

const (
	testOrigin = spec.ServerName("kaer.morhen")
)

func mustCreateTransaction() gomatrixserverlib.Transaction {
	txn := gomatrixserverlib.Transaction{}
	txn.PDUs = []json.RawMessage{
		[]byte(`{"auth_events":[["$0ok8ynDp7kjc95e3:kaer.morhen",{"sha256":"sWCi6Ckp9rDimQON+MrUlNRkyfZ2tjbPbWfg2NMB18Q"}],["$LEwEu0kxrtu5fOiS:kaer.morhen",{"sha256":"1aKajq6DWHru1R1HJjvdWMEavkJJHGaTmPvfuERUXaA"}]],"content":{"body":"Test Message"},"depth":5,"event_id":"$gl2T9l3qm0kUbiIJ:kaer.morhen","hashes":{"sha256":"Qx3nRMHLDPSL5hBAzuX84FiSSP0K0Kju2iFoBWH4Za8"},"origin":"kaer.morhen","origin_server_ts":0,"prev_events":[["$UKNe10XzYzG0TeA9:kaer.morhen",{"sha256":"KtSRyMjt0ZSjsv2koixTRCxIRCGoOp6QrKscsW97XRo"}]],"room_id":"!roomid:kaer.morhen","sender":"@userid:kaer.morhen","signatures":{"kaer.morhen":{"ed25519:auto":"sqDgv3EG7ml5VREzmT9aZeBpS4gAPNIaIeJOwqjDhY0GPU/BcpX5wY4R7hYLrNe5cChgV+eFy/GWm1Zfg5FfDg"}},"type":"m.room.message"}`),
	}
	txn.Origin = testOrigin

	return txn
}

type RelayQueueJSONDatabase struct {
	Cm     *sqlutil.Connections
	Writer sqlutil.Writer
	Table  tables.RelayQueueJSON
}

func mustCreateQueueJSONTable(
	ctx context.Context,
	t *testing.T,
	_ test.DependancyOption,
) (database RelayQueueJSONDatabase, closeDb func()) {
	t.Helper()

	connStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString:   connStr,
		MaxOpenConnections: 10,
	}, sqlutil.NewExclusiveWriter())
	assert.NoError(t, err)
	var tab tables.RelayQueueJSON
	tab, err = postgres.NewPostgresRelayQueueJSONTable(ctx, db)
	assert.NoError(t, err)

	assert.NoError(t, err)

	database = RelayQueueJSONDatabase{
		Cm:     cm,
		Writer: sqlutil.NewDefaultWriter(),
		Table:  tab,
	}
	return database, closeDb
}

func TestShoudInsertTransaction(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db, closeDb := mustCreateQueueJSONTable(ctx, t, testOpts)
		defer closeDb()

		transaction := mustCreateTransaction()
		tx, err := json.Marshal(transaction)
		if err != nil {
			t.Fatalf("Invalid transaction: %s", err.Error())
		}

		_, err = db.Table.InsertQueueJSON(ctx, string(tx))
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}
	})
}

func TestShouldRetrieveInsertedTransaction(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db, closeDb := mustCreateQueueJSONTable(ctx, t, testOpts)
		defer closeDb()

		transaction := mustCreateTransaction()
		tx, err := json.Marshal(transaction)
		if err != nil {
			t.Fatalf("Invalid transaction: %s", err.Error())
		}

		nid, err := db.Table.InsertQueueJSON(ctx, string(tx))
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}

		var storedJSON map[int64][]byte
		_ = db.Writer.Do(ctx, db.Cm, func(ctx context.Context) error {
			storedJSON, err = db.Table.SelectQueueJSON(ctx, []int64{nid})
			return err
		})
		if err != nil {
			t.Fatalf("Failed retrieving transaction: %s", err.Error())
		}

		assert.Equal(t, 1, len(storedJSON))

		var storedTx gomatrixserverlib.Transaction
		err = json.Unmarshal(storedJSON[1], &storedTx)
		if err != nil {
			t.Fatalf("Invalid transaction: %s", err.Error())
		}

		assert.Equal(t, transaction, storedTx)
	})
}

func TestShouldDeleteTransaction(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db, closeDb := mustCreateQueueJSONTable(ctx, t, testOpts)
		defer closeDb()

		transaction := mustCreateTransaction()
		tx, err := json.Marshal(transaction)
		if err != nil {
			t.Fatalf("Invalid transaction: %s", err.Error())
		}

		nid, err := db.Table.InsertQueueJSON(ctx, string(tx))
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}

		storedJSON := map[int64][]byte{}
		_ = db.Writer.Do(ctx, db.Cm, func(ctx context.Context) error {
			err = db.Table.DeleteQueueJSON(ctx, []int64{nid})
			return err
		})
		if err != nil {
			t.Fatalf("Failed deleting transaction: %s", err.Error())
		}

		storedJSON = map[int64][]byte{}
		_ = db.Writer.Do(ctx, db.Cm, func(ctx context.Context) error {
			storedJSON, err = db.Table.SelectQueueJSON(ctx, []int64{nid})
			return err
		})
		if err != nil {
			t.Fatalf("Failed retrieving transaction: %s", err.Error())
		}

		assert.Equal(t, 0, len(storedJSON))
	})
}
