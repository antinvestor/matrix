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
	"fmt"
	"github.com/pitabwire/frame"
	"testing"
	"time"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/relayapi/storage/postgres"
	"github.com/antinvestor/matrix/relayapi/storage/tables"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

type RelayQueueDatabase struct {
	Cm     *sqlutil.Connections
	Writer sqlutil.Writer
	Table  tables.RelayQueue
}

func mustCreateQueueTable(
	ctx context.Context,
	svc *frame.Service,
	t *testing.T,
	_ test.DependancyOption,
) (database RelayQueueDatabase) {
	t.Helper()

	cm := sqlutil.NewConnectionManager(svc)

	tab, err := postgres.NewPostgresRelayQueueTable(ctx, cm)
	assert.NoError(t, err)

	database = RelayQueueDatabase{
		Cm:     cm,
		Writer: sqlutil.NewDefaultWriter(),
		Table:  tab,
	}
	return database
}

func TestShoudInsertQueueTransaction(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateQueueTable(ctx, svc, t, testOpts)

		transactionID := gomatrixserverlib.TransactionID(fmt.Sprintf("%d", time.Now().UnixNano()))
		serverName := spec.ServerName("domain")
		nid := int64(1)
		err := db.Table.InsertQueueEntry(ctx, transactionID, serverName, nid)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}
	})
}

func TestShouldRetrieveInsertedQueueTransaction(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateQueueTable(ctx, svc, t, testOpts)

		transactionID := gomatrixserverlib.TransactionID(fmt.Sprintf("%d", time.Now().UnixNano()))
		serverName := spec.ServerName("domain")
		nid := int64(1)

		err := db.Table.InsertQueueEntry(ctx, transactionID, serverName, nid)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}

		retrievedNids, err := db.Table.SelectQueueEntries(ctx, serverName, 10)
		if err != nil {
			t.Fatalf("Failed retrieving transaction: %s", err.Error())
		}

		assert.Equal(t, nid, retrievedNids[0])
		assert.Equal(t, 1, len(retrievedNids))
	})
}

func TestShouldRetrieveOldestInsertedQueueTransaction(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateQueueTable(ctx, svc, t, testOpts)

		transactionID := gomatrixserverlib.TransactionID(fmt.Sprintf("%d", time.Now().UnixNano()))
		serverName := spec.ServerName("domain")
		nid := int64(2)
		err := db.Table.InsertQueueEntry(ctx, transactionID, serverName, nid)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}

		transactionID = gomatrixserverlib.TransactionID(fmt.Sprintf("%d", time.Now().UnixNano()))
		serverName = spec.ServerName("domain")
		oldestNID := int64(1)
		err = db.Table.InsertQueueEntry(ctx, transactionID, serverName, oldestNID)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}

		retrievedNids, err := db.Table.SelectQueueEntries(ctx, serverName, 1)
		if err != nil {
			t.Fatalf("Failed retrieving transaction: %s", err.Error())
		}

		assert.Equal(t, oldestNID, retrievedNids[0])
		assert.Equal(t, 1, len(retrievedNids))

		retrievedNids, err = db.Table.SelectQueueEntries(ctx, serverName, 10)
		if err != nil {
			t.Fatalf("Failed retrieving transaction: %s", err.Error())
		}

		assert.Equal(t, oldestNID, retrievedNids[0])
		assert.Equal(t, nid, retrievedNids[1])
		assert.Equal(t, 2, len(retrievedNids))
	})
}

func TestShouldDeleteQueueTransaction(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateQueueTable(ctx, svc, t, testOpts)

		transactionID := gomatrixserverlib.TransactionID(fmt.Sprintf("%d", time.Now().UnixNano()))
		serverName := spec.ServerName("domain")
		nid := int64(1)

		err := db.Table.InsertQueueEntry(ctx, transactionID, serverName, nid)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}

		_ = db.Writer.Do(ctx, db.Cm, func(ctx context.Context) error {
			err = db.Table.DeleteQueueEntries(ctx, serverName, []int64{nid})
			return err
		})
		if err != nil {
			t.Fatalf("Failed deleting transaction: %s", err.Error())
		}

		count, err := db.Table.SelectQueueEntryCount(ctx, serverName)
		if err != nil {
			t.Fatalf("Failed retrieving transaction count: %s", err.Error())
		}
		assert.Equal(t, int64(0), count)
	})
}

func TestShouldDeleteOnlySpecifiedQueueTransaction(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		db := mustCreateQueueTable(ctx, svc, t, testOpts)

		transactionID := gomatrixserverlib.TransactionID(fmt.Sprintf("%d", time.Now().UnixNano()))
		serverName := spec.ServerName("domain")
		nid := int64(1)
		transactionID2 := gomatrixserverlib.TransactionID(fmt.Sprintf("%d2", time.Now().UnixNano()))
		serverName2 := spec.ServerName("domain2")
		nid2 := int64(2)
		transactionID3 := gomatrixserverlib.TransactionID(fmt.Sprintf("%d3", time.Now().UnixNano()))

		err := db.Table.InsertQueueEntry(ctx, transactionID, serverName, nid)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}
		err = db.Table.InsertQueueEntry(ctx, transactionID2, serverName2, nid)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}
		err = db.Table.InsertQueueEntry(ctx, transactionID3, serverName, nid2)
		if err != nil {
			t.Fatalf("Failed inserting transaction: %s", err.Error())
		}

		_ = db.Writer.Do(ctx, db.Cm, func(ctx context.Context) error {
			err = db.Table.DeleteQueueEntries(ctx, serverName, []int64{nid})
			return err
		})
		if err != nil {
			t.Fatalf("Failed deleting transaction: %s", err.Error())
		}

		count, err := db.Table.SelectQueueEntryCount(ctx, serverName)
		if err != nil {
			t.Fatalf("Failed retrieving transaction count: %s", err.Error())
		}
		assert.Equal(t, int64(1), count)

		count, err = db.Table.SelectQueueEntryCount(ctx, serverName2)
		if err != nil {
			t.Fatalf("Failed retrieving transaction count: %s", err.Error())
		}
		assert.Equal(t, int64(1), count)
	})
}
