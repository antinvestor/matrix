// Copyright 2020 The Matrix.org Foundation C.I.C.
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

package query

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
)

// used to implement RoomserverInternalAPIEventDB to test getAuthChain
type getEventDB struct {
	eventMap map[string]gomatrixserverlib.PDU
}

func createEventDB() *getEventDB {
	return &getEventDB{
		eventMap: make(map[string]gomatrixserverlib.PDU),
	}
}

// Adds a fake event to the storage with given auth events.
func (db *getEventDB) addFakeEvent(eventID string, authIDs []string) error {
	authEvents := make([]any, 0, len(authIDs))
	for _, authID := range authIDs {
		authEvents = append(authEvents, []any{authID, struct{}{}})
	}
	builder := map[string]interface{}{
		"event_id":    eventID,
		"room_id":     "!room:a",
		"auth_events": authEvents,
	}

	eventJSON, err := json.Marshal(&builder)
	if err != nil {
		return err
	}

	event, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV1).NewEventFromTrustedJSON(
		eventJSON, false,
	)
	if err != nil {
		return err
	}

	db.eventMap[eventID] = event

	return nil
}

// Adds multiple events at once, each entry in the map is an eventID and set of
// auth events that are converted to an event and added.
func (db *getEventDB) addFakeEvents(graph map[string][]string) error {
	for eventID, authIDs := range graph {
		err := db.addFakeEvent(eventID, authIDs)
		if err != nil {
			return err
		}
	}

	return nil
}

// EventsFromIDs implements RoomserverInternalAPIEventDB
func (db *getEventDB) EventsFromIDs(_ context.Context, _ *types.RoomInfo, eventIDs []string) (res []types.Event, err error) {
	for _, evID := range eventIDs {
		res = append(res, types.Event{
			EventNID: 0,
			PDU:      db.eventMap[evID],
		})
	}

	return
}

func TestGetAuthChainSingle(t *testing.T) {
	ctx := testrig.NewContext(t)
	db := createEventDB()

	err := db.addFakeEvents(map[string][]string{
		"a": {},
		"b": {"a"},
		"c": {"a", "b"},
		"d": {"b", "c"},
		"e": {"a", "d"},
	})

	if err != nil {
		t.Fatalf("Failed to add events to db: %v", err)
	}

	result, err := GetAuthChain(ctx, db.EventsFromIDs, nil, []string{"e"})
	if err != nil {
		t.Fatalf("getAuthChain failed: %v", err)
	}

	var returnedIDs []string
	for _, event := range result {
		returnedIDs = append(returnedIDs, event.EventID())
	}

	expectedIDs := []string{"a", "b", "c", "d", "e"}

	if !test.UnsortedStringSliceEqual(expectedIDs, returnedIDs) {
		t.Fatalf("returnedIDs got '%v', expected '%v'", returnedIDs, expectedIDs)
	}
}

func TestGetAuthChainMultiple(t *testing.T) {

	ctx := testrig.NewContext(t)
	db := createEventDB()

	err := db.addFakeEvents(map[string][]string{
		"a": {},
		"b": {"a"},
		"c": {"a", "b"},
		"d": {"b", "c"},
		"e": {"a", "d"},
		"f": {"a", "b", "c"},
	})

	if err != nil {
		t.Fatalf("Failed to add events to db: %v", err)
	}

	result, err := GetAuthChain(ctx, db.EventsFromIDs, nil, []string{"e", "f"})
	if err != nil {
		t.Fatalf("getAuthChain failed: %v", err)
	}

	var returnedIDs []string
	for _, event := range result {
		returnedIDs = append(returnedIDs, event.EventID())
	}

	expectedIDs := []string{"a", "b", "c", "d", "e", "f"}

	if !test.UnsortedStringSliceEqual(expectedIDs, returnedIDs) {
		t.Fatalf("returnedIDs got '%v', expected '%v'", returnedIDs, expectedIDs)
	}
}

func mustCreateDatabase(ctx context.Context, t *testing.T, _ test.DependancyOption) (storage.Database, func()) {

	conStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}

	cacheConnStr, closeCache, err := test.PrepareRedisDataSourceConnection(context.TODO())
	if err != nil {
		t.Fatalf("Could not create redis container %s", err)
	}

	caches, err := caching.NewCache(&config.CacheOptions{
		ConnectionString: cacheConnStr,
	})
	if err != nil {
		t.Fatalf("Could not create redis container %s", err)
	}

	cm := sqlutil.NewConnectionManager(ctx, config.DatabaseOptions{ConnectionString: conStr})
	db, err := storage.Open(ctx, cm, &config.DatabaseOptions{ConnectionString: conStr}, caches)
	if err != nil {
		t.Fatalf("failed to create Database: %v", err)
	}
	return db, func() {
		closeCache()
		closeDb()
	}
}

func TestCurrentEventIsNil(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx := testrig.NewContext(t)
		db, closeDb := mustCreateDatabase(ctx, t, testOpts)
		defer closeDb()
		querier := Queryer{
			DB: db,
		}

		roomID, _ := spec.NewRoomID("!room:server")
		event, _ := querier.CurrentStateEvent(ctx, *roomID, spec.MRoomMember, "@user:server")
		if event != nil {
			t.Fatal("Event should equal nil, most likely this is failing because the interface type is not nil, but the value is.")
		}
	})
}
