package tables_test

import (
	"context"
	"fmt"
	"github.com/antinvestor/matrix/test/testrig"
	"testing"

	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

func mustCreateEventStateKeysTable(ctx context.Context, t *testing.T, dep test.DependancyOption) (tables.EventStateKeys, func()) {
	t.Helper()

	db, closeDb := migrateDatabase(ctx, t, dep)

	tab, err := postgres.NewPostgresEventStateKeysTable(ctx, db)

	assert.NoError(t, err)

	return tab, closeDb
}

func Test_EventStateKeysTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx := testrig.NewContext(t)
		tab, closeDb := mustCreateEventStateKeysTable(ctx, t, testOpts)
		defer closeDb()

		var stateKeyNID, gotEventStateKey types.EventStateKeyNID
		var err error
		// create some dummy data
		for i := 0; i < 10; i++ {
			stateKey := fmt.Sprintf("@user%d:localhost", i)
			stateKeyNID, err = tab.InsertEventStateKeyNID(ctx, nil, stateKey)
			assert.NoError(t, err)
			gotEventStateKey, err = tab.SelectEventStateKeyNID(ctx, nil, stateKey)
			assert.NoError(t, err)
			assert.Equal(t, stateKeyNID, gotEventStateKey)
		}
		// This should fail, since @user0:localhost already exists
		stateKey := fmt.Sprintf("@user%d:localhost", 0)
		_, err = tab.InsertEventStateKeyNID(ctx, nil, stateKey)
		assert.Error(t, err)

		stateKeyNIDsMap, err := tab.BulkSelectEventStateKeyNID(ctx, nil, []string{"@user0:localhost", "@user1:localhost"})
		assert.NoError(t, err)
		wantStateKeyNIDs := make([]types.EventStateKeyNID, 0, len(stateKeyNIDsMap))
		for _, nid := range stateKeyNIDsMap {
			wantStateKeyNIDs = append(wantStateKeyNIDs, nid)
		}
		stateKeyNIDs, err := tab.BulkSelectEventStateKey(ctx, nil, wantStateKeyNIDs)
		assert.NoError(t, err)
		// verify that BulkSelectEventStateKeyNID and BulkSelectEventStateKey return the same values
		for userID, nid := range stateKeyNIDsMap {
			if v, ok := stateKeyNIDs[nid]; ok {
				assert.Equal(t, v, userID)
			} else {
				t.Fatalf("unable to find %d in result set", nid)
			}
		}
	})
}
