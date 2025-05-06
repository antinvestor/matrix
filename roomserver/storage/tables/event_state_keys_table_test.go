package tables_test

import (
	"fmt"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

func mustCreateEventStateKeysTable(t *testing.T, _ test.DependancyOption) (tables.EventStateKeys, func()) {
	t.Helper()

	ctx, svc, cfg := testrig.Init(t, testOpts)
	defer svc.Stop(ctx)

	connStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString:   connStr,
		MaxOpenConnections: 10,
	}, sqlutil.NewExclusiveWriter())
	assert.NoError(t, err)
	var tab tables.EventStateKeys
	err = postgres.CreateEventStateKeysTable(ctx, db)
	assert.NoError(t, err)
	tab, err = postgres.PrepareEventStateKeysTable(ctx, db)

	assert.NoError(t, err)

	return tab, closeDb
}

func Test_EventStateKeysTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		tab, closeDb := mustCreateEventStateKeysTable(t, testOpts)
		defer closeDb()
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
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
