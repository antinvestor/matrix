package tables_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/pitabwire/frame"
	"github.com/stretchr/testify/assert"
)

func mustCreateEventStateKeysTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) tables.EventStateKeys {
	t.Helper()

	cm := sqlutil.NewConnectionManager(svc)
	var tab tables.EventStateKeys
	tab, err := postgres.NewPostgresEventStateKeysTable(ctx, cm)
	assert.NoError(t, err)

	err = cm.Migrate(ctx)
	if err != nil {
		t.Fatalf("failed to migrate table: %s", err)
	}

	return tab
}

func Test_EventStateKeysTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		tab := mustCreateEventStateKeysTable(ctx, svc, t, testOpts)

		var stateKeyNID, gotEventStateKey types.EventStateKeyNID
		var err error
		// create some dummy data
		for i := 0; i < 10; i++ {
			stateKey := fmt.Sprintf("@user%d:localhost", i)
			stateKeyNID, err = tab.InsertEventStateKeyNID(ctx, stateKey)
			assert.NoError(t, err)
			gotEventStateKey, err = tab.SelectEventStateKeyNID(ctx, stateKey)
			assert.NoError(t, err)
			assert.Equal(t, stateKeyNID, gotEventStateKey)
		}
		// This should fail, since @user0:localhost already exists
		stateKey := fmt.Sprintf("@user%d:localhost", 0)
		_, err = tab.InsertEventStateKeyNID(ctx, stateKey)
		assert.Error(t, err)

		stateKeyNIDsMap, err := tab.BulkSelectEventStateKeyNID(ctx, []string{"@user0:localhost", "@user1:localhost"})
		assert.NoError(t, err)
		wantStateKeyNIDs := make([]types.EventStateKeyNID, 0, len(stateKeyNIDsMap))
		for _, nid := range stateKeyNIDsMap {
			wantStateKeyNIDs = append(wantStateKeyNIDs, nid)
		}
		stateKeyNIDs, err := tab.BulkSelectEventStateKey(ctx, wantStateKeyNIDs)
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
