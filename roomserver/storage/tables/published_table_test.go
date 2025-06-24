package tables_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/pitabwire/frame"
	"github.com/stretchr/testify/assert"
)

func mustCreatePublishedTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) tables.Published {
	t.Helper()

	cm := sqlutil.NewConnectionManager(svc)
	tab, err := postgres.NewPostgresPublishedTable(ctx, cm)
	assert.NoError(t, err)

	err = cm.Migrate(ctx)
	if err != nil {
		t.Fatalf("failed to migrate table: %s", err)
	}

	return tab
}

func TestPublishedTable(t *testing.T) {

	alice := test.NewUser(t)

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		tab := mustCreatePublishedTable(ctx, svc, t, testOpts)

		// Publish some rooms
		publishedRooms := []string{}
		asID := ""
		nwID := ""
		for i := 0; i < 10; i++ {
			room := test.NewRoom(t, alice)
			published := i%2 == 0
			err := tab.UpsertRoomPublished(ctx, room.ID, asID, nwID, published)
			assert.NoError(t, err)
			if published {
				publishedRooms = append(publishedRooms, room.ID)
			}
			publishedRes, err := tab.SelectPublishedFromRoomID(ctx, room.ID)
			assert.NoError(t, err)
			assert.Equal(t, published, publishedRes)
		}
		sort.Strings(publishedRooms)

		// check that we get the expected published rooms
		roomIDs, err := tab.SelectAllPublishedRooms(ctx, "", true, true)
		assert.NoError(t, err)
		assert.Equal(t, publishedRooms, roomIDs)

		// test an actual upsert
		room := test.NewRoom(t, alice)
		err = tab.UpsertRoomPublished(ctx, room.ID, asID, nwID, true)
		assert.NoError(t, err)
		err = tab.UpsertRoomPublished(ctx, room.ID, asID, nwID, false)
		assert.NoError(t, err)
		// should now be false, due to the upsert
		publishedRes, err := tab.SelectPublishedFromRoomID(ctx, room.ID)
		assert.NoError(t, err)
		assert.False(t, publishedRes, fmt.Sprintf("expected room %s to be unpublished", room.ID))

		// network specific test
		nwID = "irc"
		room = test.NewRoom(t, alice)
		err = tab.UpsertRoomPublished(ctx, room.ID, asID, nwID, true)
		assert.NoError(t, err)
		publishedRooms = append(publishedRooms, room.ID)
		sort.Strings(publishedRooms)
		// should only return the room for network "irc"
		allNWPublished, err := tab.SelectAllPublishedRooms(ctx, nwID, true, true)
		assert.NoError(t, err)
		assert.Equal(t, []string{room.ID}, allNWPublished)

		// check that we still get all published rooms regardless networkID
		roomIDs, err = tab.SelectAllPublishedRooms(ctx, "", true, true)
		assert.NoError(t, err)
		assert.Equal(t, publishedRooms, roomIDs)
	})
}
