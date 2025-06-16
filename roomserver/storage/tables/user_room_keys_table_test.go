package tables_test

import (
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/pitabwire/frame"
	"github.com/stretchr/testify/assert"
	ed255192 "golang.org/x/crypto/ed25519"
)

func mustCreateUserRoomKeysTable(ctx context.Context, svc *frame.Service, t *testing.T, _ test.DependancyOption) (cm sqlutil.ConnectionManager, tab tables.UserRoomKeys) {
	t.Helper()

	cm = sqlutil.NewConnectionManager(svc)

	tab, err := postgres.NewPostgresUserRoomKeysTable(ctx, cm)
	assert.NoError(t, err)

	err = cm.Migrate(ctx)
	if err != nil {
		t.Fatalf("failed to migrate table: %s", err)
	}

	return cm, tab
}

func TestUserRoomKeysTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx, svc, _ := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)

		cm, tab := mustCreateUserRoomKeysTable(ctx, svc, t, testOpts)

		userNID := types.EventStateKeyNID(1)
		roomNID := types.RoomNID(1)
		_, key, err := ed25519.GenerateKey(nil)
		assert.NoError(t, err)

		err = cm.Do(ctx, func(ctx context.Context) error {
			var gotKey, key2, key3 ed25519.PrivateKey
			var pubKey ed25519.PublicKey
			gotKey, err = tab.InsertUserRoomPrivatePublicKey(ctx, userNID, roomNID, key)
			assert.NoError(t, err)
			assert.Equal(t, gotKey, key)

			// again, this shouldn't result in an error, but return the existing key
			_, key2, err = ed25519.GenerateKey(nil)
			assert.NoError(t, err)
			gotKey, err = tab.InsertUserRoomPrivatePublicKey(ctx, userNID, roomNID, key2)
			assert.NoError(t, err)
			assert.Equal(t, gotKey, key)

			// add another user
			_, key3, err = ed25519.GenerateKey(nil)
			assert.NoError(t, err)
			userNID2 := types.EventStateKeyNID(2)
			_, err = tab.InsertUserRoomPrivatePublicKey(ctx, userNID2, roomNID, key3)
			assert.NoError(t, err)

			gotKey, err = tab.SelectUserRoomPrivateKey(ctx, userNID, roomNID)
			assert.NoError(t, err)
			assert.Equal(t, key, gotKey)
			pubKey, err = tab.SelectUserRoomPublicKey(ctx, userNID, roomNID)
			assert.NoError(t, err)
			assert.Equal(t, key.Public(), pubKey)

			// try to update an existing key, this should only be done for users NOT on this homeserver
			var gotPubKey ed25519.PublicKey
			gotPubKey, err = tab.InsertUserRoomPublicKey(ctx, userNID, roomNID, key2.Public().(ed25519.PublicKey))
			assert.NoError(t, err)
			assert.Equal(t, key2.Public(), gotPubKey)

			// Key doesn't exist
			gotKey, err = tab.SelectUserRoomPrivateKey(ctx, userNID, 2)
			assert.NoError(t, err)
			assert.Nil(t, gotKey)
			pubKey, err = tab.SelectUserRoomPublicKey(ctx, userNID, 2)
			assert.NoError(t, err)
			assert.Nil(t, pubKey)

			// query user NIDs for senderKeys
			var gotKeys map[string]types.UserRoomKeyPair
			query := map[types.RoomNID][]ed25519.PublicKey{
				roomNID:          {key2.Public().(ed25519.PublicKey), key3.Public().(ed25519.PublicKey)},
				types.RoomNID(2): {key.Public().(ed25519.PublicKey), key3.Public().(ed25519.PublicKey)}, // doesn't exist
			}
			gotKeys, err = tab.BulkSelectUserNIDs(ctx, query)
			assert.NoError(t, err)
			assert.NotNil(t, gotKeys)

			wantKeys := map[string]types.UserRoomKeyPair{
				string(spec.Base64Bytes(key2.Public().(ed25519.PublicKey)).Encode()): {RoomNID: roomNID, EventStateKeyNID: userNID},
				string(spec.Base64Bytes(key3.Public().(ed25519.PublicKey)).Encode()): {RoomNID: roomNID, EventStateKeyNID: userNID2},
			}
			assert.Equal(t, wantKeys, gotKeys)

			// insert key that came in over federation
			var gotPublicKey, key4 ed255192.PublicKey
			key4, _, err = ed25519.GenerateKey(nil)
			assert.NoError(t, err)
			gotPublicKey, err = tab.InsertUserRoomPublicKey(ctx, userNID, 2, key4)
			assert.NoError(t, err)
			assert.Equal(t, key4, gotPublicKey)

			return nil
		})
		assert.NoError(t, err)

	})
}
