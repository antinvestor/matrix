package tables_test

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/storage/postgres"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
	ed255192 "golang.org/x/crypto/ed25519"
)

func mustCreateUserRoomKeysTable(t *testing.T, _ test.DependancyOption) (ctx context.Context, tab tables.UserRoomKeys, db *sql.DB, closeDb func()) {
	t.Helper()

	ctx = testrig.NewContext(t)
	connStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	db, err = sqlutil.Open(&config.DatabaseOptions{
		ConnectionString:   connStr,
		MaxOpenConnections: 10,
	}, sqlutil.NewExclusiveWriter())
	assert.NoError(t, err)
	err = postgres.CreateUserRoomKeysTable(ctx, db)
	assert.NoError(t, err)
	tab, err = postgres.PrepareUserRoomKeysTable(ctx, db)

	assert.NoError(t, err)

	return ctx, tab, db, closeDb
}

func TestUserRoomKeysTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, tab, db, closeDb := mustCreateUserRoomKeysTable(t, testOpts)
		defer closeDb()
		userNID := types.EventStateKeyNID(1)
		roomNID := types.RoomNID(1)
		_, key, err := ed25519.GenerateKey(nil)
		assert.NoError(t, err)

		err = sqlutil.WithTransaction(db, func(txn *sql.Tx) error {
			var gotKey, key2, key3 ed25519.PrivateKey
			var pubKey ed25519.PublicKey
			gotKey, err = tab.InsertUserRoomPrivatePublicKey(ctx, txn, userNID, roomNID, key)
			assert.NoError(t, err)
			assert.Equal(t, gotKey, key)

			// again, this shouldn't result in an error, but return the existing key
			_, key2, err = ed25519.GenerateKey(nil)
			assert.NoError(t, err)
			gotKey, err = tab.InsertUserRoomPrivatePublicKey(ctx, txn, userNID, roomNID, key2)
			assert.NoError(t, err)
			assert.Equal(t, gotKey, key)

			// add another user
			_, key3, err = ed25519.GenerateKey(nil)
			assert.NoError(t, err)
			userNID2 := types.EventStateKeyNID(2)
			_, err = tab.InsertUserRoomPrivatePublicKey(ctx, txn, userNID2, roomNID, key3)
			assert.NoError(t, err)

			gotKey, err = tab.SelectUserRoomPrivateKey(ctx, txn, userNID, roomNID)
			assert.NoError(t, err)
			assert.Equal(t, key, gotKey)
			pubKey, err = tab.SelectUserRoomPublicKey(ctx, txn, userNID, roomNID)
			assert.NoError(t, err)
			assert.Equal(t, key.Public(), pubKey)

			// try to update an existing key, this should only be done for users NOT on this homeserver
			var gotPubKey ed25519.PublicKey
			gotPubKey, err = tab.InsertUserRoomPublicKey(ctx, txn, userNID, roomNID, key2.Public().(ed25519.PublicKey))
			assert.NoError(t, err)
			assert.Equal(t, key2.Public(), gotPubKey)

			// Key doesn't exist
			gotKey, err = tab.SelectUserRoomPrivateKey(ctx, txn, userNID, 2)
			assert.NoError(t, err)
			assert.Nil(t, gotKey)
			pubKey, err = tab.SelectUserRoomPublicKey(ctx, txn, userNID, 2)
			assert.NoError(t, err)
			assert.Nil(t, pubKey)

			// query user NIDs for senderKeys
			var gotKeys map[string]types.UserRoomKeyPair
			query := map[types.RoomNID][]ed25519.PublicKey{
				roomNID:          {key2.Public().(ed25519.PublicKey), key3.Public().(ed25519.PublicKey)},
				types.RoomNID(2): {key.Public().(ed25519.PublicKey), key3.Public().(ed25519.PublicKey)}, // doesn't exist
			}
			gotKeys, err = tab.BulkSelectUserNIDs(ctx, txn, query)
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
			gotPublicKey, err = tab.InsertUserRoomPublicKey(ctx, txn, userNID, 2, key4)
			assert.NoError(t, err)
			assert.Equal(t, key4, gotPublicKey)

			return nil
		})
		assert.NoError(t, err)

	})
}
