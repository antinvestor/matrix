package tables_test

import (
	"context"
	"testing"
	"time"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/storage/postgres"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/test"
	"github.com/stretchr/testify/assert"
)

func mustCreateServerKeyDB(ctx context.Context, t *testing.T, dep test.DependancyOption) (tables.FederationServerSigningKeys, func()) {
	db, closeDb := migrateDatabase(ctx, t, dep)
	tab, err := postgres.NewPostgresServerSigningKeysTable(ctx, db)

	if err != nil {
		t.Fatalf("failed to create table: %s", err)
	}
	return tab, closeDb
}

func TestServerKeysTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		ctx := testrig.NewContext(t)
		ctx, cancel := context.WithCancel(ctx)
		tab, closeKeyDb := mustCreateServerKeyDB(ctx, t, testOpts)
		t.Cleanup(func() {
			closeKeyDb()
			cancel()
		})

		req := gomatrixserverlib.PublicKeyLookupRequest{
			ServerName: "localhost",
			KeyID:      "ed25519:test",
		}
		expectedTimestamp := spec.AsTimestamp(time.Now().Add(time.Hour))
		res := gomatrixserverlib.PublicKeyLookupResult{
			VerifyKey:    gomatrixserverlib.VerifyKey{Key: make(spec.Base64Bytes, 0)},
			ExpiredTS:    0,
			ValidUntilTS: expectedTimestamp,
		}

		// Insert the key
		err := tab.UpsertServerKeys(ctx, nil, req, res)
		assert.NoError(t, err)

		selectKeys := map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp{
			req: spec.AsTimestamp(time.Now()),
		}
		gotKeys, err := tab.BulkSelectServerKeys(ctx, nil, selectKeys)
		assert.NoError(t, err)

		// Now we should have a key for the req above
		assert.NotNil(t, gotKeys[req])
		assert.Equal(t, res, gotKeys[req])

		// "Expire" the key by setting ExpireTS to a non-zero value and ValidUntilTS to 0
		expectedTimestamp = spec.AsTimestamp(time.Now())
		res.ExpiredTS = expectedTimestamp
		res.ValidUntilTS = 0

		// Update the key
		err = tab.UpsertServerKeys(ctx, nil, req, res)
		assert.NoError(t, err)

		gotKeys, err = tab.BulkSelectServerKeys(ctx, nil, selectKeys)
		assert.NoError(t, err)

		// The key should be expired
		assert.NotNil(t, gotKeys[req])
		assert.Equal(t, res, gotKeys[req])

		// Upsert a different key to validate querying multiple keys
		req2 := gomatrixserverlib.PublicKeyLookupRequest{
			ServerName: "notlocalhost",
			KeyID:      "ed25519:test2",
		}
		expectedTimestamp2 := spec.AsTimestamp(time.Now().Add(time.Hour))
		res2 := gomatrixserverlib.PublicKeyLookupResult{
			VerifyKey:    gomatrixserverlib.VerifyKey{Key: make(spec.Base64Bytes, 0)},
			ExpiredTS:    0,
			ValidUntilTS: expectedTimestamp2,
		}

		err = tab.UpsertServerKeys(ctx, nil, req2, res2)
		assert.NoError(t, err)

		// Select multiple keys
		selectKeys[req2] = spec.AsTimestamp(time.Now())

		gotKeys, err = tab.BulkSelectServerKeys(ctx, nil, selectKeys)
		assert.NoError(t, err)

		// We now should receive two keys, one of which is expired
		assert.Equal(t, 2, len(gotKeys))
		assert.Equal(t, res2, gotKeys[req2])
		assert.Equal(t, res, gotKeys[req])
	})
}
