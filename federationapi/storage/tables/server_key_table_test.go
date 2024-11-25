package tables_test

import (
	"context"
	"testing"
	"time"

	"github.com/antinvestor/matrix/federationapi/storage/postgres"
	"github.com/antinvestor/matrix/federationapi/storage/tables"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
)

func mustCreateServerKeyDB(t *testing.T, testOpts test.DependancyOption) (tables.FederationServerSigningKeys, func()) {
	ctx := context.TODO()
	connStr, closeDb, err := test.PrepareDatabaseDSConnection(ctx)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, sqlutil.NewExclusiveWriter())
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	var tab tables.FederationServerSigningKeys
	tab, err = postgres.NewPostgresServerSigningKeysTable(db)

	if err != nil {
		t.Fatalf("failed to create table: %s", err)
	}
	return tab, closeDb
}

func TestServerKeysTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, cancel := context.WithCancel(context.Background())
		tab, closeKeyDb := mustCreateServerKeyDB(t, testOpts)
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
