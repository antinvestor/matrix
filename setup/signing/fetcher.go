package signing

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
)

const KeyID = "ed25519:matrix-yggdrasil"

type YggdrasilKeys struct {
}

func (f *YggdrasilKeys) KeyRing() *gomatrixserverlib.KeyRing {
	return &gomatrixserverlib.KeyRing{
		KeyDatabase: f,
	}
}

func (f *YggdrasilKeys) FetchKeys(
	ctx context.Context,
	requests map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp,
) (map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult, error) {
	res := make(map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult)
	for req := range requests {
		if req.KeyID != KeyID {
			return nil, fmt.Errorf("FetchKeys: cannot fetch key with ID %s, should be %s", req.KeyID, KeyID)
		}

		hexkey, err := hex.DecodeString(string(req.ServerName))
		if err != nil {
			return nil, fmt.Errorf("FetchKeys: can't decode server name %q: %w", req.ServerName, err)
		}

		res[req] = gomatrixserverlib.PublicKeyLookupResult{
			VerifyKey: gomatrixserverlib.VerifyKey{
				Key: hexkey,
			},
			ExpiredTS:    gomatrixserverlib.PublicKeyNotExpired,
			ValidUntilTS: spec.AsTimestamp(time.Now().Add(24 * time.Hour * 365)),
		}
	}
	return res, nil
}

func (f *YggdrasilKeys) FetcherName() string {
	return "YggdrasilKeys"
}

func (f *YggdrasilKeys) StoreKeys(ctx context.Context, results map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult) error {
	return nil
}
