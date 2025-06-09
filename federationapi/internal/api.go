package internal

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/antinvestor/gomatrix"
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/api"
	"github.com/antinvestor/matrix/federationapi/queue"
	"github.com/antinvestor/matrix/federationapi/statistics"
	"github.com/antinvestor/matrix/federationapi/storage"
	"github.com/antinvestor/matrix/federationapi/storage/cache"
	"github.com/antinvestor/matrix/internal/cacheutil"
	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame"
)

// FederationInternalAPI is an implementation of api.FederationInternalAPI
type FederationInternalAPI struct {
	db         storage.Database
	cfg        *config.FederationAPI
	statistics *statistics.Statistics
	rsAPI      roomserverAPI.FederationRoomserverAPI
	federation fclient.FederationClient
	keyRing    *gomatrixserverlib.KeyRing
	queues     *queue.OutgoingQueues
	joins      sync.Map // joins currently in progress
}

func NewFederationInternalAPI(
	ctx context.Context,
	db storage.Database, cfg *config.FederationAPI,
	rsAPI roomserverAPI.FederationRoomserverAPI,
	federation fclient.FederationClient,
	statistics *statistics.Statistics,
	caches *cacheutil.Caches,
	queues *queue.OutgoingQueues,
	keyRing *gomatrixserverlib.KeyRing,
) *FederationInternalAPI {
	serverKeyDB, err := cache.NewKeyDatabase(db, caches)
	if err != nil {
		frame.Log(ctx).WithError(err).Panic("failed to set up caching wrapper for server key database")
	}

	if keyRing == nil {
		keyRing = &gomatrixserverlib.KeyRing{
			KeyFetchers: []gomatrixserverlib.KeyFetcher{},
			KeyDatabase: serverKeyDB,
		}

		pubKey := cfg.Global.PrivateKey.Public().(ed25519.PublicKey)
		addDirectFetcher := func() {
			keyRing.KeyFetchers = append(
				keyRing.KeyFetchers,
				&gomatrixserverlib.DirectKeyFetcher{
					Client:            federation,
					IsLocalServerName: cfg.Global.IsLocalServerName,
					LocalPublicKey:    []byte(pubKey),
				},
			)
		}

		if cfg.PreferDirectFetch {
			addDirectFetcher()
		} else {
			defer addDirectFetcher()
		}

		var b64e = base64.StdEncoding.WithPadding(base64.NoPadding)
		for _, ps := range cfg.KeyPerspectives {
			perspective := &gomatrixserverlib.PerspectiveKeyFetcher{
				PerspectiveServerName: ps.ServerName,
				PerspectiveServerKeys: map[gomatrixserverlib.KeyID]ed25519.PublicKey{},
				Client:                federation,
			}

			for _, key := range ps.Keys {
				rawkey, err := b64e.DecodeString(key.PublicKey)
				if err != nil {
					frame.Log(ctx).WithError(err).
						WithField("server_name", ps.ServerName).
						WithField("public_key", key.PublicKey).
						Warn("Couldn't parse perspective key")
					continue
				}
				perspective.PerspectiveServerKeys[key.KeyID] = rawkey
			}

			keyRing.KeyFetchers = append(keyRing.KeyFetchers, perspective)

			frame.Log(ctx).
				WithField("server_name", ps.ServerName).
				WithField("num_public_keys", len(ps.Keys)).
				Info("Enabled perspective key fetcher")
		}
	}

	return &FederationInternalAPI{
		db:         db,
		cfg:        cfg,
		rsAPI:      rsAPI,
		keyRing:    keyRing,
		federation: federation,
		statistics: statistics,
		queues:     queues,
	}
}

func (r *FederationInternalAPI) IsBlacklistedOrBackingOff(ctx context.Context, s spec.ServerName) (*statistics.ServerStatistics, error) {
	stats := r.statistics.ForServer(ctx, s)
	if stats.Blacklisted() {
		return stats, &api.FederationClientError{
			Blacklisted: true,
		}
	}

	now := time.Now()
	until := stats.BackoffInfo()
	if until != nil && now.Before(*until) {
		return stats, &api.FederationClientError{
			RetryAfter: time.Until(*until),
		}
	}

	return stats, nil
}

func failBlacklistableError(ctx context.Context, err error, stats *statistics.ServerStatistics) (until time.Time, blacklisted bool) {
	if err == nil {
		return
	}
	mxerr, ok := err.(gomatrix.HTTPError)
	if !ok {
		return stats.Failure(ctx)
	}
	if mxerr.Code == 401 { // invalid signature in X-Global header
		return stats.Failure(ctx)
	}
	if mxerr.Code >= 500 && mxerr.Code < 600 { // internal server errors
		return stats.Failure(ctx)
	}
	return
}

func (r *FederationInternalAPI) doRequestIfNotBackingOffOrBlacklisted(
	ctx context.Context, s spec.ServerName, request func() (interface{}, error),
) (interface{}, error) {
	stats, err := r.IsBlacklistedOrBackingOff(ctx, s)
	if err != nil {
		return nil, err
	}
	res, err := request()
	if err != nil {
		until, blacklisted := failBlacklistableError(ctx, err, stats)
		now := time.Now()
		var retryAfter time.Duration
		if until.After(now) {
			retryAfter = time.Until(until)
		}
		return res, &api.FederationClientError{
			Err:         err.Error(),
			Blacklisted: blacklisted,
			RetryAfter:  retryAfter,
		}
	}
	stats.Success(ctx, statistics.SendDirect)
	return res, nil
}

func (r *FederationInternalAPI) doRequestIfNotBlacklisted(
	ctx context.Context, s spec.ServerName, request func() (interface{}, error),
) (interface{}, error) {
	stats := r.statistics.ForServer(ctx, s)
	if blacklisted := stats.Blacklisted(); blacklisted {
		return stats, &api.FederationClientError{
			Err:         fmt.Sprintf("server %q is blacklisted", s),
			Blacklisted: true,
		}
	}
	return request()
}
