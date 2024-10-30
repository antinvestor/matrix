package caching

import (
	"context"

	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/matrix-org/gomatrixserverlib"
)

// FederationCache contains the subset of functions needed for
// a federation event cache.
type FederationCache interface {
	GetFederationQueuedPDU(ctx context.Context, eventNID int64) (event *types.HeaderedEvent, ok bool)
	StoreFederationQueuedPDU(ctx context.Context, eventNID int64, event *types.HeaderedEvent) error
	EvictFederationQueuedPDU(ctx context.Context, eventNID int64) error

	GetFederationQueuedEDU(ctx context.Context, eventNID int64) (event *gomatrixserverlib.EDU, ok bool)
	StoreFederationQueuedEDU(ctx context.Context, eventNID int64, event *gomatrixserverlib.EDU) error
	EvictFederationQueuedEDU(ctx context.Context, eventNID int64) error
}

func (c Caches) GetFederationQueuedPDU(ctx context.Context, eventNID int64) (*types.HeaderedEvent, bool) {
	return c.FederationPDUs.Get(ctx, eventNID)
}

func (c Caches) StoreFederationQueuedPDU(ctx context.Context, eventNID int64, event *types.HeaderedEvent) error {
	return c.FederationPDUs.Set(ctx, eventNID, event)
}

func (c Caches) EvictFederationQueuedPDU(ctx context.Context, eventNID int64) error {
	return c.FederationPDUs.Unset(ctx, eventNID)
}

func (c Caches) GetFederationQueuedEDU(ctx context.Context, eventNID int64) (*gomatrixserverlib.EDU, bool) {
	return c.FederationEDUs.Get(ctx, eventNID)
}

func (c Caches) StoreFederationQueuedEDU(ctx context.Context, eventNID int64, event *gomatrixserverlib.EDU) error {
	return c.FederationEDUs.Set(ctx, eventNID, event)
}

func (c Caches) EvictFederationQueuedEDU(ctx context.Context, eventNID int64) error {
	return c.FederationEDUs.Unset(ctx, eventNID)
}
