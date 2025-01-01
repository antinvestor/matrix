package caching

import (
	"context"

	"github.com/antinvestor/gomatrixserverlib/fclient"
)

// RoomHierarchy cache caches responses to federated room hierarchy requests (A.K.A. 'space summaries')
type RoomHierarchyCache interface {
	GetRoomHierarchy(ctx context.Context, roomID string) (r fclient.RoomHierarchyResponse, ok bool)
	StoreRoomHierarchy(ctx context.Context, roomID string, r fclient.RoomHierarchyResponse) error
}

func (c Caches) GetRoomHierarchy(ctx context.Context, roomID string) (r fclient.RoomHierarchyResponse, ok bool) {
	return c.RoomHierarchies.Get(ctx, roomID)
}

func (c Caches) StoreRoomHierarchy(ctx context.Context, roomID string, r fclient.RoomHierarchyResponse) error {
	return c.RoomHierarchies.Set(ctx, roomID, r)
}
