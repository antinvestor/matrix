package caching

import (
	"context"

	"github.com/matrix-org/gomatrixserverlib"
)

// RoomVersionsCache contains the subset of functions needed for
// a room version cache.
type RoomVersionCache interface {
	GetRoomVersion(ctx context.Context, roomID string) (roomVersion gomatrixserverlib.RoomVersion, ok bool)
	StoreRoomVersion(ctx context.Context, roomID string, roomVersion gomatrixserverlib.RoomVersion) error
}

func (c Caches) GetRoomVersion(ctx context.Context, roomID string) (gomatrixserverlib.RoomVersion, bool) {
	return c.RoomVersions.Get(ctx, roomID)
}

func (c Caches) StoreRoomVersion(ctx context.Context, roomID string, roomVersion gomatrixserverlib.RoomVersion) error {
	return c.RoomVersions.Set(ctx, roomID, roomVersion)
}
