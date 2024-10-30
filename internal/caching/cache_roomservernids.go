package caching

import (
	"context"

	"github.com/antinvestor/matrix/roomserver/types"
)

type RoomServerCaches interface {
	RoomServerNIDsCache
	RoomVersionCache
	RoomServerEventsCache
	RoomHierarchyCache
	EventStateKeyCache
	EventTypeCache
}

// RoomServerNIDsCache contains the subset of functions needed for
// a roomserver NID cache.
type RoomServerNIDsCache interface {
	GetRoomServerRoomID(ctx context.Context, roomNID types.RoomNID) (string, bool)
	// StoreRoomServerRoomID stores roomNID -> roomID and roomID -> roomNID
	StoreRoomServerRoomID(ctx context.Context, roomNID types.RoomNID, roomID string) error
	GetRoomServerRoomNID(ctx context.Context, roomID string) (types.RoomNID, bool)
}

func (c Caches) GetRoomServerRoomID(ctx context.Context, roomNID types.RoomNID) (string, bool) {
	return c.RoomServerRoomIDs.Get(ctx, roomNID)
}

// StoreRoomServerRoomID stores roomNID -> roomID and roomID -> roomNID
func (c Caches) StoreRoomServerRoomID(ctx context.Context, roomNID types.RoomNID, roomID string) error {
	err := c.RoomServerRoomNIDs.Set(ctx, roomID, roomNID)
	if err != nil {
		return err
	}
	return c.RoomServerRoomIDs.Set(ctx, roomNID, roomID)
}

func (c Caches) GetRoomServerRoomNID(ctx context.Context, roomID string) (types.RoomNID, bool) {
	return c.RoomServerRoomNIDs.Get(ctx, roomID)
}
