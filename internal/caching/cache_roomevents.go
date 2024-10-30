package caching

import (
	"context"
	"github.com/antinvestor/matrix/roomserver/types"
)

// RoomServerEventsCache contains the subset of functions needed for a room server event cache.
type RoomServerEventsCache interface {
	GetRoomServerEvent(ctx context.Context, eventNID types.EventNID) (*types.HeaderedEvent, bool)
	StoreRoomServerEvent(ctx context.Context, eventNID types.EventNID, event *types.HeaderedEvent) error
	InvalidateRoomServerEvent(ctx context.Context, eventNID types.EventNID) error
}

func (c Caches) GetRoomServerEvent(ctx context.Context, eventNID types.EventNID) (*types.HeaderedEvent, bool) {
	return c.RoomServerEvents.Get(ctx, int64(eventNID))
}

func (c Caches) StoreRoomServerEvent(ctx context.Context, eventNID types.EventNID, event *types.HeaderedEvent) error {
	return c.RoomServerEvents.Set(ctx, int64(eventNID), event)
}

func (c Caches) InvalidateRoomServerEvent(ctx context.Context, eventNID types.EventNID) error {
	return c.RoomServerEvents.Unset(ctx, int64(eventNID))
}
