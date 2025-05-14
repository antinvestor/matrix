package cacheutil

import (
	"context"

	"github.com/antinvestor/matrix/roomserver/types"
)

// EventStateKeyCache contains the subset of functions needed for
// a room event state key cache.
type EventStateKeyCache interface {
	GetEventStateKey(ctx context.Context, eventStateKeyNID types.EventStateKeyNID) (string, bool)
	StoreEventStateKey(ctx context.Context, eventStateKeyNID types.EventStateKeyNID, eventStateKey string) error
	GetEventStateKeyNID(ctx context.Context, eventStateKey string) (types.EventStateKeyNID, bool)
}

func (c Caches) GetEventStateKey(ctx context.Context, eventStateKeyNID types.EventStateKeyNID) (string, bool) {
	return c.RoomServerStateKeys.Get(ctx, eventStateKeyNID)
}

func (c Caches) StoreEventStateKey(ctx context.Context, eventStateKeyNID types.EventStateKeyNID, eventStateKey string) error {
	err := c.RoomServerStateKeys.Set(ctx, eventStateKeyNID, eventStateKey)
	if err != nil {
		return err
	}
	return c.RoomServerStateKeyNIDs.Set(ctx, eventStateKey, eventStateKeyNID)
}

func (c Caches) GetEventStateKeyNID(ctx context.Context, eventStateKey string) (types.EventStateKeyNID, bool) {
	return c.RoomServerStateKeyNIDs.Get(ctx, eventStateKey)
}

type EventTypeCache interface {
	GetEventTypeKey(ctx context.Context, eventType string) (types.EventTypeNID, bool)
	StoreEventTypeKey(ctx context.Context, eventTypeNID types.EventTypeNID, eventType string) error
}

func (c Caches) StoreEventTypeKey(ctx context.Context, eventTypeNID types.EventTypeNID, eventType string) error {
	err := c.RoomServerEventTypeNIDs.Set(ctx, eventType, eventTypeNID)
	if err != nil {
		return err
	}
	return c.RoomServerEventTypes.Set(ctx, eventTypeNID, eventType)
}

func (c Caches) GetEventTypeKey(ctx context.Context, eventType string) (types.EventTypeNID, bool) {
	return c.RoomServerEventTypeNIDs.Get(ctx, eventType)
}
