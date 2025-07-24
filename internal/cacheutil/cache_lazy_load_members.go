package cacheutil

import (
	"context"

	userapi "github.com/antinvestor/matrix/userapi/api"
)

type lazyLoadingCacheKey struct {
	UserID       string // the user we're querying on behalf of
	DeviceID     string // the user we're querying on behalf of
	RoomID       string // the room in question
	TargetUserID string // the user whose membership we're asking about
}

type LazyLoadCache interface {
	StoreLazyLoadedUser(ctx context.Context, device *userapi.Device, roomID, targetUserID, eventID string) error
	IsLazyLoadedUserCached(ctx context.Context, device *userapi.Device, roomID, targetUserID string) (string, bool)
	InvalidateLazyLoadedUser(ctx context.Context, device *userapi.Device, roomID, targetUserID string) error
}

func (c Caches) StoreLazyLoadedUser(ctx context.Context, device *userapi.Device, roomID, targetUserID, eventID string) error {
	return c.LazyLoading.Set(ctx, lazyLoadingCacheKey{
		UserID:       device.UserID,
		DeviceID:     device.ID,
		RoomID:       roomID,
		TargetUserID: targetUserID,
	}, eventID)
}

func (c Caches) IsLazyLoadedUserCached(ctx context.Context, device *userapi.Device, roomID, targetUserID string) (string, bool) {
	return c.LazyLoading.Get(ctx, lazyLoadingCacheKey{
		UserID:       device.UserID,
		DeviceID:     device.ID,
		RoomID:       roomID,
		TargetUserID: targetUserID,
	})
}

func (c Caches) InvalidateLazyLoadedUser(ctx context.Context, device *userapi.Device, roomID, targetUserID string) error {
	return c.LazyLoading.Unset(ctx, lazyLoadingCacheKey{
		UserID:       device.UserID,
		DeviceID:     device.ID,
		RoomID:       roomID,
		TargetUserID: targetUserID,
	})
}
