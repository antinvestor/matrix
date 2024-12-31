package streams

import (
	"context"

	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/syncapi/internal"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/types"
	userapi "github.com/antinvestor/matrix/userapi/api"
)

type DeviceListStreamProvider struct {
	DefaultStreamProvider
	rsAPI   api.SyncRoomserverAPI
	userAPI userapi.SyncKeyAPI
}

func (p *DeviceListStreamProvider) CompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
) types.StreamPosition {
	return p.LatestPosition(ctx)
}

func (p *DeviceListStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {
	var err error
	to, _, err = internal.DeviceListCatchup(context.Background(), snapshot, p.userAPI, p.rsAPI, req.Device.UserID, req.Response, from, to)
	if err != nil {
		req.Log.With(slog.Any("error", err)).Error("internal.DeviceListCatchup failed")
		return from
	}
	err = internal.DeviceOTKCounts(req.Context, p.userAPI, req.Device.UserID, req.Device.ID, req.Response)
	if err != nil {
		req.Log.With(slog.Any("error", err)).Error("internal.DeviceListCatchup failed")
		return from
	}

	return to
}
