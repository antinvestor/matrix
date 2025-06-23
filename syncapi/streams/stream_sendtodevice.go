package streams

import (
	"context"

	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/pitabwire/util"
)

type SendToDeviceStreamProvider struct {
	DefaultStreamProvider
}

func (p *SendToDeviceStreamProvider) Setup(
	ctx context.Context,
) {
	p.DefaultStreamProvider.Setup(ctx)

	p.latestMutex.Lock()
	defer p.latestMutex.Unlock()

	id, err := p.DB.MaxStreamPositionForSendToDeviceMessages(ctx)
	if err != nil {
		panic(err)
	}
	p.latest = id
}

func (p *SendToDeviceStreamProvider) CompleteSync(
	ctx context.Context,
	req *types.SyncRequest,
) types.StreamPosition {
	return p.IncrementalSync(ctx, req, 0, p.LatestPosition(ctx))
}

func (p *SendToDeviceStreamProvider) IncrementalSync(
	ctx context.Context,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {
	// See if we have any new tasks to do for the send-to-device messaging.
	lastPos, events, err := p.DB.SendToDeviceUpdatesForSync(req.Context, req.Device.UserID, req.Device.ID, from, to)
	if err != nil {
		util.Log(ctx).WithError(err).Error("p.Cm.SendToDeviceUpdatesForSync failed")
		return from
	}

	// Add the updates into the sync response.
	for _, event := range events {
		// skip ignored user events
		if _, ok := req.IgnoredUsers.List[event.Sender]; ok {
			continue
		}
		req.Response.ToDevice.Events = append(req.Response.ToDevice.Events, event.SendToDeviceEvent)
	}

	return lastPos
}
