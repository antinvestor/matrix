package streams

import (
	"context"

	"github.com/antinvestor/matrix/internal/caching"
	rsapi "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/syncapi/notifier"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/types"
	userapi "github.com/antinvestor/matrix/userapi/api"
)

type Streams struct {
	PDUStreamProvider              StreamProvider
	TypingStreamProvider           StreamProvider
	ReceiptStreamProvider          StreamProvider
	InviteStreamProvider           StreamProvider
	SendToDeviceStreamProvider     StreamProvider
	AccountDataStreamProvider      StreamProvider
	DeviceListStreamProvider       StreamProvider
	NotificationDataStreamProvider StreamProvider
	PresenceStreamProvider         StreamProvider
}

func NewSyncStreamProviders(ctx context.Context,
	d storage.Database, userAPI userapi.SyncUserAPI,
	rsAPI rsapi.SyncRoomserverAPI,
	eduCache *caching.EDUCache, lazyLoadCache caching.LazyLoadCache, notifier *notifier.Notifier,
) *Streams {
	streams := &Streams{
		PDUStreamProvider: &PDUStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: d},
			lazyLoadCache:         lazyLoadCache,
			rsAPI:                 rsAPI,
			notifier:              notifier,
		},
		TypingStreamProvider: &TypingStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: d},
			EDUCache:              eduCache,
		},
		ReceiptStreamProvider: &ReceiptStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: d},
		},
		InviteStreamProvider: &InviteStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: d},
			rsAPI:                 rsAPI,
		},
		SendToDeviceStreamProvider: &SendToDeviceStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: d},
		},
		AccountDataStreamProvider: &AccountDataStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: d},
			userAPI:               userAPI,
		},
		NotificationDataStreamProvider: &NotificationDataStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: d},
		},
		DeviceListStreamProvider: &DeviceListStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: d},
			rsAPI:                 rsAPI,
			userAPI:               userAPI,
		},
		PresenceStreamProvider: &PresenceStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: d},
			notifier:              notifier,
		},
	}

	snapshot, err := d.NewDatabaseSnapshot(ctx)
	if err != nil {
		panic(err)
	}

	streams.PDUStreamProvider.Setup(ctx, snapshot)
	streams.TypingStreamProvider.Setup(ctx, snapshot)
	streams.ReceiptStreamProvider.Setup(ctx, snapshot)
	streams.InviteStreamProvider.Setup(ctx, snapshot)
	streams.SendToDeviceStreamProvider.Setup(ctx, snapshot)
	streams.AccountDataStreamProvider.Setup(ctx, snapshot)
	streams.NotificationDataStreamProvider.Setup(ctx, snapshot)
	streams.DeviceListStreamProvider.Setup(ctx, snapshot)
	streams.PresenceStreamProvider.Setup(ctx, snapshot)

	return streams
}

func (s *Streams) Latest(ctx context.Context) types.StreamingToken {
	return types.StreamingToken{
		PDUPosition:              s.PDUStreamProvider.LatestPosition(ctx),
		TypingPosition:           s.TypingStreamProvider.LatestPosition(ctx),
		ReceiptPosition:          s.ReceiptStreamProvider.LatestPosition(ctx),
		InvitePosition:           s.InviteStreamProvider.LatestPosition(ctx),
		SendToDevicePosition:     s.SendToDeviceStreamProvider.LatestPosition(ctx),
		AccountDataPosition:      s.AccountDataStreamProvider.LatestPosition(ctx),
		NotificationDataPosition: s.NotificationDataStreamProvider.LatestPosition(ctx),
		DeviceListPosition:       s.DeviceListStreamProvider.LatestPosition(ctx),
		PresencePosition:         s.PresenceStreamProvider.LatestPosition(ctx),
	}
}
