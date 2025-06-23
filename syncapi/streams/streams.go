package streams

import (
	"context"

	"github.com/antinvestor/matrix/internal/cacheutil"
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
	eduCache *cacheutil.EDUCache, lazyLoadCache cacheutil.LazyLoadCache, notifier *notifier.Notifier,
) *Streams {

	dbTx, _ := d.NewDatabaseTransaction(ctx)


	streams := &Streams{
		PDUStreamProvider: &PDUStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: dbTx},
			lazyLoadCache:         lazyLoadCache,
			rsAPI:                 rsAPI,
			notifier:              notifier,
		},
		TypingStreamProvider: &TypingStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: dbTx},
			EDUCache:              eduCache,
		},
		ReceiptStreamProvider: &ReceiptStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: dbTx},
		},
		InviteStreamProvider: &InviteStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: dbTx},
			rsAPI:                 rsAPI,
		},
		SendToDeviceStreamProvider: &SendToDeviceStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: dbTx},
		},
		AccountDataStreamProvider: &AccountDataStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: dbTx},
			userAPI:               userAPI,
		},
		NotificationDataStreamProvider: &NotificationDataStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: dbTx},
		},
		DeviceListStreamProvider: &DeviceListStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: dbTx},
			rsAPI:                 rsAPI,
			userAPI:               userAPI,
		},
		PresenceStreamProvider: &PresenceStreamProvider{
			DefaultStreamProvider: DefaultStreamProvider{DB: dbTx},
			notifier:              notifier,
		},
	}

	streams.PDUStreamProvider.Setup(ctx)
	streams.TypingStreamProvider.Setup(ctx)
	streams.ReceiptStreamProvider.Setup(ctx)
	streams.InviteStreamProvider.Setup(ctx)
	streams.SendToDeviceStreamProvider.Setup(ctx)
	streams.AccountDataStreamProvider.Setup(ctx)
	streams.NotificationDataStreamProvider.Setup(ctx)
	streams.DeviceListStreamProvider.Setup(ctx)
	streams.PresenceStreamProvider.Setup(ctx)

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
