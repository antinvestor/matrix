package producers

import (
	"context"
	"github.com/antinvestor/matrix/internal/queueutil"

	"github.com/antinvestor/gomatrixserverlib"
	log "github.com/sirupsen/logrus"

	"github.com/antinvestor/matrix/internal/eventutil"

	"github.com/antinvestor/matrix/userapi/storage"
)

// SyncAPI produces messages for the Sync API server to consume.
type SyncAPI struct {
	db                    storage.Notification
	qm                    queueutil.QueueManager
	clientDataTopic       string
	notificationDataTopic string
}

func NewSyncAPI(db storage.UserDatabase, qm queueutil.QueueManager, clientDataTopic string, notificationDataTopic string) *SyncAPI {
	return &SyncAPI{
		db:                    db,
		qm:                    qm,
		clientDataTopic:       clientDataTopic,
		notificationDataTopic: notificationDataTopic,
	}
}

// SendAccountData sends account data to the Sync API server.
func (p *SyncAPI) SendAccountData(ctx context.Context, userID string, data eventutil.AccountData) error {

	log.WithFields(log.Fields{
		"user_id":   userID,
		"room_id":   data.RoomID,
		"data_type": data.Type,
	}).Tracef("Producing to topic '%s'", p.clientDataTopic)

	header := map[string]string{
		queueutil.UserID: userID,
	}

	err := p.qm.Publish(ctx, p.clientDataTopic, data, header)
	if err != nil {
		return err
	}
	return nil
}

// GetAndSendNotificationData reads the database and sends data about unread
// notifications to the Sync API server.
func (p *SyncAPI) GetAndSendNotificationData(ctx context.Context, userID, roomID string) error {
	localpart, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		return err
	}

	ntotal, nhighlight, err := p.db.GetRoomNotificationCounts(ctx, localpart, domain, roomID)
	if err != nil {
		return err
	}

	return p.sendNotificationData(ctx, userID, &eventutil.NotificationData{
		RoomID:                  roomID,
		UnreadHighlightCount:    int(nhighlight),
		UnreadNotificationCount: int(ntotal),
	})
}

// sendNotificationData sends data about unread notifications to the Sync API server.
func (p *SyncAPI) sendNotificationData(ctx context.Context, userID string, data *eventutil.NotificationData) error {

	log.WithFields(log.Fields{
		"user_id": userID,
		"room_id": data.RoomID,
	}).Tracef("Producing to topic '%s'", p.clientDataTopic)

	header := map[string]string{
		queueutil.UserID: userID,
	}

	err := p.qm.Publish(ctx, p.notificationDataTopic, data, header)
	if err != nil {
		return err
	}
	return nil
}
