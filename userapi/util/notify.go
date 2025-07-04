package util

import (
	"context"
	"strings"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/pushgateway"
	"github.com/antinvestor/matrix/userapi/storage"
	"github.com/antinvestor/matrix/userapi/storage/tables"
	"github.com/pitabwire/util"
)

// NotifyUserCountsAsync sends notifications to a local user's
// notification destinations. UserDatabase lookups run synchronously, but
// a single goroutine is started when talking to the Push
// gateways. There is no way to know when the background goroutine has
// finished.
func NotifyUserCountsAsync(ctx context.Context, pgClient pushgateway.Client, localpart string, serverName spec.ServerName, db storage.UserDatabase) error {
	log := util.Log(ctx)

	pusherDevices, err := GetPushDevices(ctx, localpart, serverName, nil, db)
	if err != nil {
		return err
	}

	if len(pusherDevices) == 0 {
		return nil
	}

	userNumUnreadNotifs, err := db.GetNotificationCount(ctx, localpart, serverName, tables.AllNotifications)
	if err != nil {
		return err
	}

	log.
		WithField("localpart", localpart).
		WithField("app_id0", pusherDevices[0].Device.AppID).
		WithField("pushkey", pusherDevices[0].Device.PushKey).
		Debug("Notifying HTTP push gateway about notification counts")

	// TODO: think about bounding this to one per user, and what
	// ordering guarantees we must provide.
	go func() {
		// This background processing cannot be tied to a request.
		iCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		// TODO: we could batch all devices with the same URL, but
		// Sytest requires consumers/roomserver.go to do it
		// one-by-one, so we do the same here.
		for _, pusherDevice := range pusherDevices {
			// TODO: support "email".
			if !strings.HasPrefix(pusherDevice.URL, "http") {
				continue
			}

			req := pushgateway.NotifyRequest{
				Notification: pushgateway.Notification{
					Counts: &pushgateway.Counts{
						Unread: int(userNumUnreadNotifs),
					},
					Devices: []*pushgateway.Device{&pusherDevice.Device},
				},
			}
			if err = pgClient.Notify(iCtx, pusherDevice.URL, &req, &pushgateway.NotifyResponse{}); err != nil {
				log.
					WithField("localpart", localpart).
					WithField("app_id0", pusherDevice.Device.AppID).
					WithField("pushkey", pusherDevice.Device.PushKey).
					WithError(err).Error("HTTP push gateway request failed")
				return
			}
		}
	}()

	return nil
}
