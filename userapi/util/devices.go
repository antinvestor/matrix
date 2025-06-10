package util

import (
	"context"
	"fmt"

	"github.com/pitabwire/frame"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/pushgateway"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/antinvestor/matrix/userapi/storage"
)

type PusherDevice struct {
	Device pushgateway.Device
	Pusher *api.Pusher
	URL    string
	Format string
}

// GetPushDevices pushes to the configured devices of a local user.
func GetPushDevices(ctx context.Context, localpart string, serverName spec.ServerName, tweaks map[string]interface{}, db storage.UserDatabase) ([]*PusherDevice, error) {

	log := frame.Log(ctx)

	pushers, err := db.GetPushers(ctx, localpart, serverName)
	if err != nil {
		return nil, fmt.Errorf("db.GetPushers: %w", err)
	}

	devices := make([]*PusherDevice, 0, len(pushers))
	for _, pusher := range pushers {
		var url, format string
		data := pusher.Data
		switch pusher.Kind {
		case api.EmailKind:
			url = "mailto:"

		case api.HTTPKind:
			// TODO: The spec says only event_id_only is supported,
			// but Sytests assume "" means "full notification".
			fmtIface := pusher.Data["format"]
			var ok bool
			format, ok = fmtIface.(string)
			if ok && format != "event_id_only" {
				log.
					WithField("localpart", localpart).
					WithField("app_id", pusher.AppID).
					Error("Only data.format event_id_only or empty is supported")
				continue
			}

			urlIface := pusher.Data["url"]
			url, ok = urlIface.(string)
			if !ok {
				log.
					WithField("localpart", localpart).
					WithField("app_id", pusher.AppID).
					Error("No data.url configured for HTTP Pusher")
				continue
			}
			data = mapWithout(data, "url")

		default:
			log.
				WithField("localpart", localpart).
				WithField("app_id", pusher.AppID).
				WithField("kind", pusher.Kind).
				Error("Unhandled pusher kind")
			continue
		}

		devices = append(devices, &PusherDevice{
			Device: pushgateway.Device{
				AppID:     pusher.AppID,
				Data:      data,
				PushKey:   pusher.PushKey,
				PushKeyTS: pusher.PushKeyTS,
				Tweaks:    tweaks,
			},
			Pusher: &pusher,
			URL:    url,
			Format: format,
		})
	}

	return devices, nil
}

// mapWithout returns a shallow copy of the map, without the given
// key. Returns nil if the resulting map is empty.
func mapWithout(m map[string]interface{}, key string) map[string]interface{} {
	ret := make(map[string]interface{}, len(m))
	for k, v := range m {
		// The specification says we do not send "url".
		if k == key {
			continue
		}
		ret[k] = v
	}
	if len(ret) == 0 {
		return nil
	}
	return ret
}
