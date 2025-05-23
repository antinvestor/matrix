package testrig

import (
	"context"
	"github.com/antinvestor/matrix/internal/queueutil"
	"testing"

	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
)

type QMsg struct {
	Subject string
	Header  map[string]string
	Data    any
}

func MustPublishMsgs(ctx context.Context, t *testing.T, qm queueutil.QueueManager, msgs ...*QMsg) {
	t.Helper()
	for _, msg := range msgs {
		err := qm.Publish(ctx, msg.Subject, msg.Data, msg.Header)
		if err != nil {
			t.Fatalf("MustPublishMsgs: failed to publish message: %s", err)
		}
	}
}

func NewOutputEventMsg(t *testing.T, cfg *config.Matrix, roomID string, update api.OutputEvent) *QMsg {
	t.Helper()
	msg := QMsg{
		Subject: cfg.Global.JetStream.Prefixed(jetstream.OutputRoomEvent),
		Header: map[string]string{
			jetstream.RoomEventType: string(update.Type),
			jetstream.RoomID:        roomID,
		},
		Data: update,
	}
	return &msg
}
