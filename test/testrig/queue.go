package testrig

import (
	"context"
	"github.com/antinvestor/matrix/internal/queueutil"
	"testing"

	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/setup/config"
)

type QMsg struct {
	Header map[string]string
	Data   any
}

func MustPublishMsgs(ctx context.Context, t *testing.T, qopts *config.QueueOptions, qm queueutil.QueueManager, msgs ...*QMsg) error {
	t.Helper()

	err := qm.RegisterPublisher(ctx, qopts)
	if err != nil {
		return err
	}

	for _, msg := range msgs {
		err = qm.Publish(ctx, qopts.Ref(), msg.Data, msg.Header)
		if err != nil {
			t.Fatal("MustPublishMsgs: failed to publish message: %s", err)
		}
	}

	return nil
}

func NewOutputEventMsg(t *testing.T, roomID string, update api.OutputEvent) *QMsg {
	t.Helper()

	msg := QMsg{
		Header: map[string]string{
			queueutil.RoomEventType: string(update.Type),
			queueutil.RoomID:        roomID,
		},
		Data: update,
	}
	return &msg
}
