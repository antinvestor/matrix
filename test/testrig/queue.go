package testrig

import (
	"context"
	"testing"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/constants"
)

type QMsg struct {
	Header map[string]string
	Data   any
}

func MustPublishMsgs(ctx context.Context, t *testing.T, qopts *config.QueueOptions, qm queueutil.QueueManager, msgs ...*QMsg) error {
	t.Helper()

	publisher, err := qm.GetOrCreatePublisher(ctx, qopts)
	if err != nil {
		return err
	}

	for _, msg := range msgs {
		err = publisher.Publish(ctx, msg.Data, msg.Header)
		if err != nil {
			t.Fatalf("MustPublishMsgs: failed to publish message: %s", err)
		}
	}

	return nil
}

func NewOutputEventMsg(t *testing.T, roomID *spec.RoomID, update api.OutputEvent) *QMsg {
	t.Helper()

	msg := QMsg{
		Header: map[string]string{
			constants.RoomEventType: string(update.Type),
			constants.RoomID:        constants.EncodeRoomID(roomID),
		},
		Data: update,
	}
	return &msg
}
