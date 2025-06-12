package testrig

import (
	"context"
	"testing"

	"github.com/pitabwire/frame"

	"github.com/antinvestor/matrix/internal/queueutil"

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
			t.Fatalf("MustPublishMsgs: failed to publish message: %s", err)
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

func createStreamForQueueConfig(ctx context.Context, t *testing.T, svc *frame.Service, cfg *config.Matrix) {

	var qopts []*config.QueueOptions

	qopts = append(qopts, &cfg.AppServiceAPI.Queues.OutputAppserviceEvent,
		&cfg.ClientAPI.Queues.InputFulltextReindex,
		&cfg.FederationAPI.Queues.OutputPresenceEvent, &cfg.FederationAPI.Queues.OutputRoomEvent, &cfg.FederationAPI.Queues.OutputReceiptEvent, &cfg.FederationAPI.Queues.OutputTypingEvent, &cfg.FederationAPI.Queues.OutputSendToDeviceEvent,
		&cfg.KeyServer.Queues.OutputKeyChangeEvent,
		&cfg.RoomServer.Queues.InputRoomEvent,

		&cfg.SyncAPI.Queues.OutputRoomEvent,
		&cfg.SyncAPI.Queues.OutputClientData,
		&cfg.SyncAPI.Queues.OutputKeyChangeEvent,
		&cfg.SyncAPI.Queues.OutputSendToDeviceEvent,
		&cfg.SyncAPI.Queues.OutputTypingEvent,
		&cfg.SyncAPI.Queues.OutputReceiptEvent,
		&cfg.SyncAPI.Queues.OutputStreamEvent,
		&cfg.SyncAPI.Queues.OutputNotificationData,
		&cfg.SyncAPI.Queues.OutputPresenceEvent,

		&cfg.UserAPI.Queues.OutputRoomEvent,
		&cfg.UserAPI.Queues.OutputReceiptEvent,
		&cfg.UserAPI.Queues.InputSigningKeyUpdate,
		&cfg.UserAPI.Queues.InputDeviceListUpdate,
	)

	for _, opt := range qopts {

		//Make consumer configuration to be ephemeral

		err := svc.AddSubscriber(ctx, opt.Ref(), opt.DSrc().String())
		if err != nil {
			t.Fatalf("Could not create subscriber %s: %s", opt.Ref(), err)
		}

		err = svc.DiscardSubscriber(ctx, opt.Ref())
		if err != nil {
			t.Fatalf("Could not discard subscriber %s: %s", opt.Ref(), err)
		}
	}
}
