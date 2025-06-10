package testrig

import (
	"context"
	"testing"
	"time"

	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/test"
	"github.com/pitabwire/frame"
	"github.com/stretchr/testify/assert"
)

type h struct {
	msgs []any
}

func (h *h) Handle(ctx context.Context, metadata map[string]string, message []byte) error {
	h.msgs = append(h.msgs, message)
	println("handled message : " + string(message))

	return nil
}

func TestQueueConnections(t *testing.T) {

	testCases := []struct {
		name           string
		subscopts      *config.QueueOptions
		subscHandlers  []frame.SubscribeWorker
		wantSubscError assert.ErrorAssertionFunc
		pubcopts       *config.QueueOptions
		wantPubError   assert.ErrorAssertionFunc
		pubMessages    []any
	}{
		{
			name: "bad queue in memory",
			subscopts: &config.QueueOptions{
				Prefix:     "q",
				QReference: "badPath",
				DS:         "mem+://happyPath",
			},
			subscHandlers:  []frame.SubscribeWorker{&h{}},
			wantSubscError: assert.Error,
			pubcopts: &config.QueueOptions{
				Prefix:     "q",
				QReference: "badPath",
				DS:         "mem+://happyPath",
			},
			wantPubError: assert.Error,
			pubMessages:  []any{},
		},
		{
			name: "normal in memory",
			subscopts: &config.QueueOptions{
				Prefix:     "inmem_",
				QReference: "happyPath",
				DS:         "mem://happyPath",
			},
			subscHandlers:  []frame.SubscribeWorker{&h{}},
			wantSubscError: assert.NoError,
			pubcopts: &config.QueueOptions{
				Prefix:     "inmem_",
				QReference: "happyPath",
				DS:         "mem://happyPath",
			},
			wantPubError: assert.NoError,
			pubMessages: []any{
				"hello",
			},
		},
		{
			name: "normal nats queue",
			subscopts: &config.QueueOptions{
				Prefix:     "nats_",
				QReference: "natsPath",
				DS:         "nats://matrix:s3cr3t@localhost:4221/nats.test.subject",
			},
			subscHandlers:  []frame.SubscribeWorker{&h{}},
			wantSubscError: assert.NoError,
			pubcopts: &config.QueueOptions{
				Prefix:     "nats_",
				QReference: "natsPath",
				DS:         "nats://matrix:s3cr3t@localhost:4221/nats.test.subject",
			},
			wantPubError: assert.NoError,
			pubMessages: []any{
				"hello nats",
			},
		},
		{
			name: "jetstream nats queue",
			subscopts: &config.QueueOptions{
				Prefix:     "jetstream_",
				QReference: "jetstreamPath",
				DS:         "nats://matrix:s3cr3t@localhost:4221/jetstream.test.subject?jetstream=true&stream_name=tests_jetstream&stream_retention=workqueue&stream_storage=memory",
			},
			subscHandlers:  []frame.SubscribeWorker{&h{}},
			wantSubscError: assert.NoError,
			pubcopts: &config.QueueOptions{
				Prefix:     "jetstream_",
				QReference: "jetstreamPath",
				DS:         "nats://matrix:s3cr3t@localhost:4221/jetstream.test.subject",
			},
			wantPubError: assert.NoError,
			pubMessages: []any{
				"hello jetstream fans",
			},
		},
	}

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, _ := Init(t, testOpts)
		defer svc.Stop(ctx)

		qm := queueutil.NewQueueManager(svc)

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := qm.RegisterSubscriber(ctx, tc.subscopts, tc.subscHandlers...)
				tc.wantSubscError(t, err)

				err = qm.RegisterPublisher(ctx, tc.pubcopts)
				tc.wantPubError(t, err)

				for _, msg := range tc.pubMessages {
					err = qm.Publish(ctx, tc.pubcopts.Ref(), msg)
					assert.NoError(t, err)
				}

				if len(tc.subscHandlers) == 1 {
					time.Sleep(time.Millisecond * 100)
					handler := tc.subscHandlers[0].(*h)
					assert.Equal(t, len(tc.pubMessages), len(handler.msgs))
				}

			})
		}

	})
}
