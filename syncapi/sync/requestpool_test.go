package sync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/test/testrig"
)

type dummyPublisher struct {
	lock  sync.Mutex
	count int
}

func (d *dummyPublisher) SendPresence(ctx context.Context, userID string, presence types.Presence, statusMsg *string) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.count++
	return nil
}

type dummyDB struct{}

func (d dummyDB) UpdatePresence(ctx context.Context, userID string, presence types.Presence, statusMsg *string, lastActiveTS spec.Timestamp, fromSync bool) (types.StreamPosition, error) {
	return 0, nil
}

func (d dummyDB) GetPresences(ctx context.Context, userID []string) ([]*types.PresenceInternal, error) {
	return []*types.PresenceInternal{}, nil
}

func (d dummyDB) PresenceAfter(ctx context.Context, after types.StreamPosition, filter synctypes.EventFilter) (map[string]*types.PresenceInternal, error) {
	return map[string]*types.PresenceInternal{}, nil
}

func (d dummyDB) MaxStreamPositionForPresence(ctx context.Context) (types.StreamPosition, error) {
	return 0, nil
}

type dummyConsumer struct{}

func (d dummyConsumer) EmitPresence(ctx context.Context, userID string, presence types.Presence, statusMsg *string, ts spec.Timestamp, fromSync bool) {

}

func TestRequestPool_updatePresence(t *testing.T) {
	type args struct {
		presence string
		userID   string
		sleep    time.Duration
	}
	publisher := &dummyPublisher{}
	consumer := &dummyConsumer{}
	syncMap := sync.Map{}

	tests := []struct {
		name         string
		args         args
		wantIncrease bool
	}{
		{
			name:         "new presence is published",
			wantIncrease: true,
			args: args{
				userID: "dummy",
			},
		},
		{
			name: "presence not published, no change",
			args: args{
				userID: "dummy",
			},
		},
		{
			name:         "new presence is published dummy2",
			wantIncrease: true,
			args: args{
				userID:   "dummy2",
				presence: "online",
			},
		},
		/*
			TODO: Fixme
					{
						name:         "different presence is published dummy2",
						wantIncrease: true,
						args: args{
							userID:   "dummy2",
							presence: "unavailable",
						},
					},
					{
						name: "same presence is not published dummy2",
						args: args{
							userID:   "dummy2",
							presence: "unavailable",
							sleep:    time.Millisecond * 150,
						},
					},
				{
					name:         "same presence is published after being deleted",
					wantIncrease: true,
					args: args{
						userID:   "dummy2",
						presence: "unavailable",
					},
				},
		*/
	}
	rp := &RequestPool{
		presence: &syncMap,
		producer: publisher,
		consumer: consumer,
		cfg: &config.SyncAPI{
			Global: &config.Global{
				Presence: config.PresenceOptions{
					EnableInbound:  true,
					EnableOutbound: true,
				},
			},
		},
	}
	db := dummyDB{}

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	go rp.cleanPresence(ctx, db, time.Millisecond*50)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publisher.lock.Lock()
			beforeCount := publisher.count
			publisher.lock.Unlock()
			rp.updatePresence(ctx, db, tt.args.presence, tt.args.userID)
			publisher.lock.Lock()
			if tt.wantIncrease && publisher.count <= beforeCount {
				t.Fatalf("expected count to increase: %d <= %d", publisher.count, beforeCount)
			}
			publisher.lock.Unlock()
			time.Sleep(tt.args.sleep)
		})
	}
}
