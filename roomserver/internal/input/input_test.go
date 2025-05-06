package input_test

import (
	"context"
	"testing"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/internal/input"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/jetstream"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
)

func TestSingleTransactionOnInput(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		ctx, svc, cfg := testrig.Init(t, testOpts)
		defer svc.Stop(ctx)
		cfg, closeRig := testrig.CreateConfig(ctx, t, testOpts)
		defer closeRig()
		cm := sqlutil.NewConnectionManager(ctx, cfg.Global.DatabaseOptions)

		natsInstance := &jetstream.NATSInstance{}
		js, jc := natsInstance.Prepare(ctx, &cfg.Global.JetStream)
		caches, err := caching.NewCache(&cfg.Global.Cache)
		if err != nil {
			t.Fatalf("failed to create a cache: %v", err)
		}
		rsAPI := roomserver.NewInternalAPI(ctx, cfg, cm, natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(ctx, nil, nil)

		deadline, _ := t.Deadline()
		if maxVal := time.Now().Add(time.Second * 3); deadline.Before(maxVal) {
			deadline = maxVal
		}
		ctx, cancel := context.WithDeadline(ctx, deadline)
		defer cancel()

		event, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV6).NewEventFromTrustedJSON(
			[]byte(`{"auth_events":[],"content":{"creator":"@neilalexander:dendrite.matrix.org","room_version":"6"},"depth":1,"hashes":{"sha256":"jqOqdNEH5r0NiN3xJtj0u5XUVmRqq9YvGbki1wxxuuM"},"origin":"dendrite.matrix.org","origin_server_ts":1644595362726,"prev_events":[],"prev_state":[],"room_id":"!jSZZRknA6GkTBXNP:dendrite.matrix.org","sender":"@neilalexander:dendrite.matrix.org","signatures":{"dendrite.matrix.org":{"ed25519:6jB2aB":"bsQXO1wketf1OSe9xlndDIWe71W9KIundc6rBw4KEZdGPW7x4Tv4zDWWvbxDsG64sS2IPWfIm+J0OOozbrWIDw"}},"state_key":"","type":"m.room.create"}`),
			false,
		)
		if err != nil {
			t.Fatal(err)
		}
		in := api.InputRoomEvent{
			Kind:  api.KindOutlier, // don't panic if we generate an output event
			Event: &types.HeaderedEvent{PDU: event},
		}

		inputter := &input.Inputer{
			JetStream:  js,
			NATSClient: jc,
			Cfg:        &cfg.RoomServer,
		}
		res := &api.InputRoomEventsResponse{}
		inputter.InputRoomEvents(
			ctx,
			&api.InputRoomEventsRequest{
				InputRoomEvents: []api.InputRoomEvent{in},
				Asynchronous:    false,
			},
			res,
		)
		// If we fail here then it's because we've hit the test deadline,
		// so we probably deadlocked
		if err := res.Err(); err != nil {
			t.Fatal(err)
		}
	})
}
