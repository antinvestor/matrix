// Copyright 2022 The Global.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/producers"
	"github.com/antinvestor/matrix/internal/queueutil"
	rsAPI "github.com/antinvestor/matrix/roomserver/api"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/constants"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	keyAPI "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/frame"
	"github.com/stretchr/testify/assert"
	"gotest.tools/v3/poll"
)

const (
	testOrigin      = spec.ServerName("kaer.morhen")
	testDestination = spec.ServerName("white.orchard")
)

var (
	invalidSignatures = json.RawMessage(`{"auth_events":["$x4MKEPRSF6OGlo0qpnsP3BfSmYX5HhVlykOsQH3ECyg","$BcEcbZnlFLB5rxSNSZNBn6fO3jU/TKAJ79wfKyCQLiU"],"content":{"body":"Test Message"},"depth":3917,"hashes":{"sha256":"cNAWtlHIegrji0mMA6x1rhpYCccY8W1NsWZqSpJFhjs"},"origin":"localhost","origin_server_ts":0,"prev_events":["$4GDB0bVjkWwS3G4noUZCq5oLWzpBYpwzdMcf7gj24CI"],"room_id":"!roomid:localishhost","sender":"@userid:localhost","signatures":{"localhost":{"ed25519:auto":"NKym6Kcy3u9mGUr21Hjfe3h7DfDilDhN5PqztT0QZ4NTZ+8Y7owseLolQVXp+TvNjecvzdDywsXXVvGiaQiWAQ"}},"type":"m.room.member"}`)
	testData          = []json.RawMessage{
		[]byte(`{"auth_events":[],"content":{"creator":"@userid:kaer.morhen"},"depth":0,"event_id":"$0ok8ynDp7kjc95e3:kaer.morhen","hashes":{"sha256":"17kPoH+h0Dk4Omn7Sus0qMb6+oGcf+CZFEgDhv7UKWs"},"origin":"kaer.morhen","origin_server_ts":0,"prev_events":[],"prev_state":[],"room_id":"!roomid:kaer.morhen","sender":"@userid:kaer.morhen","signatures":{"kaer.morhen":{"ed25519:auto":"jP4a04f5/F10Pw95FPpdCyKAO44JOwUQ/MZOOeA/RTU1Dn+AHPMzGSaZnuGjRr/xQuADt+I3ctb5ZQfLKNzHDw"}},"state_key":"","type":"m.room.create"}`),
		[]byte(`{"auth_events":[["$0ok8ynDp7kjc95e3:kaer.morhen",{"sha256":"sWCi6Ckp9rDimQON+MrUlNRkyfZ2tjbPbWfg2NMB18Q"}]],"content":{"membership":"join"},"depth":1,"event_id":"$LEwEu0kxrtu5fOiS:kaer.morhen","hashes":{"sha256":"B7M88PhXf3vd1LaFtjQutFu4x/w7fHD28XKZ4sAsJTo"},"origin":"kaer.morhen","origin_server_ts":0,"prev_events":[["$0ok8ynDp7kjc95e3:kaer.morhen",{"sha256":"sWCi6Ckp9rDimQON+MrUlNRkyfZ2tjbPbWfg2NMB18Q"}]],"prev_state":[],"room_id":"!roomid:kaer.morhen","sender":"@userid:kaer.morhen","signatures":{"kaer.morhen":{"ed25519:auto":"p2vqmuJn7ZBRImctSaKbXCAxCcBlIjPH9JHte1ouIUGy84gpu4eLipOvSBCLL26hXfC0Zrm4WUto6Hr+ohdrCg"}},"state_key":"@userid:kaer.morhen","type":"m.room.member"}`),
		[]byte(`{"auth_events":[["$0ok8ynDp7kjc95e3:kaer.morhen",{"sha256":"sWCi6Ckp9rDimQON+MrUlNRkyfZ2tjbPbWfg2NMB18Q"}],["$LEwEu0kxrtu5fOiS:kaer.morhen",{"sha256":"1aKajq6DWHru1R1HJjvdWMEavkJJHGaTmPvfuERUXaA"}]],"content":{"join_rule":"public"},"depth":2,"event_id":"$SMHlqUrNhhBBRLeN:kaer.morhen","hashes":{"sha256":"vIuJQvmMjrGxshAkj1SXe0C4RqvMbv4ZADDw9pFCWqQ"},"origin":"kaer.morhen","origin_server_ts":0,"prev_events":[["$LEwEu0kxrtu5fOiS:kaer.morhen",{"sha256":"1aKajq6DWHru1R1HJjvdWMEavkJJHGaTmPvfuERUXaA"}]],"prev_state":[],"room_id":"!roomid:kaer.morhen","sender":"@userid:kaer.morhen","signatures":{"kaer.morhen":{"ed25519:auto":"hBMsb3Qppo3RaqqAl4JyTgaiWEbW5hlckATky6PrHun+F3YM203TzG7w9clwuQU5F5pZoB1a6nw+to0hN90FAw"}},"state_key":"","type":"m.room.join_rules"}`),
		[]byte(`{"auth_events":[["$0ok8ynDp7kjc95e3:kaer.morhen",{"sha256":"sWCi6Ckp9rDimQON+MrUlNRkyfZ2tjbPbWfg2NMB18Q"}],["$LEwEu0kxrtu5fOiS:kaer.morhen",{"sha256":"1aKajq6DWHru1R1HJjvdWMEavkJJHGaTmPvfuERUXaA"}]],"content":{"history_visibility":"shared"},"depth":3,"event_id":"$6F1yGIbO0J7TM93h:kaer.morhen","hashes":{"sha256":"Mr23GKSlZW7UCCYLgOWawI2Sg6KIoMjUWO2TDenuOgw"},"origin":"kaer.morhen","origin_server_ts":0,"prev_events":[["$SMHlqUrNhhBBRLeN:kaer.morhen",{"sha256":"SylzE8U02I+6eyEHgL+FlU0L5YdqrVp8OOlxKS9VQW0"}]],"prev_state":[],"room_id":"!roomid:kaer.morhen","sender":"@userid:kaer.morhen","signatures":{"kaer.morhen":{"ed25519:auto":"sHLKrFI3hKGrEJfpMVZSDS3LvLasQsy50CTsOwru9XTVxgRsPo6wozNtRVjxo1J3Rk18RC9JppovmQ5VR5EcDw"}},"state_key":"","type":"m.room.history_visibility"}`),
		[]byte(`{"auth_events":[["$0ok8ynDp7kjc95e3:kaer.morhen",{"sha256":"sWCi6Ckp9rDimQON+MrUlNRkyfZ2tjbPbWfg2NMB18Q"}],["$LEwEu0kxrtu5fOiS:kaer.morhen",{"sha256":"1aKajq6DWHru1R1HJjvdWMEavkJJHGaTmPvfuERUXaA"}]],"content":{"ban":50,"events":null,"events_default":0,"invite":0,"kick":50,"redact":50,"state_default":50,"users":null,"users_default":0},"depth":4,"event_id":"$UKNe10XzYzG0TeA9:kaer.morhen","hashes":{"sha256":"ngbP3yja9U5dlckKerUs/fSOhtKxZMCVvsfhPURSS28"},"origin":"kaer.morhen","origin_server_ts":0,"prev_events":[["$6F1yGIbO0J7TM93h:kaer.morhen",{"sha256":"A4CucrKSoWX4IaJXhq02mBg1sxIyZEftbC+5p3fZAvk"}]],"prev_state":[],"room_id":"!roomid:kaer.morhen","sender":"@userid:kaer.morhen","signatures":{"kaer.morhen":{"ed25519:auto":"zOmwlP01QL3yFchzuR9WHvogOoBZA3oVtNIF3lM0ZfDnqlSYZB9sns27G/4HVq0k7alaK7ZE3oGoCrVnMkPNCw"}},"state_key":"","type":"m.room.power_levels"}`),
		// messages
		[]byte(`{"auth_events":[["$0ok8ynDp7kjc95e3:kaer.morhen",{"sha256":"sWCi6Ckp9rDimQON+MrUlNRkyfZ2tjbPbWfg2NMB18Q"}],["$LEwEu0kxrtu5fOiS:kaer.morhen",{"sha256":"1aKajq6DWHru1R1HJjvdWMEavkJJHGaTmPvfuERUXaA"}]],"content":{"body":"Test Message"},"depth":5,"event_id":"$gl2T9l3qm0kUbiIJ:kaer.morhen","hashes":{"sha256":"Qx3nRMHLDPSL5hBAzuX84FiSSP0K0Kju2iFoBWH4Za8"},"origin":"kaer.morhen","origin_server_ts":0,"prev_events":[["$UKNe10XzYzG0TeA9:kaer.morhen",{"sha256":"KtSRyMjt0ZSjsv2koixTRCxIRCGoOp6QrKscsW97XRo"}]],"room_id":"!roomid:kaer.morhen","sender":"@userid:kaer.morhen","signatures":{"kaer.morhen":{"ed25519:auto":"sqDgv3EG7ml5VREzmT9aZeBpS4gAPNIaIeJOwqjDhY0GPU/BcpX5wY4R7hYLrNe5cChgV+eFy/GWm1Zfg5FfDg"}},"type":"m.room.message"}`),
		[]byte(`{"auth_events":[["$0ok8ynDp7kjc95e3:kaer.morhen",{"sha256":"sWCi6Ckp9rDimQON+MrUlNRkyfZ2tjbPbWfg2NMB18Q"}],["$LEwEu0kxrtu5fOiS:kaer.morhen",{"sha256":"1aKajq6DWHru1R1HJjvdWMEavkJJHGaTmPvfuERUXaA"}]],"content":{"body":"Test Message"},"depth":6,"event_id":"$MYSbs8m4rEbsCWXD:kaer.morhen","hashes":{"sha256":"kgbYM7v4Ud2YaBsjBTolM4ySg6rHcJNYI6nWhMSdFUA"},"origin":"kaer.morhen","origin_server_ts":0,"prev_events":[["$gl2T9l3qm0kUbiIJ:kaer.morhen",{"sha256":"C/rD04h9wGxRdN2G/IBfrgoE1UovzLZ+uskwaKZ37/Q"}]],"room_id":"!roomid:kaer.morhen","sender":"@userid:kaer.morhen","signatures":{"kaer.morhen":{"ed25519:auto":"x0UoKh968jj/F5l1/R7Ew0T6CTKuew3PLNHASNxqck/bkNe8yYQiDHXRr+kZxObeqPZZTpaF1+EI+bLU9W8GDQ"}},"type":"m.room.message"}`),
		[]byte(`{"auth_events":[["$0ok8ynDp7kjc95e3:kaer.morhen",{"sha256":"sWCi6Ckp9rDimQON+MrUlNRkyfZ2tjbPbWfg2NMB18Q"}],["$LEwEu0kxrtu5fOiS:kaer.morhen",{"sha256":"1aKajq6DWHru1R1HJjvdWMEavkJJHGaTmPvfuERUXaA"}]],"content":{"body":"Test Message"},"depth":7,"event_id":"$N5x9WJkl9ClPrAEg:kaer.morhen","hashes":{"sha256":"FWM8oz4yquTunRZ67qlW2gzPDzdWfBP6RPHXhK1I/x8"},"origin":"kaer.morhen","origin_server_ts":0,"prev_events":[["$MYSbs8m4rEbsCWXD:kaer.morhen",{"sha256":"fatqgW+SE8mb2wFn3UN+drmluoD4UJ/EcSrL6Ur9q1M"}]],"room_id":"!roomid:kaer.morhen","sender":"@userid:kaer.morhen","signatures":{"kaer.morhen":{"ed25519:auto":"Y+LX/xcyufoXMOIoqQBNOzy6lZfUGB1ffgXIrSugk6obMiyAsiRejHQN/pciZXsHKxMJLYRFAz4zSJoS/LGPAA"}},"type":"m.room.message"}`),
	}
	testEvent       = []byte(`{"auth_events":["$x4MKEPRSF6OGlo0qpnsP3BfSmYX5HhVlykOsQH3ECyg","$BcEcbZnlFLB5rxSNSZNBn6fO3jU/TKAJ79wfKyCQLiU"],"content":{"body":"Test Message"},"depth":3917,"hashes":{"sha256":"cNAWtlHIegrji0mMA6x1rhpYCccY8W1NsWZqSpJFhjs"},"origin":"localhost","origin_server_ts":0,"prev_events":["$4GDB0bVjkWwS3G4noUZCq5oLWzpBYpwzdMcf7gj24CI"],"room_id":"!roomid:localhost","sender":"@userid:localhost","signatures":{"localhost":{"ed25519:auto":"NKym6Kcy3u9mGUr21Hjfe3h7DfDilDhN5PqztT0QZ4NTZ+8Y7owseLolQVXp+TvNjecvzdDywsXXVvGiuQiWAQ"}},"type":"m.room.message"}`)
	testRoomVersion = gomatrixserverlib.RoomVersionV1
	testEvents      []*rstypes.HeaderedEvent
	testStateEvents = make(map[gomatrixserverlib.StateKeyTuple]*rstypes.HeaderedEvent)
)

type msgHandler struct {
	f func(ctx context.Context, metadata map[string]string, message []byte) error
}

func (h *msgHandler) Handle(ctx context.Context, metadata map[string]string, message []byte) error {
	return h.f(ctx, metadata, message)
}

type FakeRsAPI struct {
	rsAPI.RoomserverInternalAPI
	shouldFailQuery bool
	bannedFromRoom  bool
}

func (r *FakeRsAPI) QueryUserIDForSender(_ context.Context, _ spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(senderID), true)
}

func (r *FakeRsAPI) QueryRoomVersionForRoom(
	_ context.Context,
	_ string,
) (gomatrixserverlib.RoomVersion, error) {
	if r.shouldFailQuery {
		return "", fmt.Errorf("failure")
	}
	return gomatrixserverlib.RoomVersionV10, nil
}

func (r *FakeRsAPI) QueryServerBannedFromRoom(
	_ context.Context,
	_ *rsAPI.QueryServerBannedFromRoomRequest,
	res *rsAPI.QueryServerBannedFromRoomResponse,
) error {
	if r.bannedFromRoom {
		res.Banned = true
	} else {
		res.Banned = false
	}
	return nil
}

func (r *FakeRsAPI) InputRoomEvents(
	_ context.Context,
	_ *rsAPI.InputRoomEventsRequest,
	_ *rsAPI.InputRoomEventsResponse,
) {
}

func TestEmptyTransactionRequest(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	txn := NewTxnReq(&FakeRsAPI{}, nil, "ourserver", nil, nil, nil, false, []json.RawMessage{}, []gomatrixserverlib.EDU{}, "", "", "")
	txnRes, jsonRes := txn.ProcessTransaction(ctx)

	assert.Nil(t, jsonRes)
	assert.Zero(t, len(txnRes.PDUs))
}

func TestProcessTransactionRequestPDU(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	keyRing := &test.NopJSONVerifier{}
	txn := NewTxnReq(&FakeRsAPI{}, nil, "ourserver", keyRing, nil, nil, false, []json.RawMessage{testEvent}, []gomatrixserverlib.EDU{}, "", "", "")
	txnRes, jsonRes := txn.ProcessTransaction(ctx)

	assert.Nil(t, jsonRes)
	assert.Equal(t, 1, len(txnRes.PDUs))
	for _, result := range txnRes.PDUs {
		assert.Empty(t, result.Error)
	}
}

func TestProcessTransactionRequestPDUs(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	keyRing := &test.NopJSONVerifier{}
	txn := NewTxnReq(&FakeRsAPI{}, nil, "ourserver", keyRing, nil, nil, false, append(testData, testEvent), []gomatrixserverlib.EDU{}, "", "", "")
	txnRes, jsonRes := txn.ProcessTransaction(ctx)

	assert.Nil(t, jsonRes)
	assert.Equal(t, 1, len(txnRes.PDUs))
	for _, result := range txnRes.PDUs {
		assert.Empty(t, result.Error)
	}
}

func TestProcessTransactionRequestBadPDU(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	pdu := json.RawMessage("{\"room_id\":\"asdf\"}")
	pdu2 := json.RawMessage("\"roomid\":\"asdf\"")
	keyRing := &test.NopJSONVerifier{}
	txn := NewTxnReq(&FakeRsAPI{}, nil, "ourserver", keyRing, nil, nil, false, []json.RawMessage{pdu, pdu2, testEvent}, []gomatrixserverlib.EDU{}, "", "", "")
	txnRes, jsonRes := txn.ProcessTransaction(ctx)

	assert.Nil(t, jsonRes)
	assert.Equal(t, 1, len(txnRes.PDUs))
	for _, result := range txnRes.PDUs {
		assert.Empty(t, result.Error)
	}
}

func TestProcessTransactionRequestPDUQueryFailure(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	keyRing := &test.NopJSONVerifier{}
	txn := NewTxnReq(&FakeRsAPI{shouldFailQuery: true}, nil, "ourserver", keyRing, nil, nil, false, []json.RawMessage{testEvent}, []gomatrixserverlib.EDU{}, "", "", "")
	txnRes, jsonRes := txn.ProcessTransaction(ctx)

	assert.Nil(t, jsonRes)
	assert.Zero(t, len(txnRes.PDUs))
}

func TestProcessTransactionRequestPDUBannedFromRoom(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	keyRing := &test.NopJSONVerifier{}
	txn := NewTxnReq(&FakeRsAPI{bannedFromRoom: true}, nil, "ourserver", keyRing, nil, nil, false, []json.RawMessage{testEvent}, []gomatrixserverlib.EDU{}, "", "", "")
	txnRes, jsonRes := txn.ProcessTransaction(ctx)

	assert.Nil(t, jsonRes)
	assert.Equal(t, 1, len(txnRes.PDUs))
	for _, result := range txnRes.PDUs {
		assert.NotEmpty(t, result.Error)
	}
}

func TestProcessTransactionRequestPDUInvalidSignature(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	keyRing := &test.NopJSONVerifier{}
	txn := NewTxnReq(&FakeRsAPI{}, nil, "ourserver", keyRing, nil, nil, false, []json.RawMessage{invalidSignatures}, []gomatrixserverlib.EDU{}, "", "", "")
	txnRes, jsonRes := txn.ProcessTransaction(ctx)

	assert.Nil(t, jsonRes)
	assert.Equal(t, 1, len(txnRes.PDUs))
	for _, result := range txnRes.PDUs {
		assert.NotEmpty(t, result.Error)
	}
}

func createTransactionWithEDU(ctx context.Context, svc *frame.Service, cfg *config.Matrix, edus []gomatrixserverlib.EDU) (*TxnReq, queueutil.QueueManager, error) {

	qm := queueutil.NewQueueManager(svc)

	cfgUserApi := cfg.UserAPI
	cfgSyncApi := cfg.SyncAPI

	_, err := qm.GetOrCreatePublisher(ctx, &cfgSyncApi.Queues.OutputReceiptEvent)
	if err != nil {
		return nil, nil, err
	}
	_, err = qm.GetOrCreatePublisher(ctx, &cfgSyncApi.Queues.OutputSendToDeviceEvent)
	if err != nil {
		return nil, nil, err
	}
	_, err = qm.GetOrCreatePublisher(ctx, &cfgSyncApi.Queues.OutputTypingEvent)
	if err != nil {
		return nil, nil, err
	}
	_, err = qm.GetOrCreatePublisher(ctx, &cfgSyncApi.Queues.OutputPresenceEvent)
	if err != nil {
		return nil, nil, err
	}
	_, err = qm.GetOrCreatePublisher(ctx, &cfgUserApi.Queues.InputDeviceListUpdate)
	if err != nil {
		return nil, nil, err
	}
	_, err = qm.GetOrCreatePublisher(ctx, &cfgUserApi.Queues.InputSigningKeyUpdate)
	if err != nil {
		return nil, nil, err
	}

	producer := &producers.SyncAPIProducer{
		Qm:                     qm,
		TopicReceiptEvent:      cfgSyncApi.Queues.OutputReceiptEvent.Ref(),
		TopicSendToDeviceEvent: cfgSyncApi.Queues.OutputSendToDeviceEvent.Ref(),
		TopicTypingEvent:       cfgSyncApi.Queues.OutputTypingEvent.Ref(),
		TopicPresenceEvent:     cfgSyncApi.Queues.OutputPresenceEvent.Ref(),
		TopicDeviceListUpdate:  cfgUserApi.Queues.InputDeviceListUpdate.Ref(),
		TopicSigningKeyUpdate:  cfgUserApi.Queues.InputSigningKeyUpdate.Ref(),
		Config:                 &cfg.FederationAPI,
		UserAPI:                nil,
	}
	keyRing := &test.NopJSONVerifier{}
	txn := NewTxnReq(&FakeRsAPI{}, nil, "ourserver", keyRing, nil, producer, true, []json.RawMessage{}, edus, "kaer.morhen", "", "ourserver")
	return &txn, qm, nil
}

func TestProcessTransactionRequestEDUTyping(t *testing.T) {
	var err error

	ctx, svc, cfg := testrig.Init(t)
	defer svc.Stop(ctx)

	roomID := "!roomid:kaer.morhen"
	userID := "@userid:kaer.morhen"
	typing := true
	edu := gomatrixserverlib.EDU{Type: "m.typing"}
	if edu.Content, err = json.Marshal(map[string]interface{}{
		"room_id": roomID,
		"user_id": userID,
		"typing":  typing,
	}); err != nil {
		t.Errorf("failed to marshal EDU JSON")
	}
	badEDU := gomatrixserverlib.EDU{Type: "m.typing"}
	badEDU.Content = json.RawMessage("badjson")
	edus := []gomatrixserverlib.EDU{badEDU, edu}

	txn, qm, err := createTransactionWithEDU(ctx, svc, cfg, edus)
	assert.NoError(t, err)

	received := atomic.Bool{}

	h := &msgHandler{f: func(ctx context.Context, metadata map[string]string, message []byte) error {

		room := metadata[constants.RoomID]
		assert.Equal(t, roomID, room)
		user := metadata[constants.UserID]
		assert.Equal(t, userID, user)
		typ, parseErr := strconv.ParseBool(metadata["typing"])
		if parseErr != nil {
			return nil
		}
		assert.Equal(t, typing, typ)

		received.Store(true)
		return nil
	}}

	err = qm.RegisterSubscriber(ctx, &cfg.SyncAPI.Queues.OutputTypingEvent, h)
	assert.Nil(t, err)

	txnRes, jsonRes := txn.ProcessTransaction(ctx)
	assert.Nil(t, jsonRes)
	assert.Zero(t, len(txnRes.PDUs))

	check := func(log poll.LogT) poll.Result {
		if received.Load() {
			return poll.Success()
		}
		return poll.Continue("waiting for events to be processed")
	}
	poll.WaitOn(t, check, poll.WithTimeout(2*time.Second), poll.WithDelay(10*time.Millisecond))
}

func TestProcessTransactionRequestEDUToDevice(t *testing.T) {
	var err error

	ctx, svc, cfg := testrig.Init(t)
	defer svc.Stop(ctx)

	sender := "@userid:kaer.morhen"
	messageID := "$x4MKEPRSF6OGlo0qpnsP3BfSmYX5HhVlykOsQH3ECyg"
	msgType := "m.dendrite.test"
	edu := gomatrixserverlib.EDU{Type: "m.direct_to_device"}
	if edu.Content, err = json.Marshal(map[string]interface{}{
		"sender":     sender,
		"type":       msgType,
		"message_id": messageID,
		"messages": map[string]interface{}{
			"@alice:example.org": map[string]interface{}{
				"IWHQUZUIAH": map[string]interface{}{
					"algorithm":   "m.megolm.v1.aes-sha2",
					"room_id":     "!Cuyf34gef24t:localhost",
					"session_id":  "X3lUlvLELLYxeTx4yOVu6UDpasGEVO0Jbu+QFnm0cKQ",
					"session_key": "AgAAAADxKHa9uFxcXzwYoNueL5Xqi69IkD4sni8LlfJL7qNBEY...",
				},
			},
		},
	}); err != nil {
		t.Errorf("failed to marshal EDU JSON")
	}
	badEDU := gomatrixserverlib.EDU{Type: "m.direct_to_device"}
	badEDU.Content = json.RawMessage("badjson")
	edus := []gomatrixserverlib.EDU{badEDU, edu}

	txn, qm, err := createTransactionWithEDU(ctx, svc, cfg, edus)
	assert.NoError(t, err)

	received := atomic.Bool{}
	h := &msgHandler{f: func(ctx context.Context, metadata map[string]string, message []byte) error {

		var output types.OutputSendToDeviceEvent
		if err = json.Unmarshal(message, &output); err != nil {
			// If the message was invalid, log it and move on to the next message in the stream
			println(err.Error())
			return nil
		}
		assert.Equal(t, sender, output.Sender)
		assert.Equal(t, msgType, output.Type)

		received.Store(true)
		return nil
	}}

	outputQOpts := cfg.SyncAPI.Queues.OutputSendToDeviceEvent
	outputQOpts.DS = outputQOpts.DS.RemoveQuery("subject").
		ExtendQuery("consumer_headers_only", "false")

	err = qm.RegisterSubscriber(ctx, &outputQOpts, h)
	assert.Nil(t, err)

	txnRes, jsonRes := txn.ProcessTransaction(ctx)
	assert.Nil(t, jsonRes)
	assert.Zero(t, len(txnRes.PDUs))

	check := func(log poll.LogT) poll.Result {
		if received.Load() {
			return poll.Success()
		}
		return poll.Continue("waiting for events to be processed")
	}
	poll.WaitOn(t, check, poll.WithTimeout(2*time.Second), poll.WithDelay(10*time.Millisecond))
}

func TestProcessTransactionRequestEDUDeviceListUpdate(t *testing.T) {
	var err error

	ctx, svc, cfg := testrig.Init(t)
	defer svc.Stop(ctx)

	deviceID := "QBUAZIFURK"
	userID := "@john:example.com"
	edu := gomatrixserverlib.EDU{Type: "m.device_list_update"}
	if edu.Content, err = json.Marshal(map[string]interface{}{
		"device_display_name": "Mobile",
		"device_id":           deviceID,
		"key":                 "value",
		"keys": map[string]interface{}{
			"algorithms": []string{
				"m.olm.v1.curve25519-aes-sha2",
				"m.megolm.v1.aes-sha2",
			},
			"device_id": "JLAFKJWSCS",
			"keys": map[string]interface{}{
				"curve25519:JLAFKJWSCS": "3C5BFWi2Y8MaVvjM8M22DBmh24PmgR0nPvJOIArzgyI",
				"ed25519:JLAFKJWSCS":    "lEuiRJBit0IG6nUf5pUzWTUEsRVVe/HJkoKuEww9ULI",
			},
			"signatures": map[string]interface{}{
				"@alice:example.com": map[string]interface{}{
					"ed25519:JLAFKJWSCS": "dSO80A01XiigH3uBiDVx/EjzaoycHcjq9lfQX0uWsqxl2giMIiSPR8a4d291W1ihKJL/a+myXS367WT6NAIcBA",
				},
			},
			"user_id": "@alice:example.com",
		},
		"prev_id": []int{
			5,
		},
		"stream_id": 6,
		"user_id":   userID,
	}); err != nil {
		t.Errorf("failed to marshal EDU JSON")
	}
	badEDU := gomatrixserverlib.EDU{Type: "m.device_list_update"}
	badEDU.Content = json.RawMessage("badjson")
	edus := []gomatrixserverlib.EDU{badEDU, edu}

	txn, qm, err := createTransactionWithEDU(ctx, svc, cfg, edus)
	assert.NoError(t, err)

	received := atomic.Bool{}
	h := &msgHandler{f: func(ctx context.Context, metadata map[string]string, message []byte) error {

		var output gomatrixserverlib.DeviceListUpdateEvent
		if err = json.Unmarshal(message, &output); err != nil {
			// If the message was invalid, log it and move on to the next message in the stream
			println(err.Error())
			return nil
		}
		assert.Equal(t, userID, output.UserID)
		assert.Equal(t, deviceID, output.DeviceID)

		received.Store(true)
		return nil
	}}

	err = qm.RegisterSubscriber(ctx, &cfg.UserAPI.Queues.InputDeviceListUpdate, h)
	assert.Nil(t, err)

	txnRes, jsonRes := txn.ProcessTransaction(ctx)
	assert.Nil(t, jsonRes)
	assert.Zero(t, len(txnRes.PDUs))

	check := func(log poll.LogT) poll.Result {
		if received.Load() {
			return poll.Success()
		}
		return poll.Continue("waiting for events to be processed")
	}
	poll.WaitOn(t, check, poll.WithTimeout(2*time.Second), poll.WithDelay(10*time.Millisecond))
}

func TestProcessTransactionRequestEDUReceipt(t *testing.T) {
	var err error

	ctx, svc, cfg := testrig.Init(t)
	defer svc.Stop(ctx)

	roomID := "!some_room:example.org"
	edu := gomatrixserverlib.EDU{Type: "m.receipt"}
	if edu.Content, err = json.Marshal(map[string]interface{}{
		roomID: map[string]interface{}{
			"m.read": map[string]interface{}{
				"@john:kaer.morhen": map[string]interface{}{
					"data": map[string]interface{}{
						"ts": 1533358089009,
					},
					"event_ids": []string{
						"$read_this_event:matrix.org",
					},
				},
			},
		},
	}); err != nil {
		t.Errorf("failed to marshal EDU JSON")
	}
	badEDU := gomatrixserverlib.EDU{Type: "m.receipt"}
	badEDU.Content = json.RawMessage("badjson")
	badUser := gomatrixserverlib.EDU{Type: "m.receipt"}
	if badUser.Content, err = json.Marshal(map[string]interface{}{
		roomID: map[string]interface{}{
			"m.read": map[string]interface{}{
				"johnkaer.morhen": map[string]interface{}{
					"data": map[string]interface{}{
						"ts": 1533358089009,
					},
					"event_ids": []string{
						"$read_this_event:matrix.org",
					},
				},
			},
		},
	}); err != nil {
		t.Errorf("failed to marshal EDU JSON")
	}
	badDomain := gomatrixserverlib.EDU{Type: "m.receipt"}
	if badDomain.Content, err = json.Marshal(map[string]interface{}{
		roomID: map[string]interface{}{
			"m.read": map[string]interface{}{
				"@john:bad.domain": map[string]interface{}{
					"data": map[string]interface{}{
						"ts": 1533358089009,
					},
					"event_ids": []string{
						"$read_this_event:matrix.org",
					},
				},
			},
		},
	}); err != nil {
		t.Errorf("failed to marshal EDU JSON")
	}
	edus := []gomatrixserverlib.EDU{badEDU, badUser, edu}

	txn, qm, err := createTransactionWithEDU(ctx, svc, cfg, edus)
	assert.Nil(t, err)

	received := atomic.Bool{}
	h := &msgHandler{f: func(ctx context.Context, metadata map[string]string, message []byte) error {

		var output types.OutputReceiptEvent
		output.RoomID = metadata[constants.RoomID]
		assert.Equal(t, roomID, output.RoomID)

		received.Store(true)
		return nil
	}}

	err = qm.RegisterSubscriber(ctx, &cfg.SyncAPI.Queues.OutputReceiptEvent, h)
	assert.Nil(t, err)

	txnRes, jsonRes := txn.ProcessTransaction(ctx)
	assert.Nil(t, jsonRes)
	assert.Zero(t, len(txnRes.PDUs))

	check := func(log poll.LogT) poll.Result {
		if received.Load() {
			return poll.Success()
		}
		return poll.Continue("waiting for events to be processed")
	}
	poll.WaitOn(t, check, poll.WithTimeout(2*time.Second), poll.WithDelay(10*time.Millisecond))
}

func TestProcessTransactionRequestEDUSigningKeyUpdate(t *testing.T) {
	var err error

	ctx, svc, cfg := testrig.Init(t)
	defer svc.Stop(ctx)

	edu := gomatrixserverlib.EDU{Type: "m.signing_key_update"}
	if edu.Content, err = json.Marshal(map[string]any{}); err != nil {
		t.Errorf("failed to marshal EDU JSON")
	}
	badEDU := gomatrixserverlib.EDU{Type: "m.signing_key_update"}
	badEDU.Content = json.RawMessage("badjson")
	edus := []gomatrixserverlib.EDU{badEDU, edu}

	txn, qm, err := createTransactionWithEDU(ctx, svc, cfg, edus)
	assert.NoError(t, err)

	received := atomic.Bool{}
	h := &msgHandler{f: func(ctx context.Context, metadata map[string]string, message []byte) error {

		var output keyAPI.CrossSigningKeyUpdate
		if err = json.Unmarshal(message, &output); err != nil {
			// If the message was invalid, log it and move on to the next message in the stream
			println(" CrsSgn K ", string(message), " error : ", err.Error())
			return nil
		}

		received.Store(true)
		return nil
	}}

	err = qm.RegisterSubscriber(ctx, &cfg.UserAPI.Queues.InputSigningKeyUpdate, h)
	assert.Nil(t, err)

	txnRes, jsonRes := txn.ProcessTransaction(ctx)
	assert.Nil(t, jsonRes)
	assert.Zero(t, len(txnRes.PDUs))

	check := func(log poll.LogT) poll.Result {
		if received.Load() {
			return poll.Success()
		}
		return poll.Continue("waiting for events to be processed")
	}
	poll.WaitOn(t, check, poll.WithTimeout(2*time.Second), poll.WithDelay(10*time.Millisecond))
}

func TestProcessTransactionRequestEDUPresence(t *testing.T) {
	var err error

	ctx, svc, cfg := testrig.Init(t)
	defer svc.Stop(ctx)

	userID := "@john:kaer.morhen"
	presence := "online"
	edu := gomatrixserverlib.EDU{Type: "m.presence"}
	if edu.Content, err = json.Marshal(map[string]interface{}{
		"push": []map[string]interface{}{{
			"currently_active": true,
			"last_active_ago":  5000,
			"presence":         presence,
			"status_msg":       "Making cupcakes",
			"user_id":          userID,
		}},
	}); err != nil {
		t.Errorf("failed to marshal EDU JSON")
	}
	badEDU := gomatrixserverlib.EDU{Type: "m.presence"}
	badEDU.Content = json.RawMessage("badjson")
	edus := []gomatrixserverlib.EDU{badEDU, edu}

	txn, qm, err := createTransactionWithEDU(ctx, svc, cfg, edus)
	assert.NoError(t, err)

	received := atomic.Bool{}

	h := &msgHandler{f: func(ctx context.Context, metadata map[string]string, message []byte) error {

		userIDRes := metadata[constants.UserID]
		presenceRes := metadata["presence"]
		assert.Equal(t, userID, userIDRes)
		assert.Equal(t, presence, presenceRes)

		received.Store(true)
		return nil
	}}

	err = qm.RegisterSubscriber(ctx, &cfg.SyncAPI.Queues.OutputPresenceEvent, h)
	assert.Nil(t, err)

	txnRes, jsonRes := txn.ProcessTransaction(ctx)
	assert.Nil(t, jsonRes)
	assert.Zero(t, len(txnRes.PDUs))

	check := func(log poll.LogT) poll.Result {
		if received.Load() {
			return poll.Success()
		}
		return poll.Continue("waiting for events to be processed")
	}
	poll.WaitOn(t, check, poll.WithTimeout(2*time.Second), poll.WithDelay(10*time.Millisecond))
}

func TestProcessTransactionRequestEDUUnhandled(t *testing.T) {
	var err error

	ctx, svc, cfg := testrig.Init(t)
	defer svc.Stop(ctx)

	edu := gomatrixserverlib.EDU{Type: "m.unhandled"}
	if edu.Content, err = json.Marshal(map[string]interface{}{}); err != nil {
		t.Errorf("failed to marshal EDU JSON")
	}

	txn, _, err := createTransactionWithEDU(ctx, svc, cfg, []gomatrixserverlib.EDU{edu})
	assert.NoError(t, err)

	txnRes, jsonRes := txn.ProcessTransaction(ctx)

	assert.Nil(t, jsonRes)
	assert.Zero(t, len(txnRes.PDUs))
}

func init() {
	for _, j := range testData {
		e, err := gomatrixserverlib.MustGetRoomVersion(testRoomVersion).NewEventFromTrustedJSON(j, false)
		if err != nil {
			panic("cannot load test data: " + err.Error())
		}
		h := &rstypes.HeaderedEvent{PDU: e}
		testEvents = append(testEvents, h)
		if e.StateKey() != nil {
			testStateEvents[gomatrixserverlib.StateKeyTuple{
				EventType: e.Type(),
				StateKey:  *e.StateKey(),
			}] = h
		}
	}
}

type testRoomserverAPI struct {
	rsAPI.RoomserverInternalAPI
	inputRoomEvents           []rsAPI.InputRoomEvent
	queryStateAfterEvents     func(*rsAPI.QueryStateAfterEventsRequest) rsAPI.QueryStateAfterEventsResponse
	queryEventsByID           func(req *rsAPI.QueryEventsByIDRequest) rsAPI.QueryEventsByIDResponse
	queryLatestEventsAndState func(*rsAPI.QueryLatestEventsAndStateRequest) rsAPI.QueryLatestEventsAndStateResponse
}

func (t *testRoomserverAPI) QueryUserIDForSender(_ context.Context, _ spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(senderID), true)
}

func (t *testRoomserverAPI) InputRoomEvents(
	_ context.Context,
	request *rsAPI.InputRoomEventsRequest,
	_ *rsAPI.InputRoomEventsResponse,
) {
	t.inputRoomEvents = append(t.inputRoomEvents, request.InputRoomEvents...)
	for _, ire := range request.InputRoomEvents {
		fmt.Println("InputRoomEvents: ", ire.Event.EventID())
	}
}

// Query the latest events and state for a room from the room server.
func (t *testRoomserverAPI) QueryLatestEventsAndState(
	_ context.Context,
	request *rsAPI.QueryLatestEventsAndStateRequest,
	response *rsAPI.QueryLatestEventsAndStateResponse,
) error {
	r := t.queryLatestEventsAndState(request)
	response.RoomExists = r.RoomExists
	response.RoomVersion = testRoomVersion
	response.LatestEvents = r.LatestEvents
	response.StateEvents = r.StateEvents
	response.Depth = r.Depth
	return nil
}

// Query the state after a list of events in a room from the room server.
func (t *testRoomserverAPI) QueryStateAfterEvents(
	_ context.Context,
	request *rsAPI.QueryStateAfterEventsRequest,
	response *rsAPI.QueryStateAfterEventsResponse,
) error {
	response.RoomVersion = testRoomVersion
	res := t.queryStateAfterEvents(request)
	response.PrevEventsExist = res.PrevEventsExist
	response.RoomExists = res.RoomExists
	response.StateEvents = res.StateEvents
	return nil
}

// Query a list of events by event ID.
func (t *testRoomserverAPI) QueryEventsByID(
	_ context.Context,
	request *rsAPI.QueryEventsByIDRequest,
	response *rsAPI.QueryEventsByIDResponse,
) error {
	res := t.queryEventsByID(request)
	response.Events = res.Events
	return nil
}

// Query if a server is joined to a room
func (t *testRoomserverAPI) QueryServerJoinedToRoom(
	_ context.Context,
	_ *rsAPI.QueryServerJoinedToRoomRequest,
	response *rsAPI.QueryServerJoinedToRoomResponse,
) error {
	response.RoomExists = true
	response.IsInRoom = true
	return nil
}

// Asks for the room version for a given room.
func (t *testRoomserverAPI) QueryRoomVersionForRoom(
	_ context.Context,
	_ string,
) (gomatrixserverlib.RoomVersion, error) {
	return testRoomVersion, nil
}

func (t *testRoomserverAPI) QueryServerBannedFromRoom(
	_ context.Context, _ *rsAPI.QueryServerBannedFromRoomRequest, res *rsAPI.QueryServerBannedFromRoomResponse,
) error {
	res.Banned = false
	return nil
}

func mustCreateTransaction(rsAPI rsAPI.FederationRoomserverAPI, pdus []json.RawMessage) *TxnReq {
	t := NewTxnReq(
		rsAPI,
		nil,
		"",
		&test.NopJSONVerifier{},
		NewMutexByRoom(),
		nil,
		false,
		pdus,
		nil,
		testOrigin,
		gomatrixserverlib.TransactionID(fmt.Sprintf("%d", time.Now().UnixNano())),
		testDestination)
	t.PDUs = pdus
	t.Origin = testOrigin
	t.TransactionID = gomatrixserverlib.TransactionID(fmt.Sprintf("%d", time.Now().UnixNano()))
	t.Destination = testDestination
	return &t
}

func mustProcessTransaction(t *testing.T, txn *TxnReq, pdusWithErrors []string) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	res, err := txn.ProcessTransaction(ctx)
	if err != nil {
		t.Errorf("txn.processTransaction returned an error: %v", err)
		return
	}
	if len(res.PDUs) != len(txn.PDUs) {
		t.Errorf("txn.processTransaction did not return results for all PDUs, got %d want %d", len(res.PDUs), len(txn.PDUs))
		return
	}
NextPDU:
	for eventID, result := range res.PDUs {
		if result.Error == "" {
			continue
		}
		for _, eventIDWantError := range pdusWithErrors {
			if eventID == eventIDWantError {
				break NextPDU
			}
		}
		t.Errorf("txn.processTransaction PDU %s returned an error %s", eventID, result.Error)
	}
}

func assertInputRoomEvents(t *testing.T, got []rsAPI.InputRoomEvent, want []*rstypes.HeaderedEvent) {
	for _, g := range got {
		fmt.Println("GOT ", g.Event.EventID())
	}
	if len(got) != len(want) {
		t.Errorf("wrong number of InputRoomEvents: got %d want %d", len(got), len(want))
		return
	}
	for i := range got {
		if got[i].Event.EventID() != want[i].EventID() {
			t.Errorf("InputRoomEvents[%d] got %s want %s", i, got[i].Event.EventID(), want[i].EventID())
		}
	}
}

// The purpose of this test is to check that receiving an event over federation for which we have the prev_events works correctly, and passes it on
// to the roomserver. It's the most basic test possible.
func TestBasicTransaction(t *testing.T) {
	rsAPIv := &testRoomserverAPI{}
	pdus := []json.RawMessage{
		testData[len(testData)-1], // a message event
	}
	txn := mustCreateTransaction(rsAPIv, pdus)
	mustProcessTransaction(t, txn, nil)
	assertInputRoomEvents(t, rsAPIv.inputRoomEvents, []*rstypes.HeaderedEvent{testEvents[len(testEvents)-1]})
}

// The purpose of this test is to check that if the event received fails auth checks the event is still sent to the roomserver
// as it does the auth check.
func TestTransactionFailAuthChecks(t *testing.T) {
	rsAPIv := &testRoomserverAPI{}
	pdus := []json.RawMessage{
		testData[len(testData)-1], // a message event
	}
	txn := mustCreateTransaction(rsAPIv, pdus)
	mustProcessTransaction(t, txn, []string{})
	// expect message to be sent to the roomserver
	assertInputRoomEvents(t, rsAPIv.inputRoomEvents, []*rstypes.HeaderedEvent{testEvents[len(testEvents)-1]})
}
