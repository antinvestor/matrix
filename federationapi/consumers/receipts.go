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

package consumers

import (
	"context"
	"encoding/json"
	"github.com/antinvestor/matrix/internal/queueutil"
	"strconv"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/queue"
	"github.com/antinvestor/matrix/federationapi/storage"
	fedTypes "github.com/antinvestor/matrix/federationapi/types"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame"

	syncTypes "github.com/antinvestor/matrix/syncapi/types"
)

// OutputReceiptConsumer consumes events that originate in the clientapi.
type OutputReceiptConsumer struct {
	qm                queueutil.QueueManager
	db                storage.Database
	queues            *queue.OutgoingQueues
	isLocalServerName func(spec.ServerName) bool
}

// NewOutputReceiptConsumer creates a new OutputReceiptConsumer. Call Start() to begin consuming typing events.
func NewOutputReceiptConsumer(
	ctx context.Context,
	cfg *config.FederationAPI,
	qm queueutil.QueueManager,
	queues *queue.OutgoingQueues,
	store storage.Database,
) error {
	c := &OutputReceiptConsumer{
		qm:                qm,
		queues:            queues,
		db:                store,
		isLocalServerName: cfg.Global.IsLocalServerName,
	}

	return qm.RegisterSubscriber(ctx, &cfg.Queues.OutputReceiptEvent, c)
}

// Handle is called in response to a message received on the receipt
// events topic from the client api.
func (t *OutputReceiptConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {

	receipt := syncTypes.OutputReceiptEvent{
		UserID:  metadata[queueutil.UserID],
		RoomID:  metadata[queueutil.RoomID],
		EventID: metadata[queueutil.EventID],
		Type:    metadata["type"],
	}

	switch receipt.Type {
	case "m.read":
		// These are allowed to be sent over federation
	case "m.read.private", "m.fully_read":
		// These must not be sent over federation
		return nil
	}

	// only send receipt events which originated from us
	_, receiptServerName, err := gomatrixserverlib.SplitID('@', receipt.UserID)
	if err != nil {
		frame.Log(ctx).WithError(err).
			WithField("component", "federation_consumer").
			WithField("user_id", receipt.UserID).
			Error("failed to extract domain from receipt sender")
		return nil
	}
	if !t.isLocalServerName(receiptServerName) {
		return nil
	}

	timestamp, err := strconv.ParseUint(metadata["timestamp"], 10, 64)
	if err != nil {
		// If the message was invalid, log it and move on to the next message in the stream
		frame.Log(ctx).WithError(err).
			WithField("component", "federation_consumer").
			Error("EDU output log: message parse failure")
		return err
	}

	receipt.Timestamp = spec.Timestamp(timestamp)

	joined, err := t.db.GetJoinedHosts(ctx, receipt.RoomID)
	if err != nil {
		frame.Log(ctx).WithError(err).
			WithField("component", "federation_consumer").
			WithField("room_id", receipt.RoomID).
			Error("failed to get joined hosts for room")
		return err
	}

	names := make([]spec.ServerName, len(joined))
	for i := range joined {
		names[i] = joined[i].ServerName
	}

	content := map[string]fedTypes.FederationReceiptMRead{}
	content[receipt.RoomID] = fedTypes.FederationReceiptMRead{
		User: map[string]fedTypes.FederationReceiptData{
			receipt.UserID: {
				Data: fedTypes.ReceiptTS{
					TS: receipt.Timestamp,
				},
				EventIDs: []string{receipt.EventID},
			},
		},
	}

	edu := &gomatrixserverlib.EDU{
		Type:   spec.MReceipt,
		Origin: string(receiptServerName),
	}
	if edu.Content, err = json.Marshal(content); err != nil {
		frame.Log(ctx).WithError(err).
			WithField("component", "federation_consumer").
			Error("failed to marshal EDU JSON")
		return nil
	}

	err = t.queues.SendEDU(ctx, edu, receiptServerName, names)
	if err != nil {
		frame.Log(ctx).WithError(err).
			WithField("component", "federation_consumer").
			Error("failed to send EDU")
		return err
	}

	return nil
}
