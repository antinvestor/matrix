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
	"errors"
	"fmt"
	"sync"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/producers"
	"github.com/antinvestor/matrix/federationapi/types"
	"github.com/antinvestor/matrix/roomserver/api"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
	syncTypes "github.com/antinvestor/matrix/syncapi/types"
	userAPI "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	PDUCountTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "matrix",
			Subsystem: "federationapi",
			Name:      "recv_pdus",
			Help:      "Number of incoming PDUs from remote servers with labels for success",
		},
		[]string{"status"}, // 'success' or 'total'
	)
	EDUCountTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "matrix",
			Subsystem: "federationapi",
			Name:      "recv_edus",
			Help:      "Number of incoming EDUs from remote servers",
		},
	)
)

type TxnReq struct {
	gomatrixserverlib.Transaction
	rsAPI                  api.FederationRoomserverAPI
	userAPI                userAPI.FederationUserAPI
	ourServerName          spec.ServerName
	keys                   gomatrixserverlib.JSONVerifier
	roomsMu                *MutexByRoom
	producer               *producers.SyncAPIProducer
	inboundPresenceEnabled bool
}

func NewTxnReq(
	rsAPI api.FederationRoomserverAPI,
	userAPI userAPI.FederationUserAPI,
	ourServerName spec.ServerName,
	keys gomatrixserverlib.JSONVerifier,
	roomsMu *MutexByRoom,
	producer *producers.SyncAPIProducer,
	inboundPresenceEnabled bool,
	pdus []json.RawMessage,
	edus []gomatrixserverlib.EDU,
	origin spec.ServerName,
	transactionID gomatrixserverlib.TransactionID,
	destination spec.ServerName,
) TxnReq {
	t := TxnReq{
		rsAPI:                  rsAPI,
		userAPI:                userAPI,
		ourServerName:          ourServerName,
		keys:                   keys,
		roomsMu:                roomsMu,
		producer:               producer,
		inboundPresenceEnabled: inboundPresenceEnabled,
	}

	t.PDUs = pdus
	t.EDUs = edus
	t.Origin = origin
	t.TransactionID = transactionID
	t.Destination = destination

	return t
}

func (t *TxnReq) ProcessTransaction(ctx context.Context) (*fclient.RespSend, *util.JSONResponse) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if t.producer != nil {
			t.processEDUs(ctx)
		}
	}()

	results := make(map[string]fclient.PDUResult)
	roomVersions := make(map[string]gomatrixserverlib.RoomVersion)
	getRoomVersion := func(roomID string) gomatrixserverlib.RoomVersion {
		if v, ok := roomVersions[roomID]; ok {
			return v
		}

		roomVersion, err := t.rsAPI.QueryRoomVersionForRoom(ctx, roomID)
		if err != nil {
			util.Log(ctx).WithError(err).Debug("Transaction: Failed to query room version for room", roomID)
			return ""
		}
		roomVersions[roomID] = roomVersion
		return roomVersion
	}

	for _, pdu := range t.PDUs {
		PDUCountTotal.WithLabelValues("total").Inc()
		var header struct {
			RoomID string `json:"room_id"`
		}
		if err := json.Unmarshal(pdu, &header); err != nil {
			util.Log(ctx).WithError(err).Debug("Transaction: Failed to extract room ID from event")
			// We don't know the event ID at this point so we can't return the
			// failure in the PDU results
			continue
		}
		roomVersion := getRoomVersion(header.RoomID)
		verImpl, err := gomatrixserverlib.GetRoomVersion(roomVersion)
		if err != nil {
			continue
		}
		event, err := verImpl.NewEventFromUntrustedJSON(pdu)
		if err != nil {
			var badJSONError gomatrixserverlib.BadJSONError
			if errors.As(err, &badJSONError) {
				// Room version 6 states that homeservers should strictly enforce canonical JSON
				// on PDUs.
				//
				// This enforces that the entire transaction is rejected if a single bad PDU is
				// sent. It is unclear if this is the correct behaviour or not.
				//
				// See https://github.com/matrix-org/synapse/issues/7543
				return nil, &util.JSONResponse{
					Code: 400,
					JSON: spec.BadJSON("PDU contains bad JSON"),
				}
			}
			util.Log(ctx).WithError(err).Debug("Transaction: Failed to parse event JSON of event %s", string(pdu))
			continue
		}
		if event.Type() == spec.MRoomCreate && event.StateKeyEquals("") {
			continue
		}
		if api.IsServerBannedFromRoom(ctx, t.rsAPI, event.RoomID().String(), t.Origin) {
			results[event.EventID()] = fclient.PDUResult{
				Error: "Forbidden by server ACLs",
			}
			continue
		}
		if err = gomatrixserverlib.VerifyEventSignatures(ctx, event, t.keys, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return t.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		}); err != nil {
			util.Log(ctx).WithError(err).Debug("Transaction: Couldn't validate signature of event %q", event.EventID())
			results[event.EventID()] = fclient.PDUResult{
				Error: err.Error(),
			}
			continue
		}

		// pass the event to the roomserver which will do auth checks
		// If the event fail auth checks, gmsl.NotAllowed error will be returned which we be silently
		// discarded by the caller of this function
		err = api.SendEvents(
			ctx,
			t.rsAPI,
			api.KindNew,
			[]*rstypes.HeaderedEvent{
				{PDU: event},
			},
			t.Destination,
			t.Origin,
			api.DoNotSendToOtherServers,
			nil,
			true,
		)
		if err != nil {
			util.Log(ctx).WithError(err).Error("Transaction: Couldn't submit event %q to input queue: %s", event.EventID(), err)
			results[event.EventID()] = fclient.PDUResult{
				Error: err.Error(),
			}
			continue
		}

		results[event.EventID()] = fclient.PDUResult{}
		PDUCountTotal.WithLabelValues("success").Inc()
	}

	wg.Wait()
	return &fclient.RespSend{PDUs: results}, nil
}

// nolint:gocyclo
func (t *TxnReq) processEDUs(ctx context.Context) {
	log := util.Log(ctx)
	for _, e := range t.EDUs {
		EDUCountTotal.Inc()
		switch e.Type {
		case spec.MTyping:
			// https://matrix.org/docs/spec/server_server/latest#typing-notifications
			var typingPayload struct {
				RoomID string `json:"room_id"`
				UserID string `json:"user_id"`
				Typing bool   `json:"typing"`
			}
			if err := json.Unmarshal(e.Content, &typingPayload); err != nil {
				log.WithError(err).Debug("Failed to unmarshal typing event")
				continue
			}
			if _, serverName, err := gomatrixserverlib.SplitID('@', typingPayload.UserID); err != nil {
				continue
			} else if serverName == t.ourServerName {
				continue
			} else if serverName != t.Origin {
				continue
			}
			if err := t.producer.SendTyping(ctx, typingPayload.UserID, typingPayload.RoomID, typingPayload.Typing, 30*1000); err != nil {
				log.WithError(err).Error("Failed to send typing event to JetStream")
			}
		case spec.MDirectToDevice:
			// https://matrix.org/docs/spec/server_server/r0.1.3#m-direct-to-device-schema
			var directPayload gomatrixserverlib.ToDeviceMessage
			if err := json.Unmarshal(e.Content, &directPayload); err != nil {
				log.WithError(err).Debug("Failed to unmarshal send-to-device events")
				continue
			}
			if _, serverName, err := gomatrixserverlib.SplitID('@', directPayload.Sender); err != nil {
				continue
			} else if serverName == t.ourServerName {
				continue
			} else if serverName != t.Origin {
				continue
			}
			for userIDStr, byUser := range directPayload.Messages {

				userID, err0 := spec.NewUserID(userIDStr, false)
				if err0 != nil {
					util.Log(ctx).WithError(err0).Error("processEDUs.NewUserID invalid userID")
					continue
				}

				for deviceID, message := range byUser {
					// TODO: check that the user and the device actually exist here
					err := t.producer.SendToDevice(ctx, directPayload.Sender, userID, deviceID, directPayload.Type, message)
					if err != nil {

						log.WithError(err).
							WithField("sender", directPayload.Sender).
							WithField("user_id", userID).
							WithField("device_id", deviceID).
							Error("Failed to send send-to-device event to JetStream")
					}
				}
			}
		case spec.MDeviceListUpdate:
			if err := t.producer.SendDeviceListUpdate(ctx, e.Content, t.Origin); err != nil {

				log.WithError(err).Error("failed to InputDeviceListUpdate")
			}
		case spec.MReceipt:
			// https://matrix.org/docs/spec/server_server/r0.1.4#receipts
			payload := map[string]types.FederationReceiptMRead{}

			if err := json.Unmarshal(e.Content, &payload); err != nil {
				log.WithError(err).Debug("Failed to unmarshal receipt event")
				continue
			}

			for roomID, receipt := range payload {
				for userID, mread := range receipt.User {
					_, domain, err := gomatrixserverlib.SplitID('@', userID)
					if err != nil {
						log.WithError(err).Debug("Failed to split domain from receipt event sender")
						continue
					}
					if t.Origin != domain {
						log.
							WithField("sender_domain", domain).
							WithField("origin", t.Origin).
							Debug("Dropping receipt event where sender domain doesn't match origin")
						continue
					}
					err = t.processReceiptEvent(ctx, userID, roomID, "m.read", mread.Data.TS, mread.EventIDs)
					if err != nil {
						log.WithError(err).
							WithField("sender", t.Origin).
							WithField("user_id", userID).
							WithField("room_id", roomID).
							WithField("events", mread.EventIDs).
							Error("Failed to send receipt event to JetStream")
						continue
					}
				}
			}
		case types.MSigningKeyUpdate:
			err := t.producer.SendSigningKeyUpdate(ctx, e.Content, t.Origin)
			if err != nil {
				log.WithError(err).Error("Failed to process signing key update")
			}
		case spec.MPresence:
			if t.inboundPresenceEnabled {
				err := t.processPresence(ctx, e)
				if err != nil {
					log.WithError(err).Error("Failed to process presence update")
				}
			}
		default:
			log.WithField("type", e.Type).Debug("Unhandled EDU")
		}
	}
}

// processPresence handles m.receipt events
func (t *TxnReq) processPresence(ctx context.Context, e gomatrixserverlib.EDU) error {
	payload := types.Presence{}
	if err := json.Unmarshal(e.Content, &payload); err != nil {
		return err
	}
	for _, content := range payload.Push {
		if _, serverName, err := gomatrixserverlib.SplitID('@', content.UserID); err != nil {
			continue
		} else if serverName == t.ourServerName {
			continue
		} else if serverName != t.Origin {
			continue
		}
		presence, ok := syncTypes.PresenceFromString(content.Presence)
		if !ok {
			continue
		}
		if err := t.producer.SendPresence(ctx, content.UserID, presence, content.StatusMsg, content.LastActiveAgo); err != nil {
			return err
		}
	}
	return nil
}

// processReceiptEvent sends receipt events to JetStream
func (t *TxnReq) processReceiptEvent(ctx context.Context,
	userID, roomID, receiptType string,
	timestamp spec.Timestamp,
	eventIDs []string,
) error {
	if _, serverName, err := gomatrixserverlib.SplitID('@', userID); err != nil {
		return nil
	} else if serverName == t.ourServerName {
		return nil
	} else if serverName != t.Origin {
		return nil
	}
	// store every event
	for _, eventID := range eventIDs {
		if err := t.producer.SendReceipt(ctx, userID, roomID, eventID, receiptType, timestamp); err != nil {
			return fmt.Errorf("unable to set receipt event: %w", err)
		}
	}

	return nil
}
