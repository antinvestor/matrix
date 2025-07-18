// Copyright 2017 Vector Creations Ltd
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

package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antinvestor/gomatrix"
	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/statistics"
	"github.com/antinvestor/matrix/federationapi/storage"
	"github.com/antinvestor/matrix/federationapi/storage/shared/receipt"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/pitabwire/util"
)

const (
	maxPDUsPerTransaction = 50
	maxEDUsPerTransaction = 100
	maxPDUsInMemory       = 128
	maxEDUsInMemory       = 128
	queueIdleTimeout      = time.Second * 30
)

// destinationQueue is a queue of events for a single destination.
// It is responsible for sending the events to the destination and
// ensures that only one request is in flight to a given destination
// at a time.
type destinationQueue struct {
	queues             *OutgoingQueues
	db                 storage.Database
	signing            map[spec.ServerName]*fclient.SigningIdentity
	client             fclient.FederationClient        // federation client
	origin             spec.ServerName                 // origin of requests
	destination        spec.ServerName                 // destination of requests
	running            atomic.Bool                     // is the queue worker running?
	backingOff         atomic.Bool                     // true if we're backing off
	overflowed         atomic.Bool                     // the queues exceed maxPDUsInMemory/maxEDUsInMemory, so we should consult the database for more
	statistics         *statistics.ServerStatistics    // statistics about this remote server
	transactionIDMutex sync.Mutex                      // protects transactionID
	transactionID      gomatrixserverlib.TransactionID // last transaction ID if retrying, or "" if last txn was successful
	notify             chan struct{}                   // interrupts idle wait pending PDUs/EDUs
	pendingPDUs        []*queuedPDU                    // PDUs waiting to be sent
	pendingEDUs        []*queuedEDU                    // EDUs waiting to be sent
	pendingMutex       sync.RWMutex                    // protects pendingPDUs and pendingEDUs
}

// Send event adds the event to the pending queue for the destination.
// If the queue is empty then it starts a background goroutine to
// start sending events to that destination.
func (oq *destinationQueue) sendEvent(ctx context.Context, event *types.HeaderedEvent, dbReceipt *receipt.Receipt) {
	if event == nil {
		util.Log(ctx).Error("attempt to send nil PDU with destination %q", oq.destination)
		return
	}

	// Check if the destination is blacklisted. If it isn't then wake
	// up the queue.
	if !oq.statistics.Blacklisted() {
		// If there's room in memory to hold the event then add it to the
		// list.
		oq.pendingMutex.Lock()
		if len(oq.pendingPDUs) < maxPDUsInMemory {
			oq.pendingPDUs = append(oq.pendingPDUs, &queuedPDU{
				pdu:       event,
				dbReceipt: dbReceipt,
			})
		} else {
			oq.overflowed.Store(true)
		}
		oq.pendingMutex.Unlock()

		if !oq.backingOff.Load() {
			oq.wakeQueueAndNotify(ctx)
		}
	}
}

// sendEDU adds the EDU event to the pending queue for the destination.
// If the queue is empty then it starts a background goroutine to
// start sending events to that destination.
func (oq *destinationQueue) sendEDU(ctx context.Context, event *gomatrixserverlib.EDU, dbReceipt *receipt.Receipt) {
	if event == nil {
		util.Log(ctx).Error("attempt to send nil EDU with destination %q", oq.destination)
		return
	}

	// Check if the destination is blacklisted. If it isn't then wake
	// up the queue.
	if !oq.statistics.Blacklisted() {
		// If there's room in memory to hold the event then add it to the
		// list.
		oq.pendingMutex.Lock()
		if len(oq.pendingEDUs) < maxEDUsInMemory {
			oq.pendingEDUs = append(oq.pendingEDUs, &queuedEDU{
				edu:       event,
				dbReceipt: dbReceipt,
			})
		} else {
			oq.overflowed.Store(true)
		}
		oq.pendingMutex.Unlock()

		if !oq.backingOff.Load() {
			oq.wakeQueueAndNotify(ctx)
		}
	}
}

// handleBackoffNotifier is registered as the backoff notification
// callback with Statistics. It will wakeup and notify the queue
// if the queue is currently backing off.
func (oq *destinationQueue) handleBackoffNotifier(ctx context.Context) {
	// Only wake up the queue if it is backing off.
	// Otherwise there is no pending work for the queue to handle
	// so waking the queue would be a waste of resources.
	if oq.backingOff.Load() {
		oq.wakeQueueAndNotify(ctx)
	}
}

// wakeQueueIfEventsPending calls wakeQueueAndNotify only if there are
// pending events or if forceWakeup is true. This prevents starting the
// queue unnecessarily.
func (oq *destinationQueue) wakeQueueIfEventsPending(ctx context.Context, forceWakeup bool) {
	eventsPending := func() bool {
		oq.pendingMutex.Lock()
		defer oq.pendingMutex.Unlock()
		return len(oq.pendingPDUs) > 0 || len(oq.pendingEDUs) > 0
	}

	// NOTE : Only wakeup and notify the queue if there are pending events
	// or if forceWakeup is true. Otherwise there is no reason to start the
	// queue goroutine and waste resources.
	if forceWakeup || eventsPending() {
		util.Log(ctx).Info("Starting queue due to pending events or forceWakeup")
		oq.wakeQueueAndNotify(ctx)
	}
}

// wakeQueueAndNotify ensures the destination queue is running and notifies it
// that there is pending work.
func (oq *destinationQueue) wakeQueueAndNotify(ctx context.Context) {
	// NOTE : Send notification before waking queue to prevent a race
	// where the queue was running and stops due to a timeout in between
	// checking it and sending the notification.

	// Notify the queue that there are events ready to send.
	select {
	case oq.notify <- struct{}{}:
	default:
	}

	// Wake up the queue if it's asleep.
	oq.wakeQueueIfNeeded(ctx)
}

// wakeQueueIfNeeded will wake up the destination queue if it is
// not already running.
func (oq *destinationQueue) wakeQueueIfNeeded(ctx context.Context) {
	// Clear the backingOff flag and update the backoff metrics if it was set.
	if oq.backingOff.CompareAndSwap(true, false) {
		destinationQueueBackingOff.Dec()
	}

	// If we aren't running then wake up the queue.
	if !oq.running.Load() {
		// Start the queue.
		go oq.backgroundSend(ctx)
	}
}

// getPendingFromDatabase will look at the database and see if
// there are any persisted events that haven't been sent to this
// destination yet. If so, they will be queued up.
func (oq *destinationQueue) getPendingFromDatabase(ctx context.Context) {
	// Check to see if there's anything to do for this server
	// in the database.
	retrieved := false
	oq.pendingMutex.Lock()
	defer oq.pendingMutex.Unlock()

	// Take a note of all of the PDUs and EDUs that we already
	// have cached. We will index them based on the receipt,
	// which ultimately just contains the index of the PDU/EDU
	// in the database.
	gotPDUs := map[string]struct{}{}
	gotEDUs := map[string]struct{}{}
	for _, pdu := range oq.pendingPDUs {
		gotPDUs[pdu.dbReceipt.String()] = struct{}{}
	}
	for _, edu := range oq.pendingEDUs {
		gotEDUs[edu.dbReceipt.String()] = struct{}{}
	}

	overflowed := false
	if pduCapacity := maxPDUsInMemory - len(oq.pendingPDUs); pduCapacity > 0 {
		// We have room in memory for some PDUs - let's request no more than that.
		if pdus, err := oq.db.GetPendingPDUs(ctx, oq.destination, maxPDUsInMemory); err == nil {
			if len(pdus) == maxPDUsInMemory {
				overflowed = true
			}
			for pduReceipt, pdu := range pdus {
				if _, ok := gotPDUs[pduReceipt.String()]; ok {
					continue
				}
				oq.pendingPDUs = append(oq.pendingPDUs, &queuedPDU{pduReceipt, pdu})
				retrieved = true
				if len(oq.pendingPDUs) == maxPDUsInMemory {
					break
				}
			}
		} else {
			util.Log(ctx).WithError(err).Error("Failed to get pending PDUs for %q", oq.destination)
		}
	}

	if eduCapacity := maxEDUsInMemory - len(oq.pendingEDUs); eduCapacity > 0 {
		// We have room in memory for some EDUs - let's request no more than that.
		if edus, err := oq.db.GetPendingEDUs(ctx, oq.destination, maxEDUsInMemory); err == nil {
			if len(edus) == maxEDUsInMemory {
				overflowed = true
			}
			for eduReceipt, edu := range edus {
				if _, ok := gotEDUs[eduReceipt.String()]; ok {
					continue
				}
				oq.pendingEDUs = append(oq.pendingEDUs, &queuedEDU{eduReceipt, edu})
				retrieved = true
				if len(oq.pendingEDUs) == maxEDUsInMemory {
					break
				}
			}
		} else {
			util.Log(ctx).WithError(err).Error("Failed to get pending EDUs for %q", oq.destination)
		}
	}

	// If we've retrieved all of the events from the database with room to spare
	// in memory then we'll no longer consider this queue to be overflowed.
	if !overflowed {
		oq.overflowed.Store(false)
	} else {
	}
	// If we've retrieved some events then notify the destination queue goroutine.
	if retrieved {
		select {
		case oq.notify <- struct{}{}:
		default:
		}
	}
}

// checkNotificationsOnClose checks for any remaining notifications
// and starts a new backgroundSend goroutine if any exist.
func (oq *destinationQueue) checkNotificationsOnClose(ctx context.Context) {
	// NOTE : If we are stopping the queue due to blacklist then it
	// doesn't matter if we have been notified of new work since
	// this queue instance will be deleted anyway.
	if !oq.statistics.Blacklisted() {
		select {
		case <-oq.notify:
			// We received a new notification in between the
			// idle timeout firing and stopping the goroutine.
			// Immediately restart the queue.
			oq.wakeQueueAndNotify(ctx)
		default:
		}
	}
}

// backgroundSend is the worker goroutine for sending events.
func (oq *destinationQueue) backgroundSend(ctx context.Context) {
	// Check if a worker is already running, and if it isn't, then
	// mark it as started.
	if !oq.running.CompareAndSwap(false, true) {
		return
	}

	// Register queue cleanup functions.
	// NOTE : The ordering here is very intentional.
	defer oq.checkNotificationsOnClose(ctx)
	defer oq.running.Store(false)

	destinationQueueRunning.Inc()
	defer destinationQueueRunning.Dec()

	idleTimeout := time.NewTimer(queueIdleTimeout)
	defer idleTimeout.Stop()

	// Mark the queue as overflowed, so we will consult the database
	// to see if there's anything new to send.
	oq.overflowed.Store(true)

	for {
		// If we are overflowing memory and have sent things out to the
		// database then we can look up what those things are.
		if oq.overflowed.Load() {
			oq.getPendingFromDatabase(ctx)
		}

		// Reset the queue idle timeout.
		if !idleTimeout.Stop() {
			select {
			case <-idleTimeout.C:
			default:
			}
		}
		idleTimeout.Reset(queueIdleTimeout)

		// If we have nothing to do then wait either for incoming events, or
		// until we hit an idle timeout.
		select {
		case <-oq.notify:
			// There's work to do, either because getPendingFromDatabase
			// told us there is, a new event has come in via sendEvent/sendEDU,
			// or we are backing off and it is time to retry.
		case <-idleTimeout.C:
			// The worker is idle so stop the goroutine. It'll get
			// restarted automatically the next time we have an event to
			// send.
			return
		case <-ctx.Done():
			return
		}

		// Work out which PDUs/EDUs to include in the next transaction.
		oq.pendingMutex.RLock()
		pduCount := len(oq.pendingPDUs)
		eduCount := len(oq.pendingEDUs)
		if pduCount > maxPDUsPerTransaction {
			pduCount = maxPDUsPerTransaction
		}
		if eduCount > maxEDUsPerTransaction {
			eduCount = maxEDUsPerTransaction
		}
		toSendPDUs := oq.pendingPDUs[:pduCount]
		toSendEDUs := oq.pendingEDUs[:eduCount]
		oq.pendingMutex.RUnlock()

		// If we didn't get anything from the database and there are no
		// pending EDUs then there's nothing to do - stop here.
		if pduCount == 0 && eduCount == 0 {
			continue
		}

		// If we have pending PDUs or EDUs then construct a transaction.
		// Try sending the next transaction and see what happens.
		sendMethod, terr := oq.nextTransaction(ctx, toSendPDUs, toSendEDUs)
		if terr != nil {
			// We failed to send the transaction. Mark it as a failure.
			_, blacklisted := oq.statistics.Failure(ctx)
			if !blacklisted {
				// Register the backoff state and exit the goroutine.
				// It'll get restarted automatically when the backoff
				// completes.
				oq.backingOff.Store(true)
				destinationQueueBackingOff.Inc()
				return
			} else {
				// Immediately trigger the blacklist logic.
				oq.blacklistDestination(ctx)
				return
			}
		} else {
			oq.handleTransactionSuccess(ctx, pduCount, eduCount, sendMethod)
		}
	}
}

// nextTransaction creates a new transaction from the pending event
// queue and sends it.
// Returns an error if the transaction wasn't sent. And whether the success
// was to a relay server or not.
func (oq *destinationQueue) nextTransaction(ctx context.Context,
	pdus []*queuedPDU,
	edus []*queuedEDU,
) (sendMethod statistics.SendMethod, err error) {
	// Create the transaction.
	t, pduReceipts, eduReceipts := oq.createTransaction(pdus, edus)
	util.Log(ctx).WithField("server_name", oq.destination).Debug("Sending transaction %q containing %d PDUs, %d EDUs", t.TransactionID, len(t.PDUs), len(t.EDUs))

	// Try to send the transaction to the destination server.
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	relayServers := oq.statistics.KnownRelayServers()
	hasRelayServers := len(relayServers) > 0
	shouldSendToRelays := oq.statistics.AssumedOffline() && hasRelayServers
	if !shouldSendToRelays {
		sendMethod = statistics.SendDirect
		_, err = oq.client.SendTransaction(ctx, t)
	} else {
		// Try sending directly to the destination first in case they came back online.
		sendMethod = statistics.SendDirect
		_, err = oq.client.SendTransaction(ctx, t)
		if err != nil {
			// The destination is still offline, try sending to relays.
			sendMethod = statistics.SendViaRelay
			relaySuccess := false
			util.Log(ctx).Info("Sending %q to relay servers: %v", t.TransactionID, relayServers)
			// TODO : how to pass through actual userID here?!?!?!?!
			userID, userErr := spec.NewUserID("@user:"+string(oq.destination), false)
			if userErr != nil {
				return sendMethod, userErr
			}

			// Attempt sending to each known relay server.
			for _, relayServer := range relayServers {
				_, relayErr := oq.client.P2PSendTransactionToRelay(ctx, *userID, t, relayServer)
				if relayErr != nil {
					err = relayErr
				} else {
					// If sending to one of the relay servers succeeds, consider the send successful.
					relaySuccess = true

					// TODO : what about if the dest comes back online but can't see their relay?
					// How do I sync with the dest in that case?
					// Should change the database to have a "relay success" flag on events and if
					// I see the node back online, maybe directly send through the backlog of events
					// with "relay success"... could lead to duplicate events, but only those that
					// I sent. And will lead to a much more consistent experience.
				}
			}

			// Clear the error if sending to any of the relay servers succeeded.
			if relaySuccess {
				err = nil
			}
		}
	}
	switch errResponse := err.(type) {
	case nil:
		// Clean up the transaction in the database.
		if pduReceipts != nil {
			// util.Log(ctx).Info("Cleaning PDUs %q", pduReceipt.String())
			if err = oq.db.CleanPDUs(ctx, oq.destination, pduReceipts); err != nil {
				util.Log(ctx).WithError(err).Error("Failed to clean PDUs for server %q", t.Destination)
			}
		}
		if eduReceipts != nil {
			// util.Log(ctx).Info("Cleaning EDUs %q", eduReceipt.String())
			if err = oq.db.CleanEDUs(ctx, oq.destination, eduReceipts); err != nil {
				util.Log(ctx).WithError(err).Error("Failed to clean EDUs for server %q", t.Destination)
			}
		}
		// Reset the transaction ID.
		oq.transactionIDMutex.Lock()
		oq.transactionID = ""
		oq.transactionIDMutex.Unlock()
		return sendMethod, nil
	case gomatrix.HTTPError:
		// Report that we failed to send the transaction and we
		// will retry again, subject to backoff.

		// TODO: we should check for 500-ish fails vs 400-ish here,
		// since we shouldn't queue things indefinitely in response
		// to a 400-ish error
		code := errResponse.Code
		util.Log(ctx).Debug("Transaction failed with HTTP", code)
		return sendMethod, err
	default:
		util.Log(ctx).WithError(err).
			WithField("destination", oq.destination).
			Debug("Failed to send transaction %q", t.TransactionID)
		return sendMethod, err
	}
}

// createTransaction generates a gomatrixserverlib.Transaction from the provided pdus and edus.
// It also returns the associated event receipts so they can be cleaned from the database in
// the case of a successful transaction.
func (oq *destinationQueue) createTransaction(
	pdus []*queuedPDU,
	edus []*queuedEDU,
) (gomatrixserverlib.Transaction, []*receipt.Receipt, []*receipt.Receipt) {
	// If there's no projected transaction ID then generate one. If
	// the transaction succeeds then we'll set it back to "" so that
	// we generate a new one next time. If it fails, we'll preserve
	// it so that we retry with the same transaction ID.
	oq.transactionIDMutex.Lock()
	if oq.transactionID == "" {
		now := spec.AsTimestamp(time.Now())
		oq.transactionID = gomatrixserverlib.TransactionID(fmt.Sprintf("%d-%d", now, oq.statistics.SuccessCount()))
	}
	oq.transactionIDMutex.Unlock()

	t := gomatrixserverlib.Transaction{
		PDUs: []json.RawMessage{},
		EDUs: []gomatrixserverlib.EDU{},
	}
	t.Origin = oq.origin
	t.Destination = oq.destination
	t.OriginServerTS = spec.AsTimestamp(time.Now())
	t.TransactionID = oq.transactionID

	var pduReceipts []*receipt.Receipt
	var eduReceipts []*receipt.Receipt

	// Go through PDUs that we retrieved from the database, if any,
	// and add them into the transaction.
	for _, pdu := range pdus {
		// These should never be nil.
		if pdu == nil || pdu.pdu == nil {
			continue
		}
		// Append the JSON of the event, since this is a json.RawMessage type in the
		// gomatrixserverlib.Transaction struct
		t.PDUs = append(t.PDUs, pdu.pdu.JSON())
		pduReceipts = append(pduReceipts, pdu.dbReceipt)
	}

	// Do the same for pending EDUS in the queue.
	for _, edu := range edus {
		// These should never be nil.
		if edu == nil || edu.edu == nil {
			continue
		}
		t.EDUs = append(t.EDUs, *edu.edu)
		eduReceipts = append(eduReceipts, edu.dbReceipt)
	}

	return t, pduReceipts, eduReceipts
}

// blacklistDestination removes all pending PDUs and EDUs that have been cached
// and deletes this queue.
func (oq *destinationQueue) blacklistDestination(ctx context.Context) {
	// It's been suggested that we should give up because the backoff
	// has exceeded a maximum allowable value. Clean up the in-memory
	// buffers at this point. The PDU clean-up is already on a defer.
	util.Log(ctx).Warn("Blacklisting %q due to exceeding backoff threshold", oq.destination)

	oq.pendingMutex.Lock()
	for i := range oq.pendingPDUs {
		oq.pendingPDUs[i] = nil
	}
	for i := range oq.pendingEDUs {
		oq.pendingEDUs[i] = nil
	}
	oq.pendingPDUs = nil
	oq.pendingEDUs = nil
	oq.pendingMutex.Unlock()

	// Delete this queue as no more messages will be sent to this
	// destination until it is no longer blacklisted.
	oq.statistics.AssignBackoffNotifier(nil)
	oq.queues.clearQueue(oq)
}

// handleTransactionSuccess updates the cached event queues as well as the success and
// backoff information for this server.
func (oq *destinationQueue) handleTransactionSuccess(ctx context.Context, pduCount int, eduCount int, sendMethod statistics.SendMethod) {
	// If we successfully sent the transaction then clear out
	// the pending events and EDUs, and wipe our transaction ID.

	oq.statistics.Success(ctx, sendMethod)
	oq.pendingMutex.Lock()
	defer oq.pendingMutex.Unlock()

	for i := range oq.pendingPDUs[:pduCount] {
		oq.pendingPDUs[i] = nil
	}
	for i := range oq.pendingEDUs[:eduCount] {
		oq.pendingEDUs[i] = nil
	}
	oq.pendingPDUs = oq.pendingPDUs[pduCount:]
	oq.pendingEDUs = oq.pendingEDUs[eduCount:]

	if len(oq.pendingPDUs) > 0 || len(oq.pendingEDUs) > 0 {
		select {
		case oq.notify <- struct{}{}:
		default:
		}
	}
}
