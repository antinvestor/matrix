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
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/pitabwire/util"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/antinvestor/matrix/federationapi/statistics"
	"github.com/antinvestor/matrix/federationapi/storage"
	"github.com/antinvestor/matrix/federationapi/storage/shared/receipt"
	"github.com/antinvestor/matrix/roomserver/types"
)

// OutgoingQueues is a collection of queues for sending transactions to other
// matrix servers
type OutgoingQueues struct {
	db          storage.Database
	disabled    bool
	origin      spec.ServerName
	client      fclient.FederationClient
	statistics  *statistics.Statistics
	signing     map[spec.ServerName]*fclient.SigningIdentity
	queuesMutex sync.Mutex // protects the below
	queues      map[spec.ServerName]*destinationQueue
}

func init() {
	prometheus.MustRegister(
		destinationQueueTotal, destinationQueueRunning,
		destinationQueueBackingOff,
	)
}

var destinationQueueTotal = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "matrix",
		Subsystem: "federationapi",
		Name:      "destination_queues_total",
	},
)

var destinationQueueRunning = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "matrix",
		Subsystem: "federationapi",
		Name:      "destination_queues_running",
	},
)

var destinationQueueBackingOff = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "matrix",
		Subsystem: "federationapi",
		Name:      "destination_queues_backing_off",
	},
)

// NewOutgoingQueues makes a new OutgoingQueues
func NewOutgoingQueues(
	ctx context.Context,
	db storage.Database,
	disabled bool,
	origin spec.ServerName,
	client fclient.FederationClient,
	statistics *statistics.Statistics,
	signing []*fclient.SigningIdentity,
) *OutgoingQueues {
	queues := &OutgoingQueues{
		disabled:   disabled,
		db:         db,
		origin:     origin,
		client:     client,
		statistics: statistics,
		signing:    map[spec.ServerName]*fclient.SigningIdentity{},
		queues:     map[spec.ServerName]*destinationQueue{},
	}
	for _, identity := range signing {
		queues.signing[identity.ServerName] = identity
	}
	// Look up which servers we have pending items for and then rehydrate those queues.
	if !disabled {
		serverNames := map[spec.ServerName]struct{}{}
		if names, err := db.GetPendingPDUServerNames(ctx); err == nil {
			for _, serverName := range names {
				serverNames[serverName] = struct{}{}
			}
		} else {
			util.Log(ctx).WithError(err).
				WithField("component", "federation_queue").
				Error("Failed to get PDU server names for destination queue hydration")
		}
		if names, err := db.GetPendingEDUServerNames(ctx); err == nil {
			for _, serverName := range names {
				serverNames[serverName] = struct{}{}
			}
		} else {
			util.Log(ctx).WithError(err).
				WithField("component", "federation_queue").
				Error("Failed to get EDU server names for destination queue hydration")
		}
		offset, step := time.Second*5, time.Second
		if maxVal := len(serverNames); maxVal > 120 {
			step = (time.Second * 120) / time.Duration(maxVal)
		}
		for serverName := range serverNames {
			if queue := queues.getQueue(ctx, serverName); queue != nil {
				time.AfterFunc(offset, func() {
					queue.wakeQueueIfNeeded(ctx)
				})
				offset += step
			}
		}
	}
	return queues
}

type queuedPDU struct {
	dbReceipt *receipt.Receipt
	pdu       *types.HeaderedEvent
}

type queuedEDU struct {
	dbReceipt *receipt.Receipt
	edu       *gomatrixserverlib.EDU
}

func (oqs *OutgoingQueues) getQueue(ctx context.Context, destination spec.ServerName) *destinationQueue {
	if oqs.statistics.ForServer(ctx, destination).Blacklisted() {
		return nil
	}
	oqs.queuesMutex.Lock()
	defer oqs.queuesMutex.Unlock()
	oq, ok := oqs.queues[destination]
	if !ok || oq == nil {
		destinationQueueTotal.Inc()
		oq = &destinationQueue{
			queues:      oqs,
			db:          oqs.db,
			origin:      oqs.origin,
			destination: destination,
			client:      oqs.client,
			statistics:  oqs.statistics.ForServer(ctx, destination),
			notify:      make(chan struct{}, 1),
			signing:     oqs.signing,
		}
		oq.statistics.AssignBackoffNotifier(func() {
			oq.handleBackoffNotifier(ctx)
		})
		oqs.queues[destination] = oq
	}
	return oq
}

// clearQueue removes the queue for the provided destination from the
// set of destination queues.
func (oqs *OutgoingQueues) clearQueue(oq *destinationQueue) {
	oqs.queuesMutex.Lock()
	defer oqs.queuesMutex.Unlock()

	delete(oqs.queues, oq.destination)
	destinationQueueTotal.Dec()
}

// SendEvent sends an event to the destinations
func (oqs *OutgoingQueues) SendEvent(
	ctx context.Context,
	ev *types.HeaderedEvent, origin spec.ServerName,
	destinations []spec.ServerName,
) error {
	if oqs.disabled {
		util.Log(ctx).
			WithField("component", "federation_queue").
			Debug("Federation is disabled, not sending event")
		return nil
	}
	if _, ok := oqs.signing[origin]; !ok {
		return fmt.Errorf(
			"sendevent: unexpected server to send as %q",
			origin,
		)
	}

	// Deduplicate destinations and remove the origin from the list of
	// destinations just to be sure.
	destmap := map[spec.ServerName]struct{}{}
	for _, d := range destinations {
		destmap[d] = struct{}{}
	}
	delete(destmap, oqs.origin)
	for local := range oqs.signing {
		delete(destmap, local)
	}

	// If there are no remaining destinations then give up.
	if len(destmap) == 0 {
		return nil
	}

	util.Log(ctx).
		WithField("event_id", ev.EventID()).
		WithField("destinations", len(destmap)).
		WithField("event", ev.EventID()).
		Info("Sending event")

	headeredJSON, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("json.Marshal: %w", err)
	}

	nid, err := oqs.db.StoreJSON(ctx, string(headeredJSON))
	if err != nil {
		return fmt.Errorf("sendevent: oqs.db.StoreJSON: %w", err)
	}

	destQueues := make([]*destinationQueue, 0, len(destmap))
	for destination := range destmap {
		if queue := oqs.getQueue(ctx, destination); queue != nil {
			destQueues = append(destQueues, queue)
		} else {
			delete(destmap, destination)
		}
	}

	// Create a database entry that associates the given PDU NID with
	// this destinations queue. We'll then be able to retrieve the PDU
	// later.
	if err = oqs.db.AssociatePDUWithDestinations(
		ctx,
		destmap,
		nid, // NIDs from federationapi_queue_json table
	); err != nil {
		util.Log(ctx).WithError(err).
			WithField("component", "federation_queue").
			WithField("nid", nid).
			Error("Failed to associate PDUs with destinations")
		return err
	}

	// NOTE : PDUs should be associated with destinations before sending
	// them, otherwise this is technically a race.
	// If the send completes before they are associated then they won't
	// get properly cleaned up in the database.
	for _, queue := range destQueues {
		queue.sendEvent(ctx, ev, nid)
	}

	return nil
}

// SendEDU sends an EDU event to the destinations.
func (oqs *OutgoingQueues) SendEDU(
	ctx context.Context,
	e *gomatrixserverlib.EDU, origin spec.ServerName,
	destinations []spec.ServerName,
) error {
	if oqs.disabled {
		util.Log(ctx).
			WithField("component", "federation_queue").
			Debug("Federation is disabled, not sending EDU")
		return nil
	}
	if _, ok := oqs.signing[origin]; !ok {
		return fmt.Errorf(
			"sendevent: unexpected server to send as %q",
			origin,
		)
	}

	// Deduplicate destinations and remove the origin from the list of
	// destinations just to be sure.
	destmap := map[spec.ServerName]struct{}{}
	for _, d := range destinations {
		destmap[d] = struct{}{}
	}
	delete(destmap, oqs.origin)
	for local := range oqs.signing {
		delete(destmap, local)
	}

	// If there are no remaining destinations then give up.
	if len(destmap) == 0 {
		return nil
	}

	util.Log(ctx).
		WithField("edu_type", e.Type).
		WithField("destinations", len(destmap)).
		WithField("edu_type", e.Type).
		Info("Sending EDU event")

	ephemeralJSON, err := json.Marshal(e)
	if err != nil {

		return fmt.Errorf("json.Marshal: %w", err)
	}

	nid, err := oqs.db.StoreJSON(ctx, string(ephemeralJSON))
	if err != nil {

		return fmt.Errorf("sendevent: oqs.db.StoreJSON: %w", err)
	}

	destQueues := make([]*destinationQueue, 0, len(destmap))
	for destination := range destmap {
		if queue := oqs.getQueue(ctx, destination); queue != nil {
			destQueues = append(destQueues, queue)
		} else {
			delete(destmap, destination)
		}
	}

	// Create a database entry that associates the given PDU NID with
	// these destination queues. We'll then be able to retrieve the PDU
	// later.
	if err = oqs.db.AssociateEDUWithDestinations(
		ctx,
		destmap, // the destination server names
		nid,     // NIDs from federationapi_queue_json table
		e.Type,
		nil, // this will use the default expireEDUTypes map
	); err != nil {
		util.Log(ctx).WithError(err).
			WithField("component", "federation_queue").
			Error("Failed to associate EDU with destinations")
		return err
	}

	// NOTE : EDUs should be associated with destinations before sending
	// them, otherwise this is technically a race.
	// If the send completes before they are associated then they won't
	// get properly cleaned up in the database.
	for _, queue := range destQueues {
		queue.sendEDU(ctx, e, nid)
	}

	return nil
}

// RetryServer attempts to resend events to the given server if we had given up.
func (oqs *OutgoingQueues) RetryServer(ctx context.Context, srv spec.ServerName, wasBlacklisted bool) {
	if oqs.disabled {
		return
	}

	if queue := oqs.getQueue(ctx, srv); queue != nil {
		queue.wakeQueueIfEventsPending(ctx, wasBlacklisted)
	}
}
