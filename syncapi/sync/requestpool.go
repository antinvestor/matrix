// Copyright 2017 Vector Creations Ltd
// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Global.org Foundation C.I.C.
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

package sync

import (
	"context"
	"errors"
	"fmt"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/pitabwire/frame"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	roomserverAPI "github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/syncapi/internal"
	"github.com/antinvestor/matrix/syncapi/notifier"
	"github.com/antinvestor/matrix/syncapi/storage"
	"github.com/antinvestor/matrix/syncapi/streams"
	"github.com/antinvestor/matrix/syncapi/types"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
	"github.com/prometheus/client_golang/prometheus"
)

// RequestPool manages HTTP long-poll connections for /sync
type RequestPool struct {
	db       storage.Database
	wp       queueutil.WorkPookManager
	cfg      *config.SyncAPI
	userAPI  userapi.SyncUserAPI
	rsAPI    roomserverAPI.SyncRoomserverAPI
	lastseen *sync.Map
	presence *sync.Map
	streams  *streams.Streams
	Notifier *notifier.Notifier
	producer PresencePublisher
	consumer PresenceConsumer
}

type PresencePublisher interface {
	SendPresence(ctx context.Context, userID string, presence types.Presence, statusMsg *string) error
}

type PresenceConsumer interface {
	EmitPresence(ctx context.Context, userID string, presence types.Presence, statusMsg *string, ts spec.Timestamp, fromSync bool)
}

// NewRequestPool makes a new RequestPool
func NewRequestPool(
	ctx context.Context,
	db storage.Database,
	wp queueutil.WorkPookManager,
	cfg *config.SyncAPI,
	userAPI userapi.SyncUserAPI,
	rsAPI roomserverAPI.SyncRoomserverAPI,
	streams *streams.Streams, notifier *notifier.Notifier,
	producer PresencePublisher, consumer PresenceConsumer, enableMetrics bool,
) *RequestPool {
	if enableMetrics {
		prometheus.MustRegister(
			activeSyncRequests, waitingSyncRequests,
		)
	}
	rp := &RequestPool{
		db:       db,
		wp:       wp,
		cfg:      cfg,
		userAPI:  userAPI,
		rsAPI:    rsAPI,
		lastseen: &sync.Map{},
		presence: &sync.Map{},
		streams:  streams,
		Notifier: notifier,
		producer: producer,
		consumer: consumer,
	}
	go rp.cleanLastSeen()
	go rp.cleanPresence(ctx, db, time.Minute*5)
	return rp
}

// cleanLastSeen periodically clears the last seen map to prevent unbounded growth.
// This could be improved by only removing entries older than a certain threshold.
func (rp *RequestPool) cleanLastSeen() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Count entries for logging
			count := 0
			rp.lastseen.Range(func(key interface{}, _ interface{}) bool {
				rp.lastseen.Delete(key)
				count++
				return true
			})
		}
	}
}

// cleanPresence periodically removes stale presence information for users
// that haven't been active for longer than cleanupTime.
func (rp *RequestPool) cleanPresence(ctx context.Context, db storage.Presence, cleanupTime time.Duration) {
	if !rp.cfg.Global.Presence.EnableOutbound {
		return
	}

	ticker := time.NewTicker(cleanupTime)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleaned := 0
			rp.presence.Range(func(key interface{}, v interface{}) bool {
				p, ok := v.(types.PresenceInternal)
				if !ok {
					// Invalid type in map, just delete it
					rp.presence.Delete(key)
					cleaned++
					return true
				}

				if time.Since(p.LastActiveTS.Time()) > cleanupTime {
					// Use a new context for each update to avoid issues if the main context is cancelled
					updateCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					_ = rp.updatePresence(updateCtx, db, types.PresenceUnavailable.String(), p.UserID)
					cancel()

					rp.presence.Delete(key)
					cleaned++
				}
				return true
			})
		}
	}
}

// updatePresence sends presence updates to the SyncAPI and FederationAPI
func (rp *RequestPool) updatePresence(ctx context.Context, db storage.Presence, presence string, userID string) error {
	// allow checking back on presence to set offline if needed
	return rp.updatePresenceInternal(ctx, db, presence, userID, true)
}

type PresenceMap struct {
	mu   sync.Mutex
	seen map[string]map[types.Presence]time.Time
}

var lastPresence PresenceMap

// how long before the online status expires
// should be long enough that any client will have another sync before expiring
const presenceTimeout = time.Second * 10

// updatePresenceInternal sends presence updates to the SyncAPI and FederationAPI
func (rp *RequestPool) updatePresenceInternal(ctx context.Context, db storage.Presence, presence string, userID string, checkAgain bool) error {
	if !rp.cfg.Global.Presence.EnableOutbound {
		return nil
	}

	// lock the map to this thread
	lastPresence.mu.Lock()
	defer lastPresence.mu.Unlock()

	if presence == "" {
		presence = types.PresenceOnline.String()
	}

	presenceID, ok := types.PresenceFromString(presence)
	if !ok { // this should almost never happen
		return errors.New("invalid presence value")
	}

	newPresence := types.PresenceInternal{
		Presence:     presenceID,
		UserID:       userID,
		LastActiveTS: spec.AsTimestamp(time.Now()),
	}

	// make sure that the map is defined correctly as needed
	if lastPresence.seen == nil {
		lastPresence.seen = make(map[string]map[types.Presence]time.Time)
	}
	if lastPresence.seen[userID] == nil {
		lastPresence.seen[userID] = make(map[types.Presence]time.Time)
	}

	now := time.Now()
	// update time for each presence
	lastPresence.seen[userID][presenceID] = now

	// Default to unknown presence
	presenceToSet := types.PresenceUnknown
	switch {
	case now.Sub(lastPresence.seen[userID][types.PresenceOnline]) < presenceTimeout:
		// online will always get priority
		presenceToSet = types.PresenceOnline
	case now.Sub(lastPresence.seen[userID][types.PresenceUnavailable]) < presenceTimeout:
		// idle gets secondary priority because your presence shouldnt be idle if you are on a different device
		// kinda copying discord presence
		presenceToSet = types.PresenceUnavailable
	case now.Sub(lastPresence.seen[userID][types.PresenceOffline]) < presenceTimeout:
		// only set offline status if there is no known online devices
		// clients may set offline to attempt to not alter the online status of the user
		presenceToSet = types.PresenceOffline

		if checkAgain {
			// after a timeout, check presence again to make sure it gets set as offline sooner or later
			time.AfterFunc(presenceTimeout, func() {
				// Create a new context for the deferred check
				checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				if err := rp.updatePresenceInternal(checkCtx, db, types.PresenceOffline.String(), userID, false); err != nil {
					util.Log(checkCtx).WithError(err).WithField("user_id", userID).
						Warn("Failed to update presence in deferred check")
				}
			})
		}
	}

	// ensure we also send the current status_msg to federated servers and not nil
	dbPresence, err := db.GetPresences(ctx, []string{userID})
	if err != nil && !sqlutil.ErrorIsNoRows(err) {
		return fmt.Errorf("failed to get presence from database: %w", err)
	}
	if len(dbPresence) > 0 && dbPresence[0] != nil {
		newPresence.ClientFields = dbPresence[0].ClientFields
	}
	newPresence.ClientFields.Presence = presenceToSet.String()

	defer rp.presence.Store(userID, newPresence)
	// avoid spamming presence updates when syncing
	existingPresence, ok := rp.presence.LoadOrStore(userID, newPresence)
	if ok {
		p, ok := existingPresence.(types.PresenceInternal)
		if !ok {
			util.Log(ctx).WithField("user_id", userID).
				Warn("Invalid presence data in presence map")
		} else if p.ClientFields.Presence == newPresence.ClientFields.Presence {
			return nil
		}
	}

	err = rp.producer.SendPresence(ctx, userID, presenceToSet, newPresence.ClientFields.StatusMsg)
	if err != nil {
		util.Log(ctx).WithError(err).WithField("user_id", userID).
			Error("Unable to publish presence message from sync")
		return fmt.Errorf("failed to send presence update: %w", err)
	}

	// now synchronously update our view of the world. It's critical we do this before calculating
	// the /sync response else we may not return presence: online immediately.
	rp.consumer.EmitPresence(
		ctx, userID, presenceToSet, newPresence.ClientFields.StatusMsg,
		spec.AsTimestamp(time.Now()), true,
	)

	return nil
}

func (rp *RequestPool) updateLastSeen(req *http.Request, device *userapi.Device) {

	if _, ok := rp.lastseen.LoadOrStore(device.UserID+device.ID, struct{}{}); ok {
		return
	}

	remoteAddr := req.RemoteAddr
	if rp.cfg.RealIPHeader != "" {
		if header := req.Header.Get(rp.cfg.RealIPHeader); header != "" {
			// TODO: Maybe this isn't great but it will satisfy both X-Real-IP
			// and X-Forwarded-For (which can be a list where the real client
			// address is the first listed address). Make more intelligent?
			addresses := strings.Split(header, ",")
			if ip := net.ParseIP(addresses[0]); ip != nil {
				remoteAddr = addresses[0]
			}
		}
	}

	lsreq := &userapi.PerformLastSeenUpdateRequest{
		UserID:     device.UserID,
		DeviceID:   device.ID,
		RemoteAddr: remoteAddr,
		UserAgent:  req.UserAgent(),
	}
	lsres := &userapi.PerformLastSeenUpdateResponse{}
	go func(ctx context.Context) {
		err := rp.userAPI.PerformLastSeenUpdate(req.Context(), lsreq, lsres)
		if err != nil {
			util.Log(req.Context()).WithError(err).Error("PerformLastSeenUpdate failed")
		}
	}(req.Context())

	rp.lastseen.Store(device.UserID+device.ID, time.Now())
}

var activeSyncRequests = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "matrix",
		Subsystem: "syncapi",
		Name:      "active_sync_requests",
		Help:      "The number of sync requests that are active right now",
	},
)

var waitingSyncRequests = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "matrix",
		Subsystem: "syncapi",
		Name:      "waiting_sync_requests",
		Help:      "The number of sync requests that are waiting to be woken by a notifier",
	},
)

// OnIncomingSyncRequest is called when a client makes a /sync request. This function MUST be
// called in a dedicated goroutine for this request. This function will block the goroutine
// until a response is ready, or it times out.
func (rp *RequestPool) OnIncomingSyncRequest(req *http.Request, device *userapi.Device) util.JSONResponse {

	ctx := req.Context()
	log := util.Log(ctx)

	// Extract values from request
	syncReq, err := newSyncRequest(req, *device, rp.db)
	if err != nil {
		if errors.Is(err, types.ErrMalformedSyncToken) {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam(err.Error()),
			}
		}
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown(err.Error()),
		}
	}

	activeSyncRequests.Inc()
	defer activeSyncRequests.Dec()

	rp.updateLastSeen(req, device)
	_ = rp.updatePresence(ctx, rp.db, req.FormValue("set_presence"), device.UserID)

	waitingSyncRequests.Inc()
	defer waitingSyncRequests.Dec()

	// Clean up old send-to-device messages from before this stream position.
	// This is needed to avoid sending the same message multiple times
	if err = rp.db.CleanSendToDeviceUpdates(ctx, syncReq.Device.UserID, syncReq.Device.ID, syncReq.Since.SendToDevicePosition); err != nil {
		log.WithError(err).Error("p.Cm.CleanSendToDeviceUpdates failed")
	}

	// loop until we get some data
	for {
		startTime := time.Now()
		currentPos := rp.Notifier.CurrentPosition()

		// if the since token matches the current positions, wait via the notifier
		if !rp.shouldReturnImmediately(syncReq, currentPos) {

			timer := time.NewTimer(syncReq.Timeout) // case of timeout=0 is handled above

			userStreamListener := rp.Notifier.GetListener(*syncReq)
			defer userStreamListener.Close()

			giveup := func(ctx context.Context) util.JSONResponse {
				ilog := util.Log(ctx)
				ilog.Debug("Responding to sync since client gave up or timeout was reached")
				syncReq.Response.NextBatch = syncReq.Since
				// We should always try to include OTKs in sync responses, otherwise clients might upload keys
				// even if that's not required. See also:
				// https://github.com/matrix-org/synapse/blob/29f06704b8871a44926f7c99e73cf4a978fb8e81/synapse/rest/client/sync.py#L276-L281
				// Only try to get OTKs if the context isn't already done.
				if ctx.Err() == nil {
					err = internal.DeviceOTKCounts(ctx, rp.userAPI, syncReq.Device.UserID, syncReq.Device.ID, syncReq.Response)
					if err != nil && !errors.Is(context.Canceled, err) {
						ilog.WithError(err).Warn("failed to get OTK counts")
					}
				}
				return util.JSONResponse{
					Code: http.StatusOK,
					JSON: syncReq.Response,
				}
			}

			select {
			case <-ctx.Done(): // Caller gave up

				timer.Stop()
				userStreamListener.Close()
				return giveup(ctx)

			case <-timer.C: // Timeout reached
				timer.Stop()
				userStreamListener.Close()
				return giveup(ctx)

			case <-userStreamListener.GetNotifyChannel(syncReq.Since):
				timer.Stop()
				currentPos.ApplyUpdates(userStreamListener.GetSyncPosition())
				log.WithField("currentPos", currentPos).Debug("Responding to sync after wake-up")
			}
		}

		// Run a function on a worker pool and return the result through a channel
		withWorkerPoolExecution := func(from types.StreamPosition, f func(ctx context.Context) types.StreamPosition) <-chan frame.JobResult {

			poolRequestJob := frame.NewJob(func(ctx context.Context, result frame.JobResultPipe) error {

				// Execute the function with the transaction
				streamPos := f(ctx)
				return result.WriteResult(ctx, streamPos)
			})

			rp.wp.Submit(ctx, poolRequestJob)

			return poolRequestJob.ResultChan()
		}

		if syncReq.Since.IsEmpty() {
			// Complete sync - execute all stream operations concurrently
			token := types.StreamingToken{}

			// Launch each stream operation concurrently
			deviceListCh := withWorkerPoolExecution(
				syncReq.Since.DeviceListPosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.DeviceListStreamProvider.CompleteSync(
						ctx, syncReq,
					)
				},
			)

			pduCh := withWorkerPoolExecution(
				syncReq.Since.PDUPosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.PDUStreamProvider.CompleteSync(
						ctx, syncReq,
					)
				},
			)

			typingCh := withWorkerPoolExecution(
				syncReq.Since.TypingPosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.TypingStreamProvider.CompleteSync(
						ctx, syncReq,
					)
				},
			)

			receiptCh := withWorkerPoolExecution(
				syncReq.Since.ReceiptPosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.ReceiptStreamProvider.CompleteSync(
						ctx, syncReq,
					)
				},
			)

			inviteCh := withWorkerPoolExecution(
				syncReq.Since.InvitePosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.InviteStreamProvider.CompleteSync(
						ctx, syncReq,
					)
				},
			)

			sendToDeviceCh := withWorkerPoolExecution(
				syncReq.Since.SendToDevicePosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.SendToDeviceStreamProvider.CompleteSync(
						ctx, syncReq,
					)
				},
			)

			accountDataCh := withWorkerPoolExecution(
				syncReq.Since.AccountDataPosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.AccountDataStreamProvider.CompleteSync(
						ctx, syncReq,
					)
				},
			)

			notificationDataCh := withWorkerPoolExecution(
				syncReq.Since.NotificationDataPosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.NotificationDataStreamProvider.CompleteSync(
						ctx, syncReq,
					)
				},
			)

			presenceCh := withWorkerPoolExecution(
				syncReq.Since.PresencePosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.PresenceStreamProvider.CompleteSync(
						ctx, syncReq,
					)
				},
			)

			// Collect results from all channels
			deviceListResult := <-deviceListCh
			if deviceListResult.IsError() {
				log.WithError(deviceListResult.Error()).Error("Failed to get DeviceListPosition")
				token.DeviceListPosition = syncReq.Since.DeviceListPosition
			} else {
				token.DeviceListPosition = deviceListResult.Item().(types.StreamPosition)
			}

			pduResult := <-pduCh
			if pduResult.IsError() {
				log.WithError(pduResult.Error()).Error("Failed to get PDUPosition")
				token.PDUPosition = syncReq.Since.PDUPosition
			} else {
				token.PDUPosition = pduResult.Item().(types.StreamPosition)
			}

			typingResult := <-typingCh
			if typingResult.IsError() {
				log.WithError(typingResult.Error()).Error("Failed to get TypingPosition")
				token.TypingPosition = syncReq.Since.TypingPosition
			} else {
				token.TypingPosition = typingResult.Item().(types.StreamPosition)
			}

			receiptResult := <-receiptCh
			if receiptResult.IsError() {
				log.WithError(receiptResult.Error()).Error("Failed to get ReceiptPosition")
				token.ReceiptPosition = syncReq.Since.ReceiptPosition
			} else {
				token.ReceiptPosition = receiptResult.Item().(types.StreamPosition)
			}

			inviteResult := <-inviteCh
			if inviteResult.IsError() {
				log.WithError(inviteResult.Error()).Error("Failed to get InvitePosition")
				token.InvitePosition = syncReq.Since.InvitePosition
			} else {
				token.InvitePosition = inviteResult.Item().(types.StreamPosition)
			}

			sendToDeviceResult := <-sendToDeviceCh
			if sendToDeviceResult.IsError() {
				log.WithError(sendToDeviceResult.Error()).Error("Failed to get SendToDevicePosition")
				token.SendToDevicePosition = syncReq.Since.SendToDevicePosition
			} else {
				token.SendToDevicePosition = sendToDeviceResult.Item().(types.StreamPosition)
			}

			accountDataResult := <-accountDataCh
			if accountDataResult.IsError() {
				log.WithError(accountDataResult.Error()).Error("Failed to get AccountDataPosition")
				token.AccountDataPosition = syncReq.Since.AccountDataPosition
			} else {
				token.AccountDataPosition = accountDataResult.Item().(types.StreamPosition)
			}

			notificationDataResult := <-notificationDataCh
			if notificationDataResult.IsError() {
				log.WithError(notificationDataResult.Error()).Error("Failed to get NotificationDataPosition")
				token.NotificationDataPosition = syncReq.Since.NotificationDataPosition
			} else {
				token.NotificationDataPosition = notificationDataResult.Item().(types.StreamPosition)
			}

			presenceResult := <-presenceCh
			if presenceResult.IsError() {
				log.WithError(presenceResult.Error()).Error("Failed to get PresencePosition")
				token.PresencePosition = syncReq.Since.PresencePosition
			} else {
				token.PresencePosition = presenceResult.Item().(types.StreamPosition)
			}

			syncReq.Response.NextBatch = token
		} else {
			// Incremental sync - also execute all stream operations concurrently
			token := syncReq.Since
			currentPos = rp.Notifier.CurrentPosition()

			// Launch each stream operation concurrently
			deviceListCh := withWorkerPoolExecution(
				syncReq.Since.DeviceListPosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.DeviceListStreamProvider.IncrementalSync(
						ctx, syncReq,
						syncReq.Since.DeviceListPosition, currentPos.DeviceListPosition,
					)
				},
			)

			pduCh := withWorkerPoolExecution(
				syncReq.Since.PDUPosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.PDUStreamProvider.IncrementalSync(
						ctx, syncReq,
						syncReq.Since.PDUPosition, currentPos.PDUPosition,
					)
				},
			)

			typingCh := withWorkerPoolExecution(
				syncReq.Since.TypingPosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.TypingStreamProvider.IncrementalSync(
						ctx, syncReq,
						syncReq.Since.TypingPosition, currentPos.TypingPosition,
					)
				},
			)

			receiptCh := withWorkerPoolExecution(
				syncReq.Since.ReceiptPosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.ReceiptStreamProvider.IncrementalSync(
						ctx, syncReq,
						syncReq.Since.ReceiptPosition, currentPos.ReceiptPosition,
					)
				},
			)

			inviteCh := withWorkerPoolExecution(
				syncReq.Since.InvitePosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.InviteStreamProvider.IncrementalSync(
						ctx, syncReq,
						syncReq.Since.InvitePosition, currentPos.InvitePosition,
					)
				},
			)

			sendToDeviceCh := withWorkerPoolExecution(
				syncReq.Since.SendToDevicePosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.SendToDeviceStreamProvider.IncrementalSync(
						ctx, syncReq,
						syncReq.Since.SendToDevicePosition, currentPos.SendToDevicePosition,
					)
				},
			)

			accountDataCh := withWorkerPoolExecution(
				syncReq.Since.AccountDataPosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.AccountDataStreamProvider.IncrementalSync(
						ctx, syncReq,
						syncReq.Since.AccountDataPosition, currentPos.AccountDataPosition,
					)
				},
			)

			notificationDataCh := withWorkerPoolExecution(
				syncReq.Since.NotificationDataPosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.NotificationDataStreamProvider.IncrementalSync(
						ctx, syncReq,
						syncReq.Since.NotificationDataPosition, currentPos.NotificationDataPosition,
					)
				},
			)

			presenceCh := withWorkerPoolExecution(
				syncReq.Since.PresencePosition,
				func(ctx context.Context) types.StreamPosition {
					return rp.streams.PresenceStreamProvider.IncrementalSync(
						ctx, syncReq,
						syncReq.Since.PresencePosition, currentPos.PresencePosition,
					)
				},
			)

			// Collect results from all channels
			deviceListResult := <-deviceListCh
			if deviceListResult.IsError() {
				log.WithError(deviceListResult.Error()).Error("Failed to get DeviceListPosition")
			} else {
				token.DeviceListPosition = deviceListResult.Item().(types.StreamPosition)
			}

			pduResult := <-pduCh
			if pduResult.IsError() {
				log.WithError(pduResult.Error()).Error("Failed to get PDUPosition")
			} else {
				token.PDUPosition = pduResult.Item().(types.StreamPosition)
			}

			typingResult := <-typingCh
			if typingResult.IsError() {
				log.WithError(typingResult.Error()).Error("Failed to get TypingPosition")
			} else {
				token.TypingPosition = typingResult.Item().(types.StreamPosition)
			}

			receiptResult := <-receiptCh
			if receiptResult.IsError() {
				log.WithError(receiptResult.Error()).Error("Failed to get ReceiptPosition")
			} else {
				token.ReceiptPosition = receiptResult.Item().(types.StreamPosition)
			}

			inviteResult := <-inviteCh
			if inviteResult.IsError() {
				log.WithError(inviteResult.Error()).Error("Failed to get InvitePosition")
			} else {
				token.InvitePosition = inviteResult.Item().(types.StreamPosition)
			}

			sendToDeviceResult := <-sendToDeviceCh
			if sendToDeviceResult.IsError() {
				log.WithError(sendToDeviceResult.Error()).Error("Failed to get SendToDevicePosition")
			} else {
				token.SendToDevicePosition = sendToDeviceResult.Item().(types.StreamPosition)
			}

			accountDataResult := <-accountDataCh
			if accountDataResult.IsError() {
				log.WithError(accountDataResult.Error()).Error("Failed to get AccountDataPosition")
			} else {
				token.AccountDataPosition = accountDataResult.Item().(types.StreamPosition)
			}

			notificationDataResult := <-notificationDataCh
			if notificationDataResult.IsError() {
				log.WithError(notificationDataResult.Error()).Error("Failed to get NotificationDataPosition")
			} else {
				token.NotificationDataPosition = notificationDataResult.Item().(types.StreamPosition)
			}

			presenceResult := <-presenceCh
			if presenceResult.IsError() {
				log.WithError(presenceResult.Error()).Error("Failed to get PresencePosition")
			} else {
				token.PresencePosition = presenceResult.Item().(types.StreamPosition)
			}

			syncReq.Response.NextBatch = token

			// it's possible for there to be no updates for this user even though since < current pos,
			// e.g busy servers with a quiet user. In this scenario, we don't want to return a no-op
			// response immediately, so let's try this again but pretend they bumped their since token.
			// If the incremental sync was processed very quickly then we expect the next loop to block
			// with a notifier, but if things are slow it's entirely possible that currentPos is no
			// longer the current position so we will hit this code path again. We need to do this and
			// not return a no-op response because:
			// - It's an inefficient use of bandwidth.
			// - Some sytests which test 'waking up' sync rely on some sync requests to block, which
			//   they weren't always doing, resulting in flakey tests.
			if !syncReq.Response.HasUpdates() {
				syncReq.Since = currentPos
				// do not loop again if the ?timeout= is 0 as that means "return immediately"
				if syncReq.Timeout > 0 {
					syncReq.Timeout = syncReq.Timeout - time.Since(startTime)
					if syncReq.Timeout < 0 {
						syncReq.Timeout = 0
					}
					continue
				}
			}
		}

		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: syncReq.Response,
		}
	}
}

func (rp *RequestPool) OnIncomingKeyChangeRequest(req *http.Request, device *userapi.Device) util.JSONResponse {
	from := req.URL.Query().Get("from")
	to := req.URL.Query().Get("to")
	if from == "" || to == "" {
		return util.JSONResponse{
			Code: 400,
			JSON: spec.InvalidParam("missing ?from= or ?to="),
		}
	}
	fromToken, err := types.NewStreamTokenFromString(from)
	if err != nil {
		return util.JSONResponse{
			Code: 400,
			JSON: spec.InvalidParam("bad 'from' value"),
		}
	}
	toToken, err := types.NewStreamTokenFromString(to)
	if err != nil {
		return util.JSONResponse{
			Code: 400,
			JSON: spec.InvalidParam("bad 'to' value"),
		}
	}
	syncReq, err := newSyncRequest(req, *device, rp.db)
	if err != nil {
		util.Log(req.Context()).WithError(err).Error("newSyncRequest failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	snapshot, err := rp.db.NewDatabaseSnapshot(req.Context())
	if err != nil {
		util.Log(req.Context()).WithError(err).Error("Failed to acquire database snapshot for key change")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	rp.streams.PDUStreamProvider.IncrementalSync(req.Context(), syncReq, fromToken.PDUPosition, toToken.PDUPosition)
	_, _, err = internal.DeviceListCatchup(
		req.Context(), snapshot, rp.userAPI, rp.rsAPI, syncReq.Device.UserID,
		syncReq.Response, fromToken.DeviceListPosition, toToken.DeviceListPosition,
	)
	if err != nil {
		util.Log(req.Context()).WithError(err).Error("Failed to DeviceListCatchup info")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: 200,
		JSON: struct {
			Changed []string `json:"changed"`
			Left    []string `json:"left"`
		}{
			Changed: syncReq.Response.DeviceLists.Changed,
			Left:    syncReq.Response.DeviceLists.Left,
		},
	}
}

// shouldReturnImmediately returns whether the /sync request is an initial sync,
// or timeout=0, or full_state=true, in any of the cases the request should
// return immediately.
func (rp *RequestPool) shouldReturnImmediately(syncReq *types.SyncRequest, currentPos types.StreamingToken) bool {
	if currentPos.IsAfter(syncReq.Since) || syncReq.Timeout == 0 || syncReq.WantFullState {
		return true
	}
	return false
}
