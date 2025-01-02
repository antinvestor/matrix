// Copyright 2022 The Matrix.org Foundation C.I.C.
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/test/testrig"
	"gotest.tools/v3/poll"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/stretchr/testify/assert"

	"github.com/antinvestor/matrix/federationapi/statistics"
	"github.com/antinvestor/matrix/federationapi/storage"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/setup/process"
	"github.com/antinvestor/matrix/test"
)

func mustCreateFederationDatabase(t *testing.T, realDatabase bool) (storage.Database, *process.ProcessContext, func()) {
	if realDatabase {
		// Real Database/s
		cfg, processCtx, closeRig := testrig.CreateConfig(t, test.DependancyOption{})

		dbOptions := cfg.Global.DatabaseOptions

		cm := sqlutil.NewConnectionManager(processCtx, dbOptions)
		caches, err := caching.NewCache(&cfg.Global.Cache)
		if err != nil {
			t.Fatalf("failed to create a cache: %v", err)
		}

		db, err := storage.NewDatabase(processCtx.Context(), cm, &dbOptions, caches, cfg.Global.IsLocalServerName)
		if err != nil {
			t.Fatalf("NewDatabase failed with : %s", err)
		}
		return db, processCtx, closeRig
	} else {
		// Fake Database
		db := test.NewInMemoryFederationDatabase()
		return db, process.NewProcessContext(), func() {}
	}
}

type stubFederationClient struct {
	fclient.FederationClient
	shouldTxSucceed      bool
	shouldTxRelaySucceed bool
	txCount              atomic.Uint32
	txRelayCount         atomic.Uint32
}

func (f *stubFederationClient) SendTransaction(_ context.Context, _ gomatrixserverlib.Transaction) (res fclient.RespSend, err error) {
	var result error
	if !f.shouldTxSucceed {
		result = fmt.Errorf("transaction failed")
	}

	f.txCount.Add(1)
	return fclient.RespSend{}, result
}

func (f *stubFederationClient) P2PSendTransactionToRelay(_ context.Context, _ spec.UserID, _ gomatrixserverlib.Transaction, _ spec.ServerName) (res fclient.EmptyResp, err error) {
	var result error
	if !f.shouldTxRelaySucceed {
		result = fmt.Errorf("relay transaction failed")
	}

	f.txRelayCount.Add(1)
	return fclient.EmptyResp{}, result
}

func mustCreatePDU(t *testing.T) *types.HeaderedEvent {
	t.Helper()
	content := `{"type":"m.room.message", "room_id":"!room:a"}`
	ev, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10).NewEventFromTrustedJSON([]byte(content), false)
	if err != nil {
		t.Fatalf("failed to create event: %v", err)
	}
	return &types.HeaderedEvent{PDU: ev}
}

func mustCreateEDU(t *testing.T) *gomatrixserverlib.EDU {
	t.Helper()
	return &gomatrixserverlib.EDU{Type: spec.MTyping}
}

func testSetup(failuresUntilBlacklist uint32, failuresUntilAssumedOffline uint32, shouldTxSucceed bool, shouldTxRelaySucceed bool, t *testing.T, realDatabase bool) (storage.Database, *stubFederationClient, *OutgoingQueues, *process.ProcessContext, func()) {
	db, processContext, closeFn := mustCreateFederationDatabase(t, realDatabase)

	fc := &stubFederationClient{
		shouldTxSucceed:      shouldTxSucceed,
		shouldTxRelaySucceed: shouldTxRelaySucceed,
		txCount:              atomic.Uint32{},
		txRelayCount:         atomic.Uint32{},
	}

	stats := statistics.NewStatistics(db, failuresUntilBlacklist, failuresUntilAssumedOffline, false)
	signingInfo := []*fclient.SigningIdentity{
		{
			KeyID:      "ed21019:auto",
			PrivateKey: test.PrivateKeyA,
			ServerName: "localhost",
		},
	}
	queues := NewOutgoingQueues(db, processContext, false, "localhost", fc, &stats, signingInfo)

	return db, fc, queues, processContext, closeFn
}

func TestSendPDUOnSuccessRemovedFromDB(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(16)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeSetup := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, true, false, t, false)
	defer closeSetup()
	defer func() {
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	ev := mustCreatePDU(t)
	err := queues.SendEvent(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() == 1 {
			data, dbErr := db.GetPendingPDUs(pc.Context(), destination, 100)
			assert.NoError(t, dbErr)
			if len(data) == 0 {
				return poll.Success()
			}
			return poll.Continue("waiting for event to be removed from database. Currently present PDU: %d", len(data))
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestSendEDUOnSuccessRemovedFromDB(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(16)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeSetup := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, true, false, t, false)
	defer closeSetup()
	defer func() {
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	ev := mustCreateEDU(t)
	err := queues.SendEDU(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() == 1 {
			data, dbErr := db.GetPendingEDUs(pc.Context(), destination, 100)
			assert.NoError(t, dbErr)
			if len(data) == 0 {
				return poll.Success()
			}
			return poll.Continue("waiting for event to be removed from database. Currently present EDU: %d", len(data))
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestSendPDUOnFailStoredInDB(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(16)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeSetup := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, false, false, t, false)
	defer closeSetup()
	defer func() {
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	ev := mustCreatePDU(t)
	err := queues.SendEvent(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		// Wait for 2 backoff attempts to ensure there was adequate time to attempt sending
		if fc.txCount.Load() >= 2 {
			data, dbErr := db.GetPendingPDUs(pc.Context(), destination, 100)
			assert.NoError(t, dbErr)
			if len(data) == 1 {
				return poll.Success()
			}
			return poll.Continue("waiting for event to be added to database. Currently present PDU: %d", len(data))
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestSendEDUOnFailStoredInDB(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(16)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, false, false, t, false)
	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	ev := mustCreateEDU(t)
	err := queues.SendEDU(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		// Wait for 2 backoff attempts to ensure there was adequate time to attempt sending
		if fc.txCount.Load() >= 2 {
			data, dbErr := db.GetPendingEDUs(pc.Context(), destination, 100)
			assert.NoError(t, dbErr)
			if len(data) == 1 {
				return poll.Success()
			}
			return poll.Continue("waiting for event to be added to database. Currently present EDU: %d", len(data))
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestSendPDUAgainDoesntInterruptBackoff(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(16)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, false, false, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	ev := mustCreatePDU(t)
	err := queues.SendEvent(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		// Wait for 2 backoff attempts to ensure there was adequate time to attempt sending
		if fc.txCount.Load() >= 2 {
			data, dbErr := db.GetPendingPDUs(pc.Context(), destination, 100)
			assert.NoError(t, dbErr)
			if len(data) == 1 {
				return poll.Success()
			}
			return poll.Continue("waiting for event to be added to database. Currently present PDU: %d", len(data))
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))

	fc.shouldTxSucceed = true
	ev = mustCreatePDU(t)
	err = queues.SendEvent(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	pollEnd := time.Now().Add(1 * time.Second)
	immediateCheck := func(log poll.LogT) poll.Result {
		data, dbErr := db.GetPendingPDUs(pc.Context(), destination, 100)
		assert.NoError(t, dbErr)
		if len(data) == 0 {
			return poll.Error(fmt.Errorf("The backoff was interrupted early"))
		}
		if time.Now().After(pollEnd) {
			// Allow more than enough time for the backoff to be interrupted before
			// reporting that it wasn't.
			return poll.Success()
		}
		return poll.Continue("waiting for events to be removed from database. Currently present PDU: %d", len(data))
	}
	poll.WaitOn(t, immediateCheck, poll.WithTimeout(2*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestSendEDUAgainDoesntInterruptBackoff(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(16)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, false, false, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	ev := mustCreateEDU(t)
	err := queues.SendEDU(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		// Wait for 2 backoff attempts to ensure there was adequate time to attempt sending
		if fc.txCount.Load() >= 2 {
			data, dbErr := db.GetPendingEDUs(pc.Context(), destination, 100)
			assert.NoError(t, dbErr)
			if len(data) == 1 {
				return poll.Success()
			}
			return poll.Continue("waiting for event to be added to database. Currently present EDU: %d", len(data))
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))

	fc.shouldTxSucceed = true
	ev = mustCreateEDU(t)
	err = queues.SendEDU(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	pollEnd := time.Now().Add(1 * time.Second)
	immediateCheck := func(log poll.LogT) poll.Result {
		data, dbErr := db.GetPendingEDUs(pc.Context(), destination, 100)
		assert.NoError(t, dbErr)
		if len(data) == 0 {
			return poll.Error(fmt.Errorf("The backoff was interrupted early"))
		}
		if time.Now().After(pollEnd) {
			// Allow more than enough time for the backoff to be interrupted before
			// reporting that it wasn't.
			return poll.Success()
		}
		return poll.Continue("waiting for events to be removed from database. Currently present EDU: %d", len(data))
	}
	poll.WaitOn(t, immediateCheck, poll.WithTimeout(2*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestSendPDUMultipleFailuresBlacklisted(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(2)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, false, false, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	ev := mustCreatePDU(t)
	err := queues.SendEvent(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() == failuresUntilBlacklist {
			data, dbErr := db.GetPendingPDUs(pc.Context(), destination, 100)
			assert.NoError(t, dbErr)
			if len(data) == 1 {
				if val, _ := db.IsServerBlacklisted(destination); val {
					return poll.Success()
				}
				return poll.Continue("waiting for server to be blacklisted")
			}
			return poll.Continue("waiting for event to be added to database. Currently present PDU: %d", len(data))
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestSendEDUMultipleFailuresBlacklisted(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(2)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, false, false, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	ev := mustCreateEDU(t)
	err := queues.SendEDU(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() == failuresUntilBlacklist {
			data, dbErr := db.GetPendingEDUs(pc.Context(), destination, 100)
			assert.NoError(t, dbErr)
			if len(data) == 1 {
				if val, _ := db.IsServerBlacklisted(destination); val {
					return poll.Success()
				}
				return poll.Continue("waiting for server to be blacklisted")
			}
			return poll.Continue("waiting for event to be added to database. Currently present EDU: %d", len(data))
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestSendPDUBlacklistedWithPriorExternalFailure(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(2)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, false, false, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	queues.statistics.ForServer(destination).Failure()

	ev := mustCreatePDU(t)
	err := queues.SendEvent(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() == failuresUntilBlacklist {
			data, dbErr := db.GetPendingPDUs(pc.Context(), destination, 100)
			assert.NoError(t, dbErr)
			if len(data) == 1 {
				if val, _ := db.IsServerBlacklisted(destination); val {
					return poll.Success()
				}
				return poll.Continue("waiting for server to be blacklisted")
			}
			return poll.Continue("waiting for event to be added to database. Currently present PDU: %d", len(data))
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestSendEDUBlacklistedWithPriorExternalFailure(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(2)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, false, false, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	queues.statistics.ForServer(destination).Failure()

	ev := mustCreateEDU(t)
	err := queues.SendEDU(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() == failuresUntilBlacklist {
			data, dbErr := db.GetPendingEDUs(pc.Context(), destination, 100)
			assert.NoError(t, dbErr)
			if len(data) == 1 {
				if val, _ := db.IsServerBlacklisted(destination); val {
					return poll.Success()
				}
				return poll.Continue("waiting for server to be blacklisted")
			}
			return poll.Continue("waiting for event to be added to database. Currently present EDU: %d", len(data))
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestRetryServerSendsPDUSuccessfully(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(1)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, false, false, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	// NOTE : getQueue before sending event to ensure we grab the same queue reference
	// before it is blacklisted and deleted.
	dest := queues.getQueue(destination)
	ev := mustCreatePDU(t)
	err := queues.SendEvent(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	checkBlacklisted := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() == failuresUntilBlacklist {
			data, dbErr := db.GetPendingPDUs(pc.Context(), destination, 100)
			assert.NoError(t, dbErr)
			if len(data) == 1 {
				if val, _ := db.IsServerBlacklisted(destination); val {
					if !dest.running.Load() {
						return poll.Success()
					}
					return poll.Continue("waiting for queue to stop completely")
				}
				return poll.Continue("waiting for server to be blacklisted")
			}
			return poll.Continue("waiting for event to be added to database. Currently present PDU: %d", len(data))
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, checkBlacklisted, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))

	fc.shouldTxSucceed = true
	wasBlacklisted := dest.statistics.MarkServerAlive()
	queues.RetryServer(destination, wasBlacklisted)
	checkRetry := func(log poll.LogT) poll.Result {
		data, dbErr := db.GetPendingPDUs(pc.Context(), destination, 100)
		assert.NoError(t, dbErr)
		if len(data) == 0 {
			return poll.Success()
		}
		return poll.Continue("waiting for event to be removed from database. Currently present PDU: %d", len(data))
	}
	poll.WaitOn(t, checkRetry, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestRetryServerSendsEDUSuccessfully(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(1)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, false, false, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	// NOTE : getQueue before sending event to ensure we grab the same queue reference
	// before it is blacklisted and deleted.
	dest := queues.getQueue(destination)
	ev := mustCreateEDU(t)
	err := queues.SendEDU(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	checkBlacklisted := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() == failuresUntilBlacklist {
			data, dbErr := db.GetPendingEDUs(pc.Context(), destination, 100)
			assert.NoError(t, dbErr)
			if len(data) == 1 {
				if val, _ := db.IsServerBlacklisted(destination); val {
					if !dest.running.Load() {
						return poll.Success()
					}
					return poll.Continue("waiting for queue to stop completely")
				}
				return poll.Continue("waiting for server to be blacklisted")
			}
			return poll.Continue("waiting for event to be added to database. Currently present EDU: %d", len(data))
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, checkBlacklisted, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))

	fc.shouldTxSucceed = true
	wasBlacklisted := dest.statistics.MarkServerAlive()
	queues.RetryServer(destination, wasBlacklisted)
	checkRetry := func(log poll.LogT) poll.Result {
		data, dbErr := db.GetPendingEDUs(pc.Context(), destination, 100)
		assert.NoError(t, dbErr)
		if len(data) == 0 {
			return poll.Success()
		}
		return poll.Continue("waiting for event to be removed from database. Currently present EDU: %d", len(data))
	}
	poll.WaitOn(t, checkRetry, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestSendPDUBatches(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(16)
	destination := spec.ServerName("remotehost")

	// test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
	// db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, true, t, testOpts, true)
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, true, false, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	destinations := map[spec.ServerName]struct{}{destination: {}}
	// Populate database with > maxPDUsPerTransaction
	pduMultiplier := uint32(3)
	for i := 0; i < maxPDUsPerTransaction*int(pduMultiplier); i++ {
		ev := mustCreatePDU(t)
		headeredJSON, _ := json.Marshal(ev)
		nid, _ := db.StoreJSON(pc.Context(), string(headeredJSON))
		err := db.AssociatePDUWithDestinations(pc.Context(), destinations, nid)
		assert.NoError(t, err, "failed to associate PDU with destinations")
	}

	ev := mustCreatePDU(t)
	err := queues.SendEvent(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() == pduMultiplier+1 { // +1 for the extra SendEvent()
			data, dbErr := db.GetPendingPDUs(pc.Context(), destination, 200)
			assert.NoError(t, dbErr)
			if len(data) == 0 {
				return poll.Success()
			}
			return poll.Continue("waiting for all events to be removed from database. Currently present PDU: %d", len(data))
		}
		return poll.Continue("waiting for the right amount of send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
	// })
}

func TestSendEDUBatches(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(16)
	destination := spec.ServerName("remotehost")

	// test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
	// db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, true, t, testOpts, true)
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, true, false, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	destinations := map[spec.ServerName]struct{}{destination: {}}
	// Populate database with > maxEDUsPerTransaction
	eduMultiplier := uint32(3)
	for i := 0; i < maxEDUsPerTransaction*int(eduMultiplier); i++ {
		ev := mustCreateEDU(t)
		ephemeralJSON, _ := json.Marshal(ev)
		nid, _ := db.StoreJSON(pc.Context(), string(ephemeralJSON))
		err := db.AssociateEDUWithDestinations(pc.Context(), destinations, nid, ev.Type, nil)
		assert.NoError(t, err, "failed to associate EDU with destinations")
	}

	ev := mustCreateEDU(t)
	err := queues.SendEDU(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() == eduMultiplier+1 { // +1 for the extra SendEvent()
			data, dbErr := db.GetPendingEDUs(pc.Context(), destination, 200)
			assert.NoError(t, dbErr)
			if len(data) == 0 {
				return poll.Success()
			}
			return poll.Continue("waiting for all events to be removed from database. Currently present EDU: %d", len(data))
		}
		return poll.Continue("waiting for the right amount of send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
	// })
}

func TestSendPDUAndEDUBatches(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(16)
	destination := spec.ServerName("remotehost")

	// test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
	// db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, true, t, testOpts, true)
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, true, false, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	destinations := map[spec.ServerName]struct{}{destination: {}}
	// Populate database with > maxEDUsPerTransaction
	multiplier := uint32(3)
	for i := 0; i < maxPDUsPerTransaction*int(multiplier)+1; i++ {
		ev := mustCreatePDU(t)
		headeredJSON, _ := json.Marshal(ev)
		nid, _ := db.StoreJSON(pc.Context(), string(headeredJSON))
		err := db.AssociatePDUWithDestinations(pc.Context(), destinations, nid)
		assert.NoError(t, err, "failed to associate PDU with destinations")
	}

	for i := 0; i < maxEDUsPerTransaction*int(multiplier); i++ {
		ev := mustCreateEDU(t)
		ephemeralJSON, _ := json.Marshal(ev)
		nid, _ := db.StoreJSON(pc.Context(), string(ephemeralJSON))
		err := db.AssociateEDUWithDestinations(pc.Context(), destinations, nid, ev.Type, nil)
		assert.NoError(t, err, "failed to associate EDU with destinations")
	}

	ev := mustCreateEDU(t)
	err := queues.SendEDU(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() == multiplier+1 { // +1 for the extra SendEvent()
			pduData, dbErrPDU := db.GetPendingPDUs(pc.Context(), destination, 200)
			assert.NoError(t, dbErrPDU)
			eduData, dbErrEDU := db.GetPendingEDUs(pc.Context(), destination, 200)
			assert.NoError(t, dbErrEDU)
			if len(pduData) == 0 && len(eduData) == 0 {
				return poll.Success()
			}
			return poll.Continue("waiting for all events to be removed from database. Currently present PDU: %d EDU: %d", len(pduData), len(eduData))
		}
		return poll.Continue("waiting for the right amount of send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
	// })
}

func TestExternalFailureBackoffDoesntStartQueue(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(16)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, true, false, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	dest := queues.getQueue(destination)
	queues.statistics.ForServer(destination).Failure()
	destinations := map[spec.ServerName]struct{}{destination: {}}
	ev := mustCreatePDU(t)
	headeredJSON, _ := json.Marshal(ev)
	nid, _ := db.StoreJSON(pc.Context(), string(headeredJSON))
	err := db.AssociatePDUWithDestinations(pc.Context(), destinations, nid)
	assert.NoError(t, err, "failed to associate PDU with destinations")

	pollEnd := time.Now().Add(3 * time.Second)
	runningCheck := func(log poll.LogT) poll.Result {
		if dest.running.Load() || fc.txCount.Load() > 0 {
			return poll.Error(fmt.Errorf("The queue was started"))
		}
		if time.Now().After(pollEnd) {
			// Allow more than enough time for the queue to be started in the case
			// of backoff triggering it to start.
			return poll.Success()
		}
		return poll.Continue("waiting to ensure queue doesn't start.")
	}
	poll.WaitOn(t, runningCheck, poll.WithTimeout(4*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestQueueInteractsWithRealDatabasePDUAndEDU(t *testing.T) {
	// NOTE : Only one test case against real databases can be run at a time.
	t.Parallel()
	failuresUntilBlacklist := uint32(1)
	destination := spec.ServerName("remotehost")
	destinations := map[spec.ServerName]struct{}{destination: {}}
	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {
		db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilBlacklist+1, false, false, t, true)
		// NOTE : These defers aren't called if go test is killed so the dbs may not get cleaned up.

		defer func() {
			closeDb()
			pc.ShutdownDendrite()
			<-pc.WaitForShutdown()
		}()

		// NOTE : getQueue before sending event to ensure we grab the same queue reference
		// before it is blacklisted and deleted.
		dest := queues.getQueue(destination)
		ev := mustCreatePDU(t)
		err := queues.SendEvent(ev, "localhost", []spec.ServerName{destination})
		assert.NoError(t, err)

		// NOTE : The server can be blacklisted before this, so manually inject the event
		// into the database.
		edu := mustCreateEDU(t)
		ephemeralJSON, _ := json.Marshal(edu)
		nid, _ := db.StoreJSON(pc.Context(), string(ephemeralJSON))
		err = db.AssociateEDUWithDestinations(pc.Context(), destinations, nid, edu.Type, nil)
		assert.NoError(t, err, "failed to associate EDU with destinations")

		checkBlacklisted := func(log poll.LogT) poll.Result {
			if fc.txCount.Load() == failuresUntilBlacklist {
				pduData, dbErrPDU := db.GetPendingPDUs(pc.Context(), destination, 200)
				assert.NoError(t, dbErrPDU)
				eduData, dbErrEDU := db.GetPendingEDUs(pc.Context(), destination, 200)
				assert.NoError(t, dbErrEDU)
				if len(pduData) == 1 && len(eduData) == 1 {
					if val, _ := db.IsServerBlacklisted(destination); val {
						if !dest.running.Load() {
							return poll.Success()
						}
						return poll.Continue("waiting for queue to stop completely")
					}
					return poll.Continue("waiting for server to be blacklisted")
				}
				return poll.Continue("waiting for events to be added to database. Currently present PDU: %d EDU: %d", len(pduData), len(eduData))
			}
			return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
		}
		poll.WaitOn(t, checkBlacklisted, poll.WithTimeout(10*time.Second), poll.WithDelay(100*time.Millisecond))

		fc.shouldTxSucceed = true
		wasBlacklisted := dest.statistics.MarkServerAlive()
		queues.RetryServer(destination, wasBlacklisted)
		checkRetry := func(log poll.LogT) poll.Result {
			pduData, dbErrPDU := db.GetPendingPDUs(pc.Context(), destination, 200)
			assert.NoError(t, dbErrPDU)
			eduData, dbErrEDU := db.GetPendingEDUs(pc.Context(), destination, 200)
			assert.NoError(t, dbErrEDU)
			if len(pduData) == 0 && len(eduData) == 0 {
				return poll.Success()
			}
			return poll.Continue("waiting for events to be removed from database. Currently present PDU: %d EDU: %d", len(pduData), len(eduData))
		}
		poll.WaitOn(t, checkRetry, poll.WithTimeout(10*time.Second), poll.WithDelay(100*time.Millisecond))
	})
}

func TestSendPDUMultipleFailuresAssumedOffline(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(7)
	failuresUntilAssumedOffline := uint32(2)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilAssumedOffline, false, false, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	ev := mustCreatePDU(t)
	err := queues.SendEvent(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() == failuresUntilAssumedOffline {
			data, dbErr := db.GetPendingPDUs(pc.Context(), destination, 100)
			assert.NoError(t, dbErr)
			if len(data) == 1 {
				if val, _ := db.IsServerAssumedOffline(context.Background(), destination); val {
					return poll.Success()
				}
				return poll.Continue("waiting for server to be assumed offline")
			}
			return poll.Continue("waiting for event to be added to database. Currently present PDU: %d", len(data))
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestSendEDUMultipleFailuresAssumedOffline(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(7)
	failuresUntilAssumedOffline := uint32(2)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilAssumedOffline, false, false, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	ev := mustCreateEDU(t)
	err := queues.SendEDU(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() == failuresUntilAssumedOffline {
			data, dbErr := db.GetPendingEDUs(pc.Context(), destination, 100)
			assert.NoError(t, dbErr)
			if len(data) == 1 {
				if val, _ := db.IsServerAssumedOffline(context.Background(), destination); val {
					return poll.Success()
				}
				return poll.Continue("waiting for server to be assumed offline")
			}
			return poll.Continue("waiting for event to be added to database. Currently present EDU: %d", len(data))
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))
}

func TestSendPDUOnRelaySuccessRemovedFromDB(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(16)
	failuresUntilAssumedOffline := uint32(1)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilAssumedOffline, false, true, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	relayServers := []spec.ServerName{"relayserver"}
	queues.statistics.ForServer(destination).AddRelayServers(relayServers)

	ev := mustCreatePDU(t)
	err := queues.SendEvent(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() >= 1 {
			if fc.txRelayCount.Load() == 1 {
				data, dbErr := db.GetPendingPDUs(pc.Context(), destination, 100)
				assert.NoError(t, dbErr)
				if len(data) == 0 {
					return poll.Success()
				}
				return poll.Continue("waiting for event to be removed from database. Currently present PDU: %d", len(data))
			}
			return poll.Continue("waiting for more relay send attempts before checking database. Currently %d", fc.txRelayCount.Load())
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))

	assumedOffline, _ := db.IsServerAssumedOffline(context.Background(), destination)
	assert.Equal(t, true, assumedOffline)
}

func TestSendEDUOnRelaySuccessRemovedFromDB(t *testing.T) {
	t.Parallel()
	failuresUntilBlacklist := uint32(16)
	failuresUntilAssumedOffline := uint32(1)
	destination := spec.ServerName("remotehost")
	db, fc, queues, pc, closeDb := testSetup(failuresUntilBlacklist, failuresUntilAssumedOffline, false, true, t, false)

	defer func() {
		closeDb()
		pc.ShutdownDendrite()
		<-pc.WaitForShutdown()
	}()

	relayServers := []spec.ServerName{"relayserver"}
	queues.statistics.ForServer(destination).AddRelayServers(relayServers)

	ev := mustCreateEDU(t)
	err := queues.SendEDU(ev, "localhost", []spec.ServerName{destination})
	assert.NoError(t, err)

	check := func(log poll.LogT) poll.Result {
		if fc.txCount.Load() >= 1 {
			if fc.txRelayCount.Load() == 1 {
				data, dbErr := db.GetPendingEDUs(pc.Context(), destination, 100)
				assert.NoError(t, dbErr)
				if len(data) == 0 {
					return poll.Success()
				}
				return poll.Continue("waiting for event to be removed from database. Currently present EDU: %d", len(data))
			}
			return poll.Continue("waiting for more relay send attempts before checking database. Currently %d", fc.txRelayCount.Load())
		}
		return poll.Continue("waiting for more send attempts before checking database. Currently %d", fc.txCount.Load())
	}
	poll.WaitOn(t, check, poll.WithTimeout(5*time.Second), poll.WithDelay(100*time.Millisecond))

	assumedOffline, _ := db.IsServerAssumedOffline(context.Background(), destination)
	assert.Equal(t, true, assumedOffline)
}
