// Copyright 2020 New Vector Ltd
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

package perform

import (
	"context"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/internal/helpers"
	"github.com/antinvestor/matrix/roomserver/internal/input"
	"github.com/antinvestor/matrix/roomserver/internal/query"
	"github.com/antinvestor/matrix/roomserver/state"
	"github.com/antinvestor/matrix/roomserver/storage"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/pitabwire/util"
)

type InboundPeeker struct {
	DB      storage.RoomDatabase
	Inputer *input.Inputer
}

// PerformInboundPeek handles peeking into matrix rooms, including over
// federation by talking to the federationapi. called when a remote server
// initiates a /peek over federation.
//
// It should atomically figure out the current state of the room (for the
// response to /peek) while adding the new inbound peek to the kafka stream so the
// fed sender can start sending peeked events without a race between the state
// snapshot and the stream of peeked events.
func (r *InboundPeeker) PerformInboundPeek(
	ctx context.Context,
	request *api.PerformInboundPeekRequest,
	response *api.PerformInboundPeekResponse,
) error {

	roomID, err := spec.NewRoomID(request.RoomID)
	if err != nil {
		return err
	}

	info, err := r.DB.RoomInfo(ctx, request.RoomID)
	if err != nil {
		return err
	}
	if info == nil || info.IsStub() {
		return nil
	}
	response.RoomExists = true
	response.RoomVersion = info.RoomVersion

	var stateEvents []gomatrixserverlib.PDU

	var currentStateSnapshotNID types.StateSnapshotNID
	latestEventRefs, currentStateSnapshotNID, _, err :=
		r.DB.LatestEventIDs(ctx, info.RoomNID)
	if err != nil {
		return err
	}
	latestEvents, err := r.DB.EventsFromIDs(ctx, info, []string{latestEventRefs[0]})
	if err != nil {
		return err
	}
	var sortedLatestEvents []gomatrixserverlib.PDU
	for _, ev := range latestEvents {
		sortedLatestEvents = append(sortedLatestEvents, ev.PDU)
	}
	sortedLatestEvents = gomatrixserverlib.ReverseTopologicalOrdering(
		sortedLatestEvents,
		gomatrixserverlib.TopologicalOrderByPrevEvents,
	)
	response.LatestEvent = &types.HeaderedEvent{PDU: sortedLatestEvents[0]}

	// XXX: do we actually need to do a state resolution here?
	roomState := state.NewStateResolution(r.DB, info, r.Inputer.Queryer)

	var stateEntries []types.StateEntry
	stateEntries, err = roomState.LoadStateAtSnapshot(
		ctx, currentStateSnapshotNID,
	)
	if err != nil {
		return err
	}
	stateEvents, err = helpers.LoadStateEvents(ctx, r.DB, info, stateEntries)
	if err != nil {
		return err
	}

	// get the auth event IDs for the current state events
	var authEventIDs []string
	for _, se := range stateEvents {
		authEventIDs = append(authEventIDs, se.AuthEventIDs()...)
	}
	authEventIDs = util.UniqueStrings(authEventIDs) // de-dupe

	authEvents, err := query.GetAuthChain(ctx, r.DB.EventsFromIDs, info, authEventIDs)
	if err != nil {
		return err
	}

	for _, event := range stateEvents {
		response.StateEvents = append(response.StateEvents, &types.HeaderedEvent{PDU: event})
	}

	for _, event := range authEvents {
		response.AuthChainEvents = append(response.AuthChainEvents, &types.HeaderedEvent{PDU: event})
	}

	err = r.Inputer.OutputProducer.ProduceRoomEvents(ctx, roomID, []api.OutputEvent{
		{
			Type: api.OutputTypeNewInboundPeek,
			NewInboundPeek: &api.OutputNewInboundPeek{
				RoomID:          request.RoomID,
				PeekID:          request.PeekID,
				LatestEventID:   latestEvents[0].EventID(),
				ServerName:      request.ServerName,
				RenewalInterval: request.RenewalInterval,
			},
		},
	})
	return err
}
