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

package consumers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/antinvestor/matrix/internal/queueutil"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/federationapi/queue"
	"github.com/antinvestor/matrix/federationapi/storage"
	"github.com/antinvestor/matrix/federationapi/types"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/jetstream"
	log "github.com/sirupsen/logrus"

	syncAPITypes "github.com/antinvestor/matrix/syncapi/types"

	"buf.build/gen/go/antinvestor/presence/connectrpc/go/presencev1connect"
	presenceV1 "buf.build/gen/go/antinvestor/presence/protocolbuffers/go"
	"connectrpc.com/connect"
)

// OutputRoomEventConsumer consumes events that originated in the room server.
type OutputRoomEventConsumer struct {
	cfg            *config.FederationAPI
	rsAPI          api.FederationRoomserverAPI
	qm             queueutil.QueueManager
	db             storage.Database
	queues         *queue.OutgoingQueues
	presenceClient presencev1connect.PresenceServiceClient
}

// NewOutputRoomEventConsumer creates a new OutputRoomEventConsumer. Call Start() to begin consuming from room servers.
func NewOutputRoomEventConsumer(
	ctx context.Context,
	cfg *config.FederationAPI,
	qm queueutil.QueueManager,
	queues *queue.OutgoingQueues,
	store storage.Database,
	rsAPI api.FederationRoomserverAPI,
	presenceClient presencev1connect.PresenceServiceClient,
) error {
	c := &OutputRoomEventConsumer{
		cfg:            cfg,
		qm:             qm,
		db:             store,
		queues:         queues,
		rsAPI:          rsAPI,
		presenceClient: presenceClient,
	}

	return qm.RegisterSubscriber(ctx, &cfg.Queues.OutputRoomEvent, c)
}

// Handle is called when the federation server receives a new event from the room server output log.
// It is unsafe to call this with messages for the same room in multiple gorountines
// because updates it will likely fail with a types.EventIDMismatchError when it
// realises that it cannot update the room state using the deltas.
func (s *OutputRoomEventConsumer) Handle(ctx context.Context, metadata map[string]string, message []byte) error {
	receivedType := api.OutputType(metadata[jetstream.RoomEventType])

	// Only handle events we care about, avoids unneeded unmarshalling
	switch receivedType {
	case api.OutputTypeNewRoomEvent, api.OutputTypeNewInboundPeek, api.OutputTypePurgeRoom:
	default:
		return nil
	}

	// Parse out the event JSON
	var output api.OutputEvent
	err := json.Unmarshal(message, &output)
	if err != nil {
		// If the message was invalid, log it and move on to the next message in the stream
		log.WithError(err).Errorf("roomserver output log: message parse failure")
		return nil
	}

	switch output.Type {
	case api.OutputTypeNewRoomEvent:
		ev := output.NewRoomEvent.Event
		err = s.processMessage(ctx, *output.NewRoomEvent, output.NewRoomEvent.RewritesState)
		if err != nil {
			// panic rather than continue with an inconsistent database
			log.WithFields(log.Fields{
				"event_id":   ev.EventID(),
				"event":      string(ev.JSON()),
				"add":        output.NewRoomEvent.AddsStateEventIDs,
				"del":        output.NewRoomEvent.RemovesStateEventIDs,
				log.ErrorKey: err,
			}).Panicf("roomserver output log: write room event failure")
		}

	case api.OutputTypeNewInboundPeek:
		err = s.processInboundPeek(ctx, *output.NewInboundPeek)
		if err != nil {
			log.WithFields(log.Fields{
				"event":      output.NewInboundPeek,
				log.ErrorKey: err,
			}).Panicf("roomserver output log: remote peek event failure")
			return err
		}

	case api.OutputTypePurgeRoom:
		log.WithField("room_id", output.PurgeRoom.RoomID).Warn("Purging room from federation API")
		err = s.db.PurgeRoom(ctx, output.PurgeRoom.RoomID)
		if err != nil {
			log.WithField("room_id", output.PurgeRoom.RoomID).WithError(err).Error("Failed to purge room from federation API")
		} else {
			log.WithField("room_id", output.PurgeRoom.RoomID).Warn("Room purged from federation API")
		}

	default:
		log.WithField("type", output.Type).Debug(
			"roomserver output log: ignoring unknown output type",
		)
	}

	return nil
}

// processInboundPeek starts tracking a new federated inbound peek (replacing the existing one if any)
// causing the federationapi to start sending messages to the peeking server
func (s *OutputRoomEventConsumer) processInboundPeek(ctx context.Context, orp api.OutputNewInboundPeek) error {

	// FIXME: there's a race here - we should start /sending new peeked events
	// atomically after the orp.LatestEventID to ensure there are no gaps between
	// the peek beginning and the send stream beginning.
	//
	// We probably need to track orp.LatestEventID on the inbound peek, but it's
	// unclear how we then use that to prevent the race when we start the send
	// stream.
	//
	// This is making the tests flakey.

	return s.db.AddInboundPeek(ctx, orp.ServerName, orp.RoomID, orp.PeekID, orp.RenewalInterval)
}

// processMessage updates the list of currently joined hosts in the room
// and then sends the event to the hosts that were joined before the event.
func (s *OutputRoomEventConsumer) processMessage(ctx context.Context, ore api.OutputNewRoomEvent, rewritesState bool) error {

	addsStateEvents, missingEventIDs := ore.NeededStateEventIDs()

	// Ask the roomserver and add in the rest of the results into the set.
	// Finally, work out if there are any more events missing.
	if len(missingEventIDs) > 0 {
		eventsReq := &api.QueryEventsByIDRequest{
			RoomID:   ore.Event.RoomID().String(),
			EventIDs: missingEventIDs,
		}
		eventsRes := &api.QueryEventsByIDResponse{}
		if err := s.rsAPI.QueryEventsByID(ctx, eventsReq, eventsRes); err != nil {
			return fmt.Errorf("s.rsAPI.QueryEventsByID: %w", err)
		}
		if len(eventsRes.Events) != len(missingEventIDs) {
			return fmt.Errorf("missing state events")
		}
		addsStateEvents = append(addsStateEvents, eventsRes.Events...)
	}

	evs := make([]gomatrixserverlib.PDU, len(addsStateEvents))
	for i := range evs {
		evs[i] = addsStateEvents[i].PDU
	}

	addsJoinedHosts, err := JoinedHostsFromEvents(ctx, evs, s.rsAPI)
	if err != nil {
		return err
	}
	// Update our copy of the current state.
	// We keep a copy of the current state because the state at each event is
	// expressed as a delta against the current state.
	// TODO(#290): handle EventIDMismatchError and recover the current state by
	// talking to the roomserver
	oldJoinedHosts, err := s.db.UpdateRoom(
		ctx,
		ore.Event.RoomID().String(),
		addsJoinedHosts,
		ore.RemovesStateEventIDs,
		rewritesState, // if we're re-writing state, nuke all joined hosts before adding
	)
	if err != nil {
		return err
	}

	// If we added new hosts, inform them about our known presence events for this room
	if s.cfg.Global.Presence.EnableOutbound && len(addsJoinedHosts) > 0 && ore.Event.Type() == spec.MRoomMember && ore.Event.StateKey() != nil {
		membership, _ := ore.Event.Membership()
		if membership == spec.Join {
			s.sendPresence(ctx, ore.Event.RoomID().String(), addsJoinedHosts)
		}
	}

	if oldJoinedHosts == nil {
		// This means that there is nothing to update as this is a duplicate
		// message.
		// This can happen if dendrite crashed between reading the message and
		// persisting the stream position.
		return nil
	}

	if ore.SendAsServer == api.DoNotSendToOtherServers {
		// Ignore event that we don't need to send anywhere.
		return nil
	}

	// Work out which hosts were joined at the event itself.
	joinedHostsAtEvent, err := s.joinedHostsAtEvent(ctx, ore, oldJoinedHosts)
	if err != nil {
		return err
	}

	// TODO: do housekeeping to evict unrenewed peeking hosts

	// TODO: implement query to let the fedapi check whether a given peek is live or not

	// Send the event.
	return s.queues.SendEvent(ctx,
		ore.Event, spec.ServerName(ore.SendAsServer), joinedHostsAtEvent,
	)
}

func (s *OutputRoomEventConsumer) sendPresence(ctx context.Context, roomID string, addedJoined []types.JoinedHost) {
	joined := make([]spec.ServerName, 0, len(addedJoined))
	for _, added := range addedJoined {
		joined = append(joined, added.ServerName)
	}

	// get our locally joined users
	var queryRes api.QueryMembershipsForRoomResponse
	err := s.rsAPI.QueryMembershipsForRoom(ctx, &api.QueryMembershipsForRoomRequest{
		JoinedOnly: true,
		LocalOnly:  true,
		RoomID:     roomID,
	}, &queryRes)
	if err != nil {
		log.WithError(err).Error("failed to calculate joined rooms for user")
		return
	}

	// send every presence we know about to the remote server
	content := types.Presence{}
	for _, ev := range queryRes.JoinEvents {

		resp, err0 := s.presenceClient.GetPresence(ctx, connect.NewRequest(&presenceV1.GetPresenceRequest{
			UserId: ev.Sender,
		}))
		if err0 != nil {
			log.WithError(err0).Errorf("unable to get presence")
			continue
		}

		presence := resp.Msg
		statusMsg := presence.GetStatusMsg()

		lastActive := presence.GetLastActiveTs()

		p := syncAPITypes.PresenceInternal{LastActiveTS: spec.Timestamp(lastActive)}

		content.Push = append(content.Push, types.PresenceContent{
			CurrentlyActive: p.CurrentlyActive(),
			LastActiveAgo:   p.LastActiveAgo(),
			Presence:        presence.GetPresence(),
			StatusMsg:       &statusMsg,
			UserID:          ev.Sender,
		})
	}

	if len(content.Push) == 0 {
		return
	}

	edu := &gomatrixserverlib.EDU{
		Type:   spec.MPresence,
		Origin: string(s.cfg.Global.ServerName),
	}
	if edu.Content, err = json.Marshal(content); err != nil {
		log.WithError(err).Error("failed to marshal EDU JSON")
		return
	}
	if err := s.queues.SendEDU(ctx, edu, s.cfg.Global.ServerName, joined); err != nil {
		log.WithError(err).Error("failed to send EDU")
	}
}

// joinedHostsAtEvent works out a list of matrix servers that were joined to
// the room at the event (including peeking ones)
// It is important to use the state at the event for sending messages because:
//
//  1. We shouldn't send messages to servers that weren't in the room.
//  2. If a server is kicked from the rooms it should still be told about the
//     kick event.
//
// Usually the list can be calculated locally, but sometimes it will need fetch
// events from the room server.
// Returns an error if there was a problem talking to the room server.
func (s *OutputRoomEventConsumer) joinedHostsAtEvent(
	ctx context.Context, ore api.OutputNewRoomEvent, oldJoinedHosts []types.JoinedHost,
) ([]spec.ServerName, error) {
	// Combine the delta into a single delta so that the adds and removes can
	// cancel each other out. This should reduce the number of times we need
	// to fetch a state event from the room server.
	combinedAdds, combinedRemoves := combineDeltas(
		ore.AddsStateEventIDs, ore.RemovesStateEventIDs,
		ore.StateBeforeAddsEventIDs, ore.StateBeforeRemovesEventIDs,
	)
	combinedAddsEvents, err := s.lookupStateEvents(ctx, combinedAdds, ore.Event.PDU)
	if err != nil {
		return nil, err
	}

	combinedAddsJoinedHosts, err := JoinedHostsFromEvents(ctx, combinedAddsEvents, s.rsAPI)
	if err != nil {
		return nil, err
	}

	removed := map[string]bool{}
	for _, eventID := range combinedRemoves {
		removed[eventID] = true
	}

	joined := map[spec.ServerName]bool{}
	for _, joinedHost := range oldJoinedHosts {
		if removed[joinedHost.MemberEventID] {
			// This m.room.member event is part of the current state of the
			// room, but not part of the state at the event we are processing
			// Therefore we can't use it to tell whether the server was in
			// the room at the event.
			continue
		}
		joined[joinedHost.ServerName] = true
	}

	for _, joinedHost := range combinedAddsJoinedHosts {
		// This m.room.member event was part of the state of the room at the
		// event, but isn't part of the current state of the room now.
		joined[joinedHost.ServerName] = true
	}

	// handle peeking hosts
	inboundPeeks, err := s.db.GetInboundPeeks(ctx, ore.Event.PDU.RoomID().String())
	if err != nil {
		return nil, err
	}
	for _, inboundPeek := range inboundPeeks {
		joined[inboundPeek.ServerName] = true
	}

	var result []spec.ServerName
	for serverName, include := range joined {
		if include {
			result = append(result, serverName)
		}
	}
	return result, nil
}

// JoinedHostsFromEvents turns a list of state events into a list of joined hosts.
// This errors if one of the events was invalid.
// It should be impossible for an invalid event to get this far in the pipeline.
func JoinedHostsFromEvents(ctx context.Context, evs []gomatrixserverlib.PDU, rsAPI api.FederationRoomserverAPI) ([]types.JoinedHost, error) {
	var joinedHosts []types.JoinedHost
	for _, ev := range evs {
		if ev.Type() != "m.room.member" || ev.StateKey() == nil {
			continue
		}
		membership, err := ev.Membership()
		if err != nil {
			return nil, err
		}
		if membership != spec.Join {
			continue
		}
		var domain spec.ServerName
		userID, err := rsAPI.QueryUserIDForSender(ctx, ev.RoomID(), spec.SenderID(*ev.StateKey()))
		if err != nil {
			if errors.As(err, new(base64.CorruptInputError)) {
				// Fallback to using the "old" way of getting the user domain, avoids
				// "illegal base64 data at input byte 0" errors
				// FIXME: we should do this in QueryUserIDForSender instead
				_, domain, err = gomatrixserverlib.SplitID('@', *ev.StateKey())
				if err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		} else {
			domain = userID.Domain()
		}

		joinedHosts = append(joinedHosts, types.JoinedHost{
			MemberEventID: ev.EventID(), ServerName: domain,
		})
	}
	return joinedHosts, nil
}

// combineDeltas combines two deltas into a single delta.
// Assumes that the order of operations is add(1), remove(1), add(2), remove(2).
// Removes duplicate entries and redundant operations from each delta.
func combineDeltas(adds1, removes1, adds2, removes2 []string) (adds, removes []string) {
	addSet := map[string]bool{}
	removeSet := map[string]bool{}

	// combine processes each unique value in a list.
	// If the value is in the removeFrom set then it is removed from that set.
	// Otherwise it is added to the addTo set.
	combine := func(values []string, removeFrom, addTo map[string]bool) {
		processed := map[string]bool{}
		for _, value := range values {
			if processed[value] {
				continue
			}
			processed[value] = true
			if removeFrom[value] {
				delete(removeFrom, value)
			} else {
				addTo[value] = true
			}
		}
	}

	combine(adds1, nil, addSet)
	combine(removes1, addSet, removeSet)
	combine(adds2, removeSet, addSet)
	combine(removes2, addSet, removeSet)

	for value := range addSet {
		adds = append(adds, value)
	}
	for value := range removeSet {
		removes = append(removes, value)
	}
	return
}

// lookupStateEvents looks up the state events that are added by a new event.
func (s *OutputRoomEventConsumer) lookupStateEvents(
	ctx context.Context, addsStateEventIDs []string, event gomatrixserverlib.PDU,
) ([]gomatrixserverlib.PDU, error) {
	// Fast path if there aren't any new state events.
	if len(addsStateEventIDs) == 0 {
		return nil, nil
	}

	// Fast path if the only state event added is the event itself.
	if len(addsStateEventIDs) == 1 && addsStateEventIDs[0] == event.EventID() {
		return []gomatrixserverlib.PDU{event}, nil
	}

	missing := addsStateEventIDs
	var result []gomatrixserverlib.PDU

	// Check if event itself is being added.
	for _, eventID := range missing {
		if eventID == event.EventID() {
			result = append(result, event)
			break
		}
	}
	missing = missingEventsFrom(result, addsStateEventIDs)

	if len(missing) == 0 {
		return result, nil
	}

	// At this point the missing events are neither the event itself nor are
	// they present in our local database. Our only option is to fetch them
	// from the room server using the query API.
	eventReq := api.QueryEventsByIDRequest{EventIDs: missing, RoomID: event.RoomID().String()}
	var eventResp api.QueryEventsByIDResponse
	if err := s.rsAPI.QueryEventsByID(ctx, &eventReq, &eventResp); err != nil {
		return nil, err
	}

	for _, headeredEvent := range eventResp.Events {
		result = append(result, headeredEvent.PDU)
	}

	missing = missingEventsFrom(result, addsStateEventIDs)

	if len(missing) != 0 {
		return nil, fmt.Errorf(
			"missing %d state events IDs at event %q", len(missing), event.EventID(),
		)
	}

	return result, nil
}

func missingEventsFrom(events []gomatrixserverlib.PDU, required []string) []string {
	have := map[string]bool{}
	for _, event := range events {
		have[event.EventID()] = true
	}
	var missing []string
	for _, eventID := range required {
		if !have[eventID] {
			missing = append(missing, eventID)
		}
	}
	return missing
}
