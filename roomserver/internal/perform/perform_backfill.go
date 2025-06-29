// Copyright 2025 Ant Investor Ltd.
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
	"fmt"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	federationAPI "github.com/antinvestor/matrix/federationapi/api"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/auth"
	"github.com/antinvestor/matrix/roomserver/internal/helpers"
	"github.com/antinvestor/matrix/roomserver/state"
	"github.com/antinvestor/matrix/roomserver/storage"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/pitabwire/util"
)

// the max number of servers to backfill from per request. If this is too low we may fail to backfill when
// we could've from another server. If this is too high we may take far too long to successfully backfill
// as we try dead servers.
const maxBackfillServers = 5

type Backfiller struct {
	IsLocalServerName func(spec.ServerName) bool
	DB                storage.Database
	FSAPI             federationAPI.RoomserverFederationAPI
	KeyRing           gomatrixserverlib.JSONVerifier
	Querier           api.QuerySenderIDAPI

	// The servers which should be preferred above other servers when backfilling
	PreferServers []spec.ServerName
}

// PerformBackfill implements api.RoomServerQueryAPI
func (r *Backfiller) PerformBackfill(
	ctx context.Context,
	request *api.PerformBackfillRequest,
	response *api.PerformBackfillResponse,
) error {
	log := util.Log(ctx)
	// if we are requesting the backfill then we need to do a federation hit
	// TODO: we could be more sensible and fetch as many events we already have then request the rest
	//       which is what the syncapi does already.
	if r.IsLocalServerName(request.ServerName) {
		return r.backfillViaFederation(ctx, request, response)
	}
	// someone else is requesting the backfill, try to service their request.
	var err error
	var front []string

	// The limit defines the maximum number of events to retrieve, so it also
	// defines the highest number of elements in the map below.
	visited := make(map[string]bool, request.Limit)

	// this will include these events which is what we want
	front = request.PrevEventIDs()

	info, err := r.DB.RoomInfo(ctx, request.RoomID)
	if err != nil {
		return err
	}
	if info == nil || info.IsStub() {
		log.WithField("room_id", request.RoomID).Error("PerformBackfill: missing room info for room")
		return fmt.Errorf("PerformBackfill: missing room info for room %s", request.RoomID)
	}

	// Scan the event tree for events to send back.
	resultNIDs, redactEventIDs, err := helpers.ScanEventTree(ctx, r.DB, info, front, visited, request.Limit, request.ServerName, r.Querier)
	if err != nil {
		return err
	}

	// Retrieve events from the list that was filled previously. If we fail to get
	// events from the database then attempt once to get them from federation instead.
	var loadedEvents []gomatrixserverlib.PDU
	loadedEvents, err = helpers.LoadEvents(ctx, r.DB, info, resultNIDs)
	if err != nil {
		if _, ok := err.(types.MissingEventError); ok {
			return r.backfillViaFederation(ctx, request, response)
		}
		return err
	}

	for _, event := range loadedEvents {
		if _, ok := redactEventIDs[event.EventID()]; ok {
			event.Redact()
		}
		response.Events = append(response.Events, &types.HeaderedEvent{PDU: event})
	}

	return err
}

func (r *Backfiller) backfillViaFederation(ctx context.Context, req *api.PerformBackfillRequest, res *api.PerformBackfillResponse) error {
	log := util.Log(ctx)
	info, err := r.DB.RoomInfo(ctx, req.RoomID)
	if err != nil {
		return err
	}
	if info == nil || info.IsStub() {
		log.WithField("room_id", req.RoomID).Error("backfillViaFederation: missing room info for room")
		return fmt.Errorf("backfillViaFederation: missing room info for room %s", req.RoomID)
	}
	requester := newBackfillRequester(r.DB, r.FSAPI, r.Querier, req.VirtualHost, r.IsLocalServerName, req.BackwardsExtremities, r.PreferServers, info.RoomVersion)
	// Request 100 items regardless of what the query asks for.
	// We don't want to go much higher than this.
	// We can't honour exactly the limit as some sytests rely on requesting more for tests to pass
	// (so we don't need to hit /state_ids which the test has no listener for)
	// Specifically the test "Outbound federation can backfill events"
	events, err := gomatrixserverlib.RequestBackfill(
		ctx, req.VirtualHost, requester,
		r.KeyRing, req.RoomID, info.RoomVersion, req.PrevEventIDs(), 100, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return r.Querier.QueryUserIDForSender(ctx, roomID, senderID)
		},
	)
	// Only return an error if we really couldn't get any events.
	if err != nil && len(events) == 0 {
		log.WithError(err).Error("gomatrixserverlib.RequestBackfill failed")
		return err
	}
	// If we got an error but still got events, that's fine, because a server might have returned a 404 (or something)
	// but other servers could provide the missing event.
	log.WithError(err).WithField("room_id", req.RoomID).WithField("count", len(events)).Info("backfilled events")

	// persist these new events - auth checks have already been done
	roomNID, backfilledEventMap := persistEvents(ctx, r.DB, r.Querier, events)

	for _, ev := range backfilledEventMap {
		// now add state for these events
		stateIDs, ok := requester.eventIDToBeforeStateIDs[ev.EventID()]
		if !ok {
			// this should be impossible as all events returned must have pass Step 5 of the PDU checks
			// which requires a list of state IDs.
			log.WithError(err).WithField("event_id", ev.EventID()).Error("backfillViaFederation: failed to find state IDs for event which passed auth checks")
			continue
		}
		var entries []types.StateEntry
		if entries, err = r.DB.StateEntriesForEventIDs(ctx, stateIDs, true); err != nil {
			// attempt to fetch the missing events
			r.fetchAndStoreMissingEvents(ctx, info.RoomVersion, requester, stateIDs, req.VirtualHost)
			// try again
			entries, err = r.DB.StateEntriesForEventIDs(ctx, stateIDs, true)
			if err != nil {
				log.WithError(err).WithField("event_id", ev.EventID()).Error("backfillViaFederation: failed to get state entries for event")
				return err
			}
		}

		var beforeStateSnapshotNID types.StateSnapshotNID
		if beforeStateSnapshotNID, err = r.DB.AddState(ctx, roomNID, nil, entries); err != nil {
			log.WithError(err).WithField("event_id", ev.EventID()).Error("backfillViaFederation: failed to persist state entries to get snapshot nid")
			return err
		}
		if err = r.DB.SetState(ctx, ev.EventNID, beforeStateSnapshotNID); err != nil {
			log.WithError(err).WithField("event_id", ev.EventID()).Error("backfillViaFederation: failed to persist snapshot nid")
		}
	}

	// TODO: update backwards extremities, as that should be moved from syncapi to roomserver at some point.

	res.Events = make([]*types.HeaderedEvent, len(events))
	for i := range events {
		res.Events[i] = &types.HeaderedEvent{PDU: events[i]}
	}
	res.HistoryVisibility = requester.historyVisiblity
	return nil
}

// fetchAndStoreMissingEvents does a best-effort fetch and store of missing events specified in stateIDs. Returns no error as it is just
// best effort.
func (r *Backfiller) fetchAndStoreMissingEvents(ctx context.Context, roomVer gomatrixserverlib.RoomVersion,
	backfillRequester *backfillRequester, stateIDs []string, virtualHost spec.ServerName) {

	baseLog := util.Log(ctx)
	servers := backfillRequester.servers

	// work out which are missing
	nidMap, err := r.DB.EventNIDs(ctx, stateIDs)
	if err != nil {
		baseLog.WithError(err).Warn("cannot query missing events")
		return
	}
	missingMap := make(map[string]*types.HeaderedEvent) // id -> event
	for _, id := range stateIDs {
		if _, ok := nidMap[id]; !ok {
			missingMap[id] = nil
		}
	}
	baseLog.WithField("num_missing_events", len(missingMap)).WithField("num_servers", len(servers)).Info("Fetching missing state events")

	// fetch the events from federation. Loop the servers first so if we find one that works we stick with them
	for _, srv := range servers {
		for id, ev := range missingMap {
			if ev != nil {
				continue // already found
			}
			loopLog := baseLog.WithField("server", srv).WithField("event_id", id)
			res, err := r.FSAPI.GetEvent(ctx, virtualHost, srv, id)
			if err != nil {
				loopLog.WithError(err).Warn("failed to get event from server")
				continue
			}
			loader := gomatrixserverlib.NewEventsLoader(roomVer, r.KeyRing, backfillRequester, backfillRequester.ProvideEvents, false)
			result, err := loader.LoadAndVerify(ctx, res.PDUs, gomatrixserverlib.TopologicalOrderByPrevEvents, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return r.Querier.QueryUserIDForSender(ctx, roomID, senderID)
			})
			if err != nil {
				loopLog.WithError(err).Warn("failed to load and verify event")
				continue
			}
			loopLog.WithField("num_pdus", len(res.PDUs)).WithField("num_events", len(result)).Info("returned PDUs which made events")
			for _, res := range result {
				switch err := res.Error.(type) {
				case nil:
				case gomatrixserverlib.SignatureErr:
					// The signature of the event might not be valid anymore, for example if
					// the key ID was reused with a different signature.
					loopLog.WithError(err).Error("event failed PDU checks, storing anyway")
				case gomatrixserverlib.AuthChainErr, gomatrixserverlib.AuthRulesErr:
					loopLog.WithError(err).Warn("event failed PDU checks")
					continue
				default:
					loopLog.WithError(err).Warn("event failed PDU checks")
					continue
				}
				missingMap[id] = &types.HeaderedEvent{PDU: res.Event}
			}
		}
	}

	var newEvents []gomatrixserverlib.PDU
	for _, ev := range missingMap {
		if ev != nil {
			newEvents = append(newEvents, ev.PDU)
		}
	}
	baseLog.WithField("num_new_events", len(newEvents)).Info("Persisting new events")
	persistEvents(ctx, r.DB, r.Querier, newEvents)
}

// backfillRequester implements gomatrixserverlib.BackfillRequester
type backfillRequester struct {
	db                storage.Database
	fsAPI             federationAPI.RoomserverFederationAPI
	querier           api.QuerySenderIDAPI
	virtualHost       spec.ServerName
	isLocalServerName func(spec.ServerName) bool
	preferServer      map[spec.ServerName]bool
	bwExtrems         map[string][]string

	// per-request state
	servers                 []spec.ServerName
	eventIDToBeforeStateIDs map[string][]string
	eventIDMap              map[string]gomatrixserverlib.PDU
	historyVisiblity        gomatrixserverlib.HistoryVisibility
	roomVersion             gomatrixserverlib.RoomVersion
}

func newBackfillRequester(
	db storage.Database, fsAPI federationAPI.RoomserverFederationAPI,
	querier api.QuerySenderIDAPI,
	virtualHost spec.ServerName,
	isLocalServerName func(spec.ServerName) bool,
	bwExtrems map[string][]string, preferServers []spec.ServerName,
	roomVersion gomatrixserverlib.RoomVersion,
) *backfillRequester {
	preferServer := make(map[spec.ServerName]bool)
	for _, p := range preferServers {
		preferServer[p] = true
	}
	return &backfillRequester{
		db:                      db,
		fsAPI:                   fsAPI,
		querier:                 querier,
		virtualHost:             virtualHost,
		isLocalServerName:       isLocalServerName,
		eventIDToBeforeStateIDs: make(map[string][]string),
		eventIDMap:              make(map[string]gomatrixserverlib.PDU),
		bwExtrems:               bwExtrems,
		preferServer:            preferServer,
		historyVisiblity:        gomatrixserverlib.HistoryVisibilityShared,
		roomVersion:             roomVersion,
	}
}

func (b *backfillRequester) StateIDsBeforeEvent(ctx context.Context, targetEvent gomatrixserverlib.PDU) ([]string, error) {
	log := util.Log(ctx)
	b.eventIDMap[targetEvent.EventID()] = targetEvent
	if ids, ok := b.eventIDToBeforeStateIDs[targetEvent.EventID()]; ok {
		return ids, nil
	}
	if len(targetEvent.PrevEventIDs()) == 0 && targetEvent.Type() == "m.room.create" && targetEvent.StateKeyEquals("") {
		log.WithField("room_id", targetEvent.RoomID().String()).Info("Backfilled to the beginning of the room")
		b.eventIDToBeforeStateIDs[targetEvent.EventID()] = []string{}
		return nil, nil
	}
	// if we have exactly 1 prev event and we know the state of the room at that prev event, then just roll forward the prev event.
	// Else, we have to hit /state_ids because either we don't know the state at all at this event (new backwards extremity) or
	// we don't know the result of state res to merge forks (2 or more prev_events)
	if len(targetEvent.PrevEventIDs()) == 1 {
		prevEventID := targetEvent.PrevEventIDs()[0]
		prevEvent, ok := b.eventIDMap[prevEventID]
		if !ok {
			goto FederationHit
		}
		prevEventStateIDs, ok := b.eventIDToBeforeStateIDs[prevEventID]
		if !ok {
			goto FederationHit
		}
		newStateIDs := b.calculateNewStateIDs(targetEvent, prevEvent, prevEventStateIDs)
		if newStateIDs != nil {
			b.eventIDToBeforeStateIDs[targetEvent.EventID()] = newStateIDs
			return newStateIDs, nil
		}
		// else we failed to calculate the new state, so fallthrough
	}

FederationHit:
	var lastErr error
	log.WithField("event_id", targetEvent.EventID()).Info("Requesting /state_ids at event")
	for _, srv := range b.servers { // hit any valid server
		c := gomatrixserverlib.FederatedStateProvider{
			FedClient:          b.fsAPI,
			RememberAuthEvents: false,
			Server:             srv,
			Origin:             b.virtualHost,
		}
		res, err := c.StateIDsBeforeEvent(ctx, targetEvent)
		if err != nil {
			lastErr = err
			continue
		}
		b.eventIDToBeforeStateIDs[targetEvent.EventID()] = res
		return res, nil
	}
	return nil, lastErr
}

func (b *backfillRequester) calculateNewStateIDs(targetEvent, prevEvent gomatrixserverlib.PDU, prevEventStateIDs []string) []string {
	newStateIDs := prevEventStateIDs[:]
	if prevEvent.StateKey() == nil {
		// state is the same as the previous event
		b.eventIDToBeforeStateIDs[targetEvent.EventID()] = newStateIDs
		return newStateIDs
	}

	missingState := false // true if we are missing the info for a state event ID
	foundEvent := false   // true if we found a (type, state_key) match
	// find which state ID to replace, if any
	for i, id := range newStateIDs {
		ev, ok := b.eventIDMap[id]
		if !ok {
			missingState = true
			continue
		}
		// The state IDs BEFORE the target event are the state IDs BEFORE the prev_event PLUS the prev_event itself
		if ev.Type() == prevEvent.Type() && ev.StateKeyEquals(*prevEvent.StateKey()) {
			newStateIDs[i] = prevEvent.EventID()
			foundEvent = true
			break
		}
	}
	if !foundEvent && !missingState {
		// we can be certain that this is new state
		newStateIDs = append(newStateIDs, prevEvent.EventID())
		foundEvent = true
	}

	if foundEvent {
		b.eventIDToBeforeStateIDs[targetEvent.EventID()] = newStateIDs
		return newStateIDs
	}
	return nil
}

func (b *backfillRequester) StateBeforeEvent(ctx context.Context, roomVer gomatrixserverlib.RoomVersion,
	event gomatrixserverlib.PDU, eventIDs []string) (map[string]gomatrixserverlib.PDU, error) {

	log := util.Log(ctx)
	// try to fetch the events from the database first
	events, err := b.ProvideEvents(ctx, roomVer, eventIDs)
	if err != nil {
		// non-fatal, fallthrough
		log.WithError(err).WithField("event_ids_count", len(eventIDs)).Error("Failed to fetch events from DB for StateBeforeEvent")
	} else {
		log.WithField("fetched_count", len(events)).WithField("requested_count", len(eventIDs)).Info("Fetched events from DB for StateBeforeEvent")
		if len(events) == len(eventIDs) {
			result := make(map[string]gomatrixserverlib.PDU)
			for i := range events {
				result[events[i].EventID()] = events[i]
				b.eventIDMap[events[i].EventID()] = events[i]
			}
			return result, nil
		}
	}

	var lastErr error
	for _, srv := range b.servers {
		c := gomatrixserverlib.FederatedStateProvider{
			FedClient:          b.fsAPI,
			RememberAuthEvents: false,
			Server:             srv,
			Origin:             b.virtualHost,
		}
		result, err := c.StateBeforeEvent(ctx, roomVer, event, eventIDs)
		if err != nil {
			lastErr = err
			continue
		}
		for eventID, ev := range result {
			b.eventIDMap[eventID] = ev
		}
		return result, nil
	}
	return nil, lastErr
}

// ServersAtEvent is called when trying to determine which server to request from.
// It returns a list of servers which can be queried for backfill requests. These servers
// will be servers that are in the room already. The entries at the beginning are preferred servers
// and will be tried first. An empty list will fail the request.
func (b *backfillRequester) ServersAtEvent(ctx context.Context, roomID, eventID string) []spec.ServerName {
	log := util.Log(ctx)
	// eventID will be a prev_event ID of a backwards extremity, meaning we will not have a database entry for it. Instead, use
	// its successor, so look it up.
	successor := ""
FindSuccessor:
	for sucID, prevEventIDs := range b.bwExtrems {
		for _, pe := range prevEventIDs {
			if pe == eventID {
				successor = sucID
				break FindSuccessor
			}
		}
	}
	if successor == "" {
		log.WithField("event_id", eventID).Error("ServersAtEvent: failed to find successor of this event to determine room state")
		return nil
	}
	eventID = successor

	// getMembershipsBeforeEventNID requires a NID, so retrieving the NID for
	// the event is necessary.
	NIDs, err := b.db.EventNIDs(ctx, []string{eventID})
	if err != nil {
		log.WithField("event_id", eventID).WithError(err).Error("ServersAtEvent: failed to get event NID for event")
		return nil
	}

	info, err := b.db.RoomInfo(ctx, roomID)
	if err != nil {
		log.WithError(err).WithField("room_id", roomID).Error("ServersAtEvent: failed to get RoomInfo for room")
		return nil
	}
	if info == nil || info.IsStub() {
		log.WithField("room_id", roomID).Error("ServersAtEvent: RoomInfo missing or stub")
		return nil
	}

	stateEntries, err := helpers.StateBeforeEvent(ctx, b.db, info, NIDs[eventID].EventNID, b.querier)
	if err != nil {
		log.WithField("event_id", eventID).WithError(err).Error("ServersAtEvent: failed to load state before event")
		return nil
	}

	// possibly return all joined servers depending on history visiblity
	memberEventsFromVis, visibility, err := joinEventsFromHistoryVisibility(ctx, b.db, b.querier, info, stateEntries, b.virtualHost)
	b.historyVisiblity = visibility
	if err != nil {
		log.WithError(err).Error("ServersAtEvent: failed calculate servers from history visibility rules")
		return nil
	}
	log.WithField("count", len(memberEventsFromVis)).WithField("visibility", visibility).Info("ServersAtEvent: current events from history visibility")

	// Retrieve all "m.room.member" state events of "join" membership, which
	// contains the list of users in the room before the event, therefore all
	// the servers in it at that moment.
	memberEvents, err := helpers.GetMembershipsAtState(ctx, b.db, info, stateEntries, true)
	if err != nil {
		log.WithField("event_id", eventID).WithError(err).Error("ServersAtEvent: failed to get memberships before event")
		return nil
	}
	memberEvents = append(memberEvents, memberEventsFromVis...)

	// Store the server names in a temporary map to avoid duplicates.
	serverSet := make(map[spec.ServerName]bool)
	for _, event := range memberEvents {
		if sender, err := b.querier.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID()); err == nil {
			serverSet[sender.Domain()] = true
		}
	}
	var servers []spec.ServerName
	for server := range serverSet {
		if b.isLocalServerName(server) {
			continue
		}
		if b.preferServer[server] { // insert at the front
			servers = append([]spec.ServerName{server}, servers...)
		} else { // insert at the back
			servers = append(servers, server)
		}
	}
	if len(servers) > maxBackfillServers {
		servers = servers[:maxBackfillServers]
	}

	b.servers = servers
	return servers
}

// Backfill performs a backfill request to the given server.
// https://matrix.org/docs/spec/server_server/latest#get-matrix-federation-v1-backfill-roomid
func (b *backfillRequester) Backfill(ctx context.Context, origin, server spec.ServerName, roomID string,
	limit int, fromEventIDs []string) (gomatrixserverlib.Transaction, error) {

	tx, err := b.fsAPI.Backfill(ctx, origin, server, roomID, limit, fromEventIDs)
	return tx, err
}

func (b *backfillRequester) ProvideEvents(ctx context.Context, roomVer gomatrixserverlib.RoomVersion, eventIDs []string) ([]gomatrixserverlib.PDU, error) {
	log := util.Log(ctx)
	nidMap, err := b.db.EventNIDs(ctx, eventIDs)
	if err != nil {
		log.WithError(err).WithField("event_ids_count", len(eventIDs)).Error("Failed to find event NIDs")
		return nil, err
	}
	eventNIDs := make([]types.EventNID, len(nidMap))
	i := 0
	for _, nid := range nidMap {
		eventNIDs[i] = nid.EventNID
		i++
	}
	eventsWithNids, err := b.db.Events(ctx, b.roomVersion, eventNIDs)
	if err != nil {
		log.WithError(err).WithField("event_nids_count", len(eventNIDs)).Error("Failed to load events from NIDs")
		return nil, err
	}
	events := make([]gomatrixserverlib.PDU, len(eventsWithNids))
	for i := range eventsWithNids {
		events[i] = eventsWithNids[i].PDU
	}
	return events, nil
}

// joinEventsFromHistoryVisibility returns all CURRENTLY joined members if our server can read the room history
//
// TODO: Long term we probably want a history_visibility table which stores eventNID | visibility_enum so we can just
// pull all events and then filter by that table.
func joinEventsFromHistoryVisibility(
	ctx context.Context, db storage.RoomDatabase, querier api.QuerySenderIDAPI, roomInfo *types.RoomInfo, stateEntries []types.StateEntry,
	thisServer spec.ServerName) ([]types.Event, gomatrixserverlib.HistoryVisibility, error) {

	log := util.Log(ctx)
	var eventNIDs []types.EventNID
	for _, entry := range stateEntries {
		// Filter the events to retrieve to only keep the membership events
		if entry.EventTypeNID == types.MRoomHistoryVisibilityNID && entry.EventStateKeyNID == types.EmptyStateKeyNID {
			eventNIDs = append(eventNIDs, entry.EventNID)
			break
		}
	}

	// Get all of the events in this state
	if roomInfo == nil {
		return nil, gomatrixserverlib.HistoryVisibilityJoined, types.ErrorInvalidRoomInfo
	}
	stateEvents, err := db.Events(ctx, roomInfo.RoomVersion, eventNIDs)
	if err != nil {
		// even though the default should be shared, restricting the visibility to joined
		// feels more secure here.
		return nil, gomatrixserverlib.HistoryVisibilityJoined, err
	}
	events := make([]gomatrixserverlib.PDU, len(stateEvents))
	for i := range stateEvents {
		events[i] = stateEvents[i].PDU
	}

	// Can we see events in the room?
	canSeeEvents := auth.IsServerAllowed(ctx, querier, thisServer, true, events)
	visibility := auth.HistoryVisibilityForRoom(events)
	if !canSeeEvents {
		log.WithField("visibility", visibility).Info("ServersAtEvent history not visible to us")
		return nil, visibility, nil
	}
	// get joined members
	joinEventNIDs, err := db.GetMembershipEventNIDsForRoom(ctx, roomInfo.RoomNID, true, false)
	if err != nil {
		return nil, visibility, err
	}
	evs, err := db.Events(ctx, roomInfo.RoomVersion, joinEventNIDs)
	return evs, visibility, err
}

func persistEvents(ctx context.Context, db storage.Database, querier api.QuerySenderIDAPI, events []gomatrixserverlib.PDU) (types.RoomNID, map[string]types.Event) {
	log := util.Log(ctx)
	var roomNID types.RoomNID
	var eventNID types.EventNID
	backfilledEventMap := make(map[string]types.Event)
	for j, ev := range events {
		nidMap, err := db.EventNIDs(ctx, ev.AuthEventIDs())
		if err != nil { // this shouldn't happen as RequestBackfill already found them
			log.WithError(err).WithField("auth_events_count", len(ev.AuthEventIDs())).Error("Failed to find one or more auth events")
			continue
		}
		authNids := make([]types.EventNID, len(nidMap))
		i := 0
		for _, nid := range nidMap {
			authNids[i] = nid.EventNID
			i++
		}

		roomInfo, err := db.GetOrCreateRoomInfo(ctx, ev)
		if err != nil {
			log.WithError(err).Error("failed to get or create roomNID")
			continue
		}
		roomNID = roomInfo.RoomNID

		eventTypeNID, err := db.GetOrCreateEventTypeNID(ctx, ev.Type())
		if err != nil {
			log.WithError(err).Error("failed to get or create eventType NID")
			continue
		}

		eventStateKeyNID, err := db.GetOrCreateEventStateKeyNID(ctx, ev.StateKey())
		if err != nil {
			log.WithError(err).Error("failed to get or create eventStateKey NID")
			continue
		}

		eventNID, _, err = db.StoreEvent(ctx, ev, roomInfo, eventTypeNID, eventStateKeyNID, authNids, false)
		if err != nil {
			log.WithError(err).WithField("event_id", ev.EventID()).Error("Failed to persist event")
			continue
		}

		resolver := state.NewStateResolution(db, roomInfo, querier)

		_, redactedEvent, err := db.MaybeRedactEvent(ctx, roomInfo, eventNID, ev, &resolver, querier)
		if err != nil {
			log.WithError(err).WithField("event_id", ev.EventID()).Error("Failed to redact event")
			continue
		}
		// If storing this event results in it being redacted, then do so.
		// It's also possible for this event to be a redaction which results in another event being
		// redacted, which we don't care about since we aren't returning it in this backfill.
		if redactedEvent != nil && redactedEvent.EventID() == ev.EventID() {
			ev = redactedEvent
			events[j] = ev
		}
		backfilledEventMap[ev.EventID()] = types.Event{
			EventNID: eventNID,
			PDU:      ev,
		}
	}
	return roomNID, backfilledEventMap
}
