package helpers

import (
	"context"
	"fmt"
	"sort"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/auth"
	"github.com/antinvestor/matrix/roomserver/state"
	"github.com/antinvestor/matrix/roomserver/storage"
	"github.com/antinvestor/matrix/roomserver/storage/shared"
	"github.com/antinvestor/matrix/roomserver/storage/tables"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/pitabwire/util"
)

// TODO: temporary package which has helper functions used by both internal/perform packages.
// Move these to a more sensible place.

func UpdateToInviteMembership(ctx context.Context,
	mu *shared.MembershipUpdater, add *types.Event, updates []api.OutputEvent,
	roomVersion gomatrixserverlib.RoomVersion,
) ([]api.OutputEvent, error) {
	// We may have already sent the invite to the user, either because we are
	// reprocessing this event, or because the we received this invite from a
	// remote server via the federation invite API. In those cases we don't need
	// to send the event.
	needsSending, retired, err := mu.Update(ctx, tables.MembershipStateInvite, add)
	if err != nil {
		return nil, err
	}
	if needsSending {
		// We notify the consumers using a special event even though we will
		// notify them about the change in current state as part of the normal
		// room event stream. This ensures that the consumers only have to
		// consider a single stream of events when determining whether a user
		// is invited, rather than having to combine multiple streams themselves.
		updates = append(updates, api.OutputEvent{
			Type: api.OutputTypeNewInviteEvent,
			NewInviteEvent: &api.OutputNewInviteEvent{
				Event:       &types.HeaderedEvent{PDU: add.PDU},
				RoomVersion: roomVersion,
			},
		})
	}
	for _, eventID := range retired {
		updates = append(updates, api.OutputEvent{
			Type: api.OutputTypeRetireInviteEvent,
			RetireInviteEvent: &api.OutputRetireInviteEvent{
				EventID:          eventID,
				RoomID:           add.RoomID().String(),
				Membership:       spec.Join,
				RetiredByEventID: add.EventID(),
				TargetSenderID:   spec.SenderID(*add.StateKey()),
			},
		})
	}
	return updates, nil
}

// IsServerCurrentlyInRoom checks if a server is in a given room, based on the room
// memberships. If the servername is not supplied then the local server will be
// checked instead using a faster code path.
// TODO: This should probably be replaced by an API call.
func IsServerCurrentlyInRoom(ctx context.Context, db storage.Database, querier api.QuerySenderIDAPI, serverName spec.ServerName, roomID string) (bool, error) {
	info, err := db.RoomInfo(ctx, roomID)
	if err != nil {
		return false, err
	}
	if info == nil {
		return false, fmt.Errorf("unknown room %s", roomID)
	}

	if serverName == "" {
		return db.GetLocalServerInRoom(ctx, info.RoomNID)
	}

	eventNIDs, err := db.GetMembershipEventNIDsForRoom(ctx, info.RoomNID, true, false)
	if err != nil {
		return false, err
	}

	events, err := db.Events(ctx, info.RoomVersion, eventNIDs)
	if err != nil {
		return false, err
	}
	gmslEvents := make([]gomatrixserverlib.PDU, len(events))
	for i := range events {
		gmslEvents[i] = events[i].PDU
	}
	return auth.IsAnyUserOnServerWithMembership(ctx, querier, serverName, gmslEvents, spec.Join), nil
}

func IsInvitePending(
	ctx context.Context, db storage.Database,
	roomID string, senderID spec.SenderID,
) (bool, spec.SenderID, string, gomatrixserverlib.PDU, error) {
	// Look up the room NID for the supplied room ID.
	info, err := db.RoomInfo(ctx, roomID)
	if err != nil {
		return false, "", "", nil, fmt.Errorf("r.Cm.RoomInfo: %w", err)
	}
	if info == nil {
		return false, "", "", nil, fmt.Errorf("cannot get RoomInfo: unknown room ID %s", roomID)
	}

	// Look up the state key NID for the supplied user ID.
	targetUserNIDs, err := db.EventStateKeyNIDs(ctx, []string{string(senderID)})
	if err != nil {
		return false, "", "", nil, fmt.Errorf("r.Cm.EventStateKeyNIDs: %w", err)
	}
	targetUserNID, targetUserFound := targetUserNIDs[string(senderID)]
	if !targetUserFound {
		return false, "", "", nil, fmt.Errorf("missing NID for user %q (%+v)", senderID, targetUserNIDs)
	}

	// Let's see if we have an event active for the user in the room. If
	// we do then it will contain a server name that we can direct the
	// send_leave to.
	senderUserNIDs, eventIDs, eventJSON, err := db.GetInvitesForUser(ctx, info.RoomNID, targetUserNID)
	if err != nil {
		return false, "", "", nil, fmt.Errorf("r.Cm.GetInvitesForUser: %w", err)
	}
	if len(senderUserNIDs) == 0 {
		return false, "", "", nil, nil
	}
	userNIDToEventID := make(map[types.EventStateKeyNID]string)
	for i, nid := range senderUserNIDs {
		userNIDToEventID[nid] = eventIDs[i]
	}

	// Look up the user ID from the NID.
	senderUsers, err := db.EventStateKeys(ctx, senderUserNIDs)
	if err != nil {
		return false, "", "", nil, fmt.Errorf("r.Cm.EventStateKeys: %w", err)
	}
	if len(senderUsers) == 0 {
		return false, "", "", nil, fmt.Errorf("no senderUsers")
	}

	senderUser, senderUserFound := senderUsers[senderUserNIDs[0]]
	if !senderUserFound {
		return false, "", "", nil, fmt.Errorf("missing user for NID %d (%+v)", senderUserNIDs[0], senderUsers)
	}

	verImpl, err := gomatrixserverlib.GetRoomVersion(info.RoomVersion)
	if err != nil {
		return false, "", "", nil, err
	}

	event, err := verImpl.NewEventFromTrustedJSON(eventJSON, false)

	return true, spec.SenderID(senderUser), userNIDToEventID[senderUserNIDs[0]], event, err
}

// GetMembershipsAtState filters the state events to
// only keep the "m.room.member" events with a "join" membership. These events are returned.
// Returns an error if there was an issue fetching the events.
func GetMembershipsAtState(
	ctx context.Context, db storage.RoomDatabase, roomInfo *types.RoomInfo, stateEntries []types.StateEntry, joinedOnly bool,
) ([]types.Event, error) {

	var eventNIDs types.EventNIDs
	for _, entry := range stateEntries {
		// Filter the events to retrieve to only keep the membership events
		if entry.EventTypeNID == types.MRoomMemberNID {
			eventNIDs = append(eventNIDs, entry.EventNID)
		}
	}

	// There are no events to get, don't bother asking the database
	if len(eventNIDs) == 0 {
		return []types.Event{}, nil
	}

	sort.Sort(eventNIDs)
	util.Unique(eventNIDs)

	// Get all of the events in this state
	if roomInfo == nil {
		return nil, types.ErrorInvalidRoomInfo
	}
	stateEvents, err := db.Events(ctx, roomInfo.RoomVersion, eventNIDs)
	if err != nil {
		return nil, err
	}

	if !joinedOnly {
		return stateEvents, nil
	}

	// Filter the events to only keep the "join" membership events
	var events []types.Event
	for _, event := range stateEvents {
		membership, err := event.Membership()
		if err != nil {
			return nil, err
		}

		if membership == spec.Join {
			events = append(events, event)
		}
	}

	return events, nil
}

func StateBeforeEvent(ctx context.Context, db storage.Database, info *types.RoomInfo, eventNID types.EventNID, querier api.QuerySenderIDAPI) ([]types.StateEntry, error) {
	roomState := state.NewStateResolution(db, info, querier)
	// Lookup the event NID
	eIDs, err := db.EventIDs(ctx, []types.EventNID{eventNID})
	if err != nil {
		return nil, err
	}
	eventIDs := []string{eIDs[eventNID]}

	prevState, err := db.StateAtEventIDs(ctx, eventIDs)
	if err != nil {
		return nil, err
	}

	// Fetch the state as it was when this event was fired
	return roomState.LoadCombinedStateAfterEvents(ctx, prevState)
}

func MembershipAtEvent(ctx context.Context, db storage.RoomDatabase, info *types.RoomInfo, eventIDs []string, stateKeyNID types.EventStateKeyNID, querier api.QuerySenderIDAPI) (map[string][]types.StateEntry, error) {
	roomState := state.NewStateResolution(db, info, querier)
	// Fetch the state as it was when this event was fired
	return roomState.LoadMembershipAtEvent(ctx, eventIDs, stateKeyNID)
}

func LoadEvents(
	ctx context.Context, db storage.RoomDatabase, roomInfo *types.RoomInfo, eventNIDs []types.EventNID,
) ([]gomatrixserverlib.PDU, error) {
	if roomInfo == nil {
		return nil, types.ErrorInvalidRoomInfo
	}
	stateEvents, err := db.Events(ctx, roomInfo.RoomVersion, eventNIDs)
	if err != nil {
		return nil, err
	}

	result := make([]gomatrixserverlib.PDU, len(stateEvents))
	for i := range stateEvents {
		result[i] = stateEvents[i].PDU
	}
	return result, nil
}

func LoadStateEvents(
	ctx context.Context, db storage.RoomDatabase, roomInfo *types.RoomInfo, stateEntries []types.StateEntry,
) ([]gomatrixserverlib.PDU, error) {
	eventNIDs := make([]types.EventNID, len(stateEntries))
	for i := range stateEntries {
		eventNIDs[i] = stateEntries[i].EventNID
	}
	return LoadEvents(ctx, db, roomInfo, eventNIDs)
}

func CheckServerAllowedToSeeEvent(
	ctx context.Context, db storage.Database, info *types.RoomInfo, roomID string, eventID string, serverName spec.ServerName, isServerInRoom bool, querier api.QuerySenderIDAPI,
) (bool, error) {
	stateAtEvent, err := db.GetHistoryVisibilityState(ctx, info, eventID, string(serverName))
	switch err {
	case nil:
		// No error, so continue normally
	case tables.ErrOptimisationNotSupported:
		// The database engine didn't support this optimisation, so fall back to using
		// the old and slow method
		stateAtEvent, err = slowGetHistoryVisibilityState(ctx, db, info, roomID, eventID, serverName, querier)
		if err != nil {
			return false, err
		}
	default:
		switch err.(type) {
		case types.MissingStateError:
			// If there's no state then we assume it's open visibility, as Synapse does:
			// https://github.com/matrix-org/synapse/blob/aec87a0f9369a3015b2a53469f88d1de274e8b71/synapse/visibility.py#L654-L655
			return true, nil
		default:
			// Something else went wrong
			return false, err
		}
	}
	return auth.IsServerAllowed(ctx, querier, serverName, isServerInRoom, stateAtEvent), nil
}

func slowGetHistoryVisibilityState(
	ctx context.Context, db storage.Database, info *types.RoomInfo, roomID, eventID string, serverName spec.ServerName, querier api.QuerySenderIDAPI,
) ([]gomatrixserverlib.PDU, error) {
	roomState := state.NewStateResolution(db, info, querier)
	stateEntries, err := roomState.LoadStateAtEvent(ctx, eventID)
	if err != nil {
		if sqlutil.ErrorIsNoRows(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("roomState.LoadStateAtEvent: %w", err)
	}

	// Extract all of the event state key NIDs from the room state.
	var stateKeyNIDs []types.EventStateKeyNID
	for _, entry := range stateEntries {
		stateKeyNIDs = append(stateKeyNIDs, entry.EventStateKeyNID)
	}

	// Then request those state key NIDs from the database.
	stateKeys, err := db.EventStateKeys(ctx, stateKeyNIDs)
	if err != nil {
		return nil, fmt.Errorf("db.EventStateKeys: %w", err)
	}

	// If the event state key doesn't match the given servername
	// then we'll filter it out. This does preserve state keys that
	// are "" since these will contain history visibility etc.
	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return nil, err
	}
	for nid, key := range stateKeys {
		if key != "" {
			userID, err := querier.QueryUserIDForSender(ctx, *validRoomID, spec.SenderID(key))
			if err == nil && userID != nil {
				if userID.Domain() != serverName {
					delete(stateKeys, nid)
				}
			}
		}
	}

	// Now filter through all of the state events for the room.
	// If the state key NID appears in the list of valid state
	// keys then we'll add it to the list of filtered entries.
	var filteredEntries []types.StateEntry
	for _, entry := range stateEntries {
		if _, ok := stateKeys[entry.EventStateKeyNID]; ok {
			filteredEntries = append(filteredEntries, entry)
		}
	}

	if len(filteredEntries) == 0 {
		return nil, nil
	}

	return LoadStateEvents(ctx, db, info, filteredEntries)
}

// TODO: Remove this when we have tests to assert correctness of this function
func ScanEventTree(
	ctx context.Context, db storage.Database, info *types.RoomInfo, front []string, visited map[string]bool, limit int,
	serverName spec.ServerName, querier api.QuerySenderIDAPI,
) ([]types.EventNID, map[string]struct{}, error) {
	var resultNIDs []types.EventNID
	var err error
	var allowed bool
	var events []types.Event
	var next []string
	var pre string

	// TODO: add tests for this function to ensure it meets the contract that callers expect (and doc what that is supposed to be)
	// Currently, callers like PerformBackfill will call scanEventTree with a pre-populated `visited` map, assuming that by doing
	// so means that the events in that map will NOT be returned from this function. That is not currently true, resulting in
	// duplicate events being sent in response to /backfill requests.
	initialIgnoreList := make(map[string]bool, len(visited))
	for k, v := range visited {
		initialIgnoreList[k] = v
	}

	resultNIDs = make([]types.EventNID, 0, limit)

	var checkedServerInRoom bool
	var isServerInRoom bool
	redactEventIDs := make(map[string]struct{})

	// Loop through the event IDs to retrieve the requested events and go
	// through the whole tree (up to the provided limit) using the events'
	// "prev_event" key.
BFSLoop:
	for len(front) > 0 {
		// Prevent unnecessary allocations: reset the slice only when not empty.
		if len(next) > 0 {
			next = make([]string, 0)
		}
		// Retrieve the events to process from the database.
		events, err = db.EventsFromIDs(ctx, info, front)
		if err != nil {
			return resultNIDs, redactEventIDs, err
		}

		if !checkedServerInRoom && len(events) > 0 {
			// It's nasty that we have to extract the room ID from an event, but many federation requests
			// only talk in event IDs, no room IDs at all (!!!)
			ev := events[0]
			isServerInRoom, err = IsServerCurrentlyInRoom(ctx, db, querier, serverName, ev.RoomID().String())
			if err != nil {
				util.Log(ctx).WithError(err).WithField("server", serverName).WithField("room_id", ev.RoomID().String()).Error("Failed to check if server is currently in room, assuming not.")
			}
			checkedServerInRoom = true
		}

		for _, ev := range events {
			// Break out of the loop if the provided limit is reached.
			if len(resultNIDs) == limit {
				break BFSLoop
			}

			if !initialIgnoreList[ev.EventID()] {
				// Update the list of events to retrieve.
				resultNIDs = append(resultNIDs, ev.EventNID)
			}
			// Loop through the event's parents.
			for _, pre = range ev.PrevEventIDs() {
				// Only add an event to the list of next events to process if it
				// hasn't been seen before.
				if !visited[pre] {
					visited[pre] = true
					allowed, err = CheckServerAllowedToSeeEvent(ctx, db, info, ev.RoomID().String(), pre, serverName, isServerInRoom, querier)
					if err != nil {
						util.Log(ctx).WithError(err).WithField("server", serverName).WithField("event_id", pre).Error("Error checking if allowed to see event")
						// drop the error, as we will often error at the Cm level if we don't have the prev_event itself. Let's
						// just return what we have.
						return resultNIDs, redactEventIDs, nil
					}

					// If the event hasn't been seen before and the HS
					// requesting to retrieve it is allowed to do so, add it to
					// the list of events to retrieve.
					next = append(next, pre)
					if !allowed {
						util.Log(ctx).WithField("server", serverName).WithField("event_id", pre).Info("Not allowed to see event")
						redactEventIDs[pre] = struct{}{}
					}
				}
			}
		}
		// Repeat the same process with the parent events we just processed.
		front = next
	}

	return resultNIDs, redactEventIDs, err
}

func QueryLatestEventsAndState(
	ctx context.Context, db storage.Database, querier api.QuerySenderIDAPI,
	request *api.QueryLatestEventsAndStateRequest,
	response *api.QueryLatestEventsAndStateResponse,
) error {
	roomInfo, err := db.RoomInfo(ctx, request.RoomID)
	if err != nil {
		return err
	}
	if roomInfo == nil || roomInfo.IsStub() {
		response.RoomExists = false
		return nil
	}

	roomState := state.NewStateResolution(db, roomInfo, querier)
	response.RoomExists = true
	response.RoomVersion = roomInfo.RoomVersion

	var currentStateSnapshotNID types.StateSnapshotNID
	response.LatestEvents, currentStateSnapshotNID, response.Depth, err =
		db.LatestEventIDs(ctx, roomInfo.RoomNID)
	if err != nil {
		return err
	}

	var stateEntries []types.StateEntry
	if len(request.StateToFetch) == 0 {
		// Look up all room state.
		stateEntries, err = roomState.LoadStateAtSnapshot(
			ctx, currentStateSnapshotNID,
		)
	} else {
		// Look up the current state for the requested tuples.
		stateEntries, err = roomState.LoadStateAtSnapshotForStringTuples(
			ctx, currentStateSnapshotNID, request.StateToFetch,
		)
	}
	if err != nil {
		return err
	}

	stateEvents, err := LoadStateEvents(ctx, db, roomInfo, stateEntries)
	if err != nil {
		return err
	}

	for _, event := range stateEvents {
		response.StateEvents = append(response.StateEvents, &types.HeaderedEvent{PDU: event})
	}

	return nil
}
