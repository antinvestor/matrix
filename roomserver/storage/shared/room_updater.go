package shared

import (
	"context"
	"fmt"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/roomserver/types"
)

type RoomUpdater struct {
	d                       *Database
	roomInfo                *types.RoomInfo
	latestEvents            []types.StateAtEventAndReference
	lastEventIDSent         string
	currentStateSnapshotNID types.StateSnapshotNID
	roomExists              bool
}

func NewRoomUpdater(ctx context.Context, d *Database, roomInfo *types.RoomInfo) (*RoomUpdater, error) {
	// If the roomInfo is nil then that means that the room doesn't exist
	// yet, so we can't do `SelectLatestEventsNIDsForUpdate` because that
	// would involve locking a row on the table that doesn't exist. Instead
	// we will just run with a normal database transaction. It'll either
	// succeed, processing a create event which creates the room, or it won't.
	if roomInfo == nil {
		return &RoomUpdater{
			d, nil, nil, "", 0, false,
		}, nil
	}

	eventNIDs, lastEventNIDSent, currentStateSnapshotNID, err :=
		d.RoomsTable.SelectLatestEventsNIDsForUpdate(ctx, roomInfo.RoomNID)
	if err != nil {
		return nil, err
	}
	stateAndRefs, err := d.EventsTable.BulkSelectStateAtEventAndReference(ctx, eventNIDs)
	if err != nil {
		return nil, err
	}
	var lastEventIDSent string
	if lastEventNIDSent != 0 {
		lastEventIDSent, err = d.EventsTable.SelectEventID(ctx, lastEventNIDSent)
		if err != nil {
			return nil, err
		}
	}
	return &RoomUpdater{
		d, roomInfo, stateAndRefs, lastEventIDSent, currentStateSnapshotNID, true,
	}, nil
}

// RoomExists returns true if the room exists and false otherwise.
func (u *RoomUpdater) RoomExists() bool {
	return u.roomExists
}

// RoomVersion implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) RoomVersion() (version gomatrixserverlib.RoomVersion) {
	return u.roomInfo.RoomVersion
}

// LatestEvents implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) LatestEvents() []types.StateAtEventAndReference {
	return u.latestEvents
}

// LastEventIDSent implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) LastEventIDSent() string {
	return u.lastEventIDSent
}

// CurrentStateSnapshotNID implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) CurrentStateSnapshotNID() types.StateSnapshotNID {
	return u.currentStateSnapshotNID
}

func (u *RoomUpdater) Events(ctx context.Context, _ gomatrixserverlib.RoomVersion, eventNIDs []types.EventNID) ([]types.Event, error) {
	if u.roomInfo == nil {
		return nil, types.ErrorInvalidRoomInfo
	}
	return u.d.events(ctx, u.roomInfo.RoomVersion, eventNIDs)
}

func (u *RoomUpdater) SnapshotNIDFromEventID(
	ctx context.Context, eventID string,
) (types.StateSnapshotNID, error) {
	return u.d.snapshotNIDFromEventID(ctx, eventID)
}

func (u *RoomUpdater) StateBlockNIDs(
	ctx context.Context, stateNIDs []types.StateSnapshotNID,
) ([]types.StateBlockNIDList, error) {
	return u.d.stateBlockNIDs(ctx, stateNIDs)
}

func (u *RoomUpdater) StateEntries(
	ctx context.Context, stateBlockNIDs []types.StateBlockNID,
) ([]types.StateEntryList, error) {
	return u.d.stateEntries(ctx, stateBlockNIDs)
}

func (u *RoomUpdater) StateEntriesForTuples(
	ctx context.Context,
	stateBlockNIDs []types.StateBlockNID,
	stateKeyTuples []types.StateKeyTuple,
) ([]types.StateEntryList, error) {
	return u.d.stateEntriesForTuples(ctx, stateBlockNIDs, stateKeyTuples)
}

func (u *RoomUpdater) AddState(
	ctx context.Context,
	roomNID types.RoomNID,
	stateBlockNIDs []types.StateBlockNID,
	state []types.StateEntry,
) (stateNID types.StateSnapshotNID, err error) {
	return u.d.addState(ctx, roomNID, stateBlockNIDs, state)
}

func (u *RoomUpdater) SetState(
	ctx context.Context, eventNID types.EventNID, stateNID types.StateSnapshotNID,
) error {
	return u.d.EventsTable.UpdateEventState(ctx, eventNID, stateNID)
}

func (u *RoomUpdater) EventTypeNIDs(
	ctx context.Context, eventTypes []string,
) (map[string]types.EventTypeNID, error) {
	return u.d.eventTypeNIDs(ctx, eventTypes)
}

func (u *RoomUpdater) EventStateKeyNIDs(
	ctx context.Context, eventStateKeys []string,
) (map[string]types.EventStateKeyNID, error) {
	return u.d.eventStateKeyNIDs(ctx, eventStateKeys)
}

func (u *RoomUpdater) RoomInfo(ctx context.Context, roomID string) (*types.RoomInfo, error) {
	return u.d.roomInfo(ctx, roomID)
}

func (u *RoomUpdater) EventIDs(
	ctx context.Context, eventNIDs []types.EventNID,
) (map[types.EventNID]string, error) {
	return u.d.EventsTable.BulkSelectEventID(ctx, eventNIDs)
}

func (u *RoomUpdater) BulkSelectSnapshotsFromEventIDs(ctx context.Context, eventIDs []string) (map[types.StateSnapshotNID][]string, error) {
	return u.d.EventsTable.BulkSelectSnapshotsFromEventIDs(ctx, eventIDs)
}

func (u *RoomUpdater) StateAtEventIDs(
	ctx context.Context, eventIDs []string,
) ([]types.StateAtEvent, error) {
	return u.d.EventsTable.BulkSelectStateAtEventByID(ctx, eventIDs)
}

func (u *RoomUpdater) EventsFromIDs(ctx context.Context, roomInfo *types.RoomInfo, eventIDs []string) ([]types.Event, error) {
	return u.d.eventsFromIDs(ctx, u.roomInfo, eventIDs, NoFilter)
}

// IsReferenced implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) IsReferenced(ctx context.Context, eventID string) (bool, error) {
	err := u.d.PrevEventsTable.SelectPreviousEventExists(ctx, eventID)
	if err == nil {
		return true, nil
	}
	if sqlutil.ErrorIsNoRows(err) {
		return false, nil
	}
	return false, fmt.Errorf("u.d.PrevEventsTable.SelectPreviousEventExists: %w", err)
}

// SetLatestEvents implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) SetLatestEvents(ctx context.Context,
	roomNID types.RoomNID, latest []types.StateAtEventAndReference,
	lastEventNIDSent types.EventNID, currentStateSnapshotNID types.StateSnapshotNID,
) error {
	switch {
	case len(latest) == 0:
		return fmt.Errorf("cannot set latest events with no latest event references")
	case currentStateSnapshotNID == 0:
		return fmt.Errorf("cannot set latest events with invalid state snapshot NID")
	case lastEventNIDSent == 0:
		return fmt.Errorf("cannot set latest events with invalid latest event NID")
	}
	eventNIDs := make([]types.EventNID, len(latest))
	for i := range latest {
		eventNIDs[i] = latest[i].EventNID
	}

	// Use the existing transaction instead of creating a new one with u.d.Cm.Do
	if err := u.d.RoomsTable.UpdateLatestEventNIDs(ctx, roomNID, eventNIDs, lastEventNIDSent, currentStateSnapshotNID); err != nil {
		return fmt.Errorf("u.d.RoomsTable.updateLatestEventNIDs: %w", err)
	}

	// Since it's entirely possible that this types.RoomInfo came from the
	// cache, we should make sure to update that entry so that the next run
	// works from live data.
	if u.roomInfo != nil {
		u.roomInfo.SetStateSnapshotNID(currentStateSnapshotNID)
		u.roomInfo.SetIsStub(false)
	}
	return nil
}

// HasEventBeenSent implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) HasEventBeenSent(ctx context.Context, eventNID types.EventNID) (bool, error) {
	return u.d.EventsTable.SelectEventSentToOutput(ctx, eventNID)
}

// MarkEventAsSent implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) MarkEventAsSent(ctx context.Context, eventNID types.EventNID) error {
	return u.d.Cm.Do(ctx, func(ctx context.Context) error {
		return u.d.EventsTable.UpdateEventSentToOutput(ctx, eventNID)
	})
}

func (u *RoomUpdater) MembershipUpdater(ctx context.Context, targetUserNID types.EventStateKeyNID, targetLocal bool) (context.Context, *MembershipUpdater, error) {
	return u.d.membershipUpdaterTxn(ctx, u.roomInfo.RoomNID, targetUserNID, targetLocal)
}

func (u *RoomUpdater) IsEventRejected(ctx context.Context, roomNID types.RoomNID, eventID string) (bool, error) {
	return u.d.IsEventRejected(ctx, roomNID, eventID)
}
