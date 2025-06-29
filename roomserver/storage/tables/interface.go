package tables

import (
	"context"
	"crypto/ed25519"
	"errors"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/roomserver/types"
	"github.com/tidwall/gjson"
)

var ErrOptimisationNotSupported = errors.New("optimisation not supported")

type EventJSONPair struct {
	EventNID  types.EventNID
	EventJSON []byte
}

type EventJSON interface {
	// Insert the event JSON. On conflict, replace the event JSON with the new value (for redactions).
	InsertEventJSON(ctx context.Context, eventNID types.EventNID, eventJSON []byte) error
	BulkSelectEventJSON(ctx context.Context, eventNIDs []types.EventNID) ([]EventJSONPair, error)
}

type EventTypes interface {
	InsertEventTypeNID(ctx context.Context, eventType string) (types.EventTypeNID, error)
	SelectEventTypeNID(ctx context.Context, eventType string) (types.EventTypeNID, error)
	BulkSelectEventTypeNID(ctx context.Context, eventTypes []string) (map[string]types.EventTypeNID, error)
}

type EventStateKeys interface {
	InsertEventStateKeyNID(ctx context.Context, eventStateKey string) (types.EventStateKeyNID, error)
	SelectEventStateKeyNID(ctx context.Context, eventStateKey string) (types.EventStateKeyNID, error)
	BulkSelectEventStateKeyNID(ctx context.Context, eventStateKeys []string) (map[string]types.EventStateKeyNID, error)
	BulkSelectEventStateKey(ctx context.Context, eventStateKeyNIDs []types.EventStateKeyNID) (map[types.EventStateKeyNID]string, error)
}

type Events interface {
	InsertEvent(
		ctx context.Context, roomNID types.RoomNID, eventTypeNID types.EventTypeNID,
		eventStateKeyNID types.EventStateKeyNID, eventID string,
		authEventNIDs []types.EventNID, depth int64, isRejected bool,
	) (types.EventNID, types.StateSnapshotNID, error)
	SelectEvent(ctx context.Context, eventID string) (types.EventNID, types.StateSnapshotNID, error)
	BulkSelectSnapshotsFromEventIDs(ctx context.Context, eventIDs []string) (map[types.StateSnapshotNID][]string, error)
	// BulkSelectStateEventByID lookups a list of state events by event ID.
	// If any of the requested events are missing from the database it returns a types.MissingEventError
	BulkSelectStateEventByID(ctx context.Context, eventIDs []string, excludeRejected bool) ([]types.StateEntry, error)
	BulkSelectStateEventByNID(ctx context.Context, eventNIDs []types.EventNID, stateKeyTuples []types.StateKeyTuple) ([]types.StateEntry, error)
	// BulkSelectStateAtEventByID lookups the state at a list of events by event ID.
	// If any of the requested events are missing from the database it returns a types.MissingEventError.
	// If we do not have the state for any of the requested events it returns a types.MissingEventError.
	BulkSelectStateAtEventByID(ctx context.Context, eventIDs []string) ([]types.StateAtEvent, error)
	UpdateEventState(ctx context.Context, eventNID types.EventNID, stateNID types.StateSnapshotNID) error
	SelectEventSentToOutput(ctx context.Context, eventNID types.EventNID) (sentToOutput bool, err error)
	UpdateEventSentToOutput(ctx context.Context, eventNID types.EventNID) error
	SelectEventID(ctx context.Context, eventNID types.EventNID) (eventID string, err error)
	BulkSelectStateAtEventAndReference(ctx context.Context, eventNIDs []types.EventNID) ([]types.StateAtEventAndReference, error)
	// BulkSelectEventID returns a map from numeric event ID to string event ID.
	BulkSelectEventID(ctx context.Context, eventNIDs []types.EventNID) (map[types.EventNID]string, error)
	// BulkSelectEventNIDs returns a map from string event ID to numeric event ID.
	// If an event ID is not in the database then it is omitted from the map.
	BulkSelectEventNID(ctx context.Context, eventIDs []string) (map[string]types.EventMetadata, error)
	BulkSelectUnsentEventNID(ctx context.Context, eventIDs []string) (map[string]types.EventMetadata, error)
	SelectMaxEventDepth(ctx context.Context, eventNIDs []types.EventNID) (int64, error)
	SelectRoomNIDsForEventNIDs(ctx context.Context, eventNIDs []types.EventNID) (roomNIDs map[types.EventNID]types.RoomNID, err error)
	SelectEventRejected(ctx context.Context, roomNID types.RoomNID, eventID string) (rejected bool, err error)

	SelectRoomsWithEventTypeNID(ctx context.Context, eventTypeNID types.EventTypeNID) ([]types.RoomNID, error)
}

type Rooms interface {
	InsertRoomNID(ctx context.Context, roomID string, roomVersion gomatrixserverlib.RoomVersion) (types.RoomNID, error)
	SelectRoomNID(ctx context.Context, roomID string) (types.RoomNID, error)
	SelectRoomNIDForUpdate(ctx context.Context, roomID string) (types.RoomNID, error)
	SelectLatestEventNIDs(ctx context.Context, roomNID types.RoomNID) ([]types.EventNID, types.StateSnapshotNID, error)
	SelectLatestEventsNIDsForUpdate(ctx context.Context, roomNID types.RoomNID) ([]types.EventNID, types.EventNID, types.StateSnapshotNID, error)
	UpdateLatestEventNIDs(ctx context.Context, roomNID types.RoomNID, eventNIDs []types.EventNID, lastEventSentNID types.EventNID, stateSnapshotNID types.StateSnapshotNID) error
	SelectRoomVersionsForRoomNIDs(ctx context.Context, roomNID []types.RoomNID) (map[types.RoomNID]gomatrixserverlib.RoomVersion, error)
	SelectRoomInfo(ctx context.Context, roomID string) (*types.RoomInfo, error)
	BulkSelectRoomIDs(ctx context.Context, roomNIDs []types.RoomNID) ([]string, error)
	BulkSelectRoomNIDs(ctx context.Context, roomIDs []string) ([]types.RoomNID, error)
}

type StateSnapshot interface {
	InsertState(ctx context.Context, roomNID types.RoomNID, stateBlockNIDs types.StateBlockNIDs) (stateNID types.StateSnapshotNID, err error)
	BulkSelectStateBlockNIDs(ctx context.Context, stateNIDs []types.StateSnapshotNID) ([]types.StateBlockNIDList, error)
	// BulkSelectStateForHistoryVisibility is a PostgreSQL-only optimisation for finding
	// which users are in a room faster than having to load the entire room state. In the
	// case of SQLite, this will return tables.ErrOptimisationNotSupported.
	BulkSelectStateForHistoryVisibility(ctx context.Context, stateSnapshotNID types.StateSnapshotNID, domain string) ([]types.EventNID, error)

	BulkSelectMembershipForHistoryVisibility(
		ctx context.Context, userNID types.EventStateKeyNID, roomInfo *types.RoomInfo, eventIDs ...string,
	) (map[string]*types.HeaderedEvent, error)
}

type StateBlock interface {
	BulkInsertStateData(ctx context.Context, entries types.StateEntries) (types.StateBlockNID, error)
	BulkSelectStateBlockEntries(ctx context.Context, stateBlockNIDs types.StateBlockNIDs) ([][]types.EventNID, error)
	// BulkSelectFilteredStateBlockEntries(ctx context.Context, stateBlockNIDs []types.StateBlockNID, stateKeyTuples []types.StateKeyTuple) ([]types.StateEntryList, error)
}

type RoomAliases interface {
	InsertRoomAlias(ctx context.Context, alias string, roomID string, creatorUserID string) (err error)
	SelectRoomIDFromAlias(ctx context.Context, alias string) (roomID string, err error)
	SelectAliasesFromRoomID(ctx context.Context, roomID string) ([]string, error)
	SelectCreatorIDFromAlias(ctx context.Context, alias string) (creatorID string, err error)
	DeleteRoomAlias(ctx context.Context, alias string) (err error)
}

type PreviousEvents interface {
	InsertPreviousEvent(ctx context.Context, previousEventID string, eventNID types.EventNID) error
	// Check if the event reference exists
	// Returns sql.ErrNoRows if the event reference doesn't exist.
	SelectPreviousEventExists(ctx context.Context, eventID string) error
}

type Invites interface {
	InsertInviteEvent(ctx context.Context, inviteEventID string, roomNID types.RoomNID, targetUserNID, senderUserNID types.EventStateKeyNID, inviteEventJSON []byte) (bool, error)
	UpdateInviteRetired(ctx context.Context, roomNID types.RoomNID, targetUserNID types.EventStateKeyNID) ([]string, error)
	// SelectInviteActiveForUserInRoom returns a list of sender state key NIDs and invite event IDs matching those nids.
	SelectInviteActiveForUserInRoom(ctx context.Context, targetUserNID types.EventStateKeyNID, roomNID types.RoomNID) ([]types.EventStateKeyNID, []string, []byte, error)
}

type ReportedEvents interface {
	InsertReportedEvent(
		ctx context.Context,

		roomNID types.RoomNID,
		eventNID types.EventNID,
		reportingUserID types.EventStateKeyNID,
		eventSenderID types.EventStateKeyNID,
		reason string,
		score int64,
	) (int64, error)
	SelectReportedEvents(
		ctx context.Context,

		from, limit uint64,
		backwards bool,
		reportingUserID types.EventStateKeyNID,
		roomNID types.RoomNID,
	) ([]api.QueryAdminEventReportsResponse, int64, error)
	SelectReportedEvent(
		ctx context.Context,

		reportID uint64,
	) (api.QueryAdminEventReportResponse, error)
	DeleteReportedEvent(ctx context.Context, reportID uint64) error
}

type MembershipState int64

const (
	MembershipStateLeaveOrBan MembershipState = 1
	MembershipStateInvite     MembershipState = 2
	MembershipStateJoin       MembershipState = 3
	MembershipStateKnock      MembershipState = 4
)

type Membership interface {
	InsertMembership(ctx context.Context, roomNID types.RoomNID, targetUserNID types.EventStateKeyNID, localTarget bool) error
	SelectMembershipForUpdate(ctx context.Context, roomNID types.RoomNID, targetUserNID types.EventStateKeyNID) (MembershipState, error)
	SelectMembershipFromRoomAndTarget(ctx context.Context, roomNID types.RoomNID, targetUserNID types.EventStateKeyNID) (types.EventNID, MembershipState, bool, error)
	SelectMembershipsFromRoom(ctx context.Context, roomNID types.RoomNID, localOnly bool) (eventNIDs []types.EventNID, err error)
	SelectMembershipsFromRoomAndMembership(ctx context.Context, roomNID types.RoomNID, membership MembershipState, localOnly bool) (eventNIDs []types.EventNID, err error)
	UpdateMembership(ctx context.Context, roomNID types.RoomNID, targetUserNID types.EventStateKeyNID, senderUserNID types.EventStateKeyNID, membership MembershipState, eventNID types.EventNID, forgotten bool) (bool, error)
	SelectRoomsWithMembership(ctx context.Context, userID types.EventStateKeyNID, membershipState MembershipState) ([]types.RoomNID, error)
	// SelectJoinedUsersSetForRooms returns how many times each of the given users appears across the given rooms.
	SelectJoinedUsersSetForRooms(ctx context.Context, roomNIDs []types.RoomNID, userNIDs []types.EventStateKeyNID, localOnly bool) (map[types.EventStateKeyNID]int, error)
	SelectKnownUsers(ctx context.Context, userID types.EventStateKeyNID, searchString string, limit int) ([]string, error)
	UpdateForgetMembership(ctx context.Context, roomNID types.RoomNID, targetUserNID types.EventStateKeyNID, forget bool) error
	SelectLocalServerInRoom(ctx context.Context, roomNID types.RoomNID) (bool, error)
	SelectServerInRoom(ctx context.Context, roomNID types.RoomNID, serverName spec.ServerName) (bool, error)
	DeleteMembership(ctx context.Context, roomNID types.RoomNID, targetUserNID types.EventStateKeyNID) error
	SelectJoinedUsers(ctx context.Context, targetUserNIDs []types.EventStateKeyNID) ([]types.EventStateKeyNID, error)
}

type Published interface {
	UpsertRoomPublished(ctx context.Context, roomID, appserviceID, networkID string, published bool) (err error)
	SelectPublishedFromRoomID(ctx context.Context, roomID string) (published bool, err error)
	SelectAllPublishedRooms(ctx context.Context, networkdID string, published, includeAllNetworks bool) ([]string, error)
}

type RedactionInfo struct {
	// whether this redaction is validated (we have both events)
	Validated bool
	// the ID of the event being redacted
	RedactsEventID string
	// the ID of the redaction event
	RedactionEventID string
}

type Redactions interface {
	InsertRedaction(ctx context.Context, info RedactionInfo) error
	// SelectRedactionInfoByRedactionEventID returns the redaction info for the given redaction event ID, or nil if there is no match.
	SelectRedactionInfoByRedactionEventID(ctx context.Context, redactionEventID string) (*RedactionInfo, error)
	// SelectRedactionInfoByEventBeingRedacted returns the redaction info for the given redacted event ID, or nil if there is no match.
	SelectRedactionInfoByEventBeingRedacted(ctx context.Context, eventID string) (*RedactionInfo, error)
	// Mark this redaction event as having been validated. This means we have both sides of the redaction and have
	// successfully redacted the event JSON.
	MarkRedactionValidated(ctx context.Context, redactionEventID string, validated bool) error
}

type Purge interface {
	PurgeRoom(
		ctx context.Context, roomNID types.RoomNID, roomID string,
	) error
}

type UserRoomKeys interface {
	// InsertUserRoomPrivatePublicKey inserts the given private key as well as the public key for it. This should be used
	// when creating keys locally.
	InsertUserRoomPrivatePublicKey(ctx context.Context, userNID types.EventStateKeyNID, roomNID types.RoomNID, key ed25519.PrivateKey) (ed25519.PrivateKey, error)
	// InsertUserRoomPublicKey inserts the given public key, this should be used for users NOT local to this server
	InsertUserRoomPublicKey(ctx context.Context, userNID types.EventStateKeyNID, roomNID types.RoomNID, key ed25519.PublicKey) (ed25519.PublicKey, error)
	// SelectUserRoomPrivateKey selects the private key for the given user and room combination
	SelectUserRoomPrivateKey(ctx context.Context, userNID types.EventStateKeyNID, roomNID types.RoomNID) (ed25519.PrivateKey, error)
	// SelectUserRoomPublicKey selects the public key for the given user and room combination
	SelectUserRoomPublicKey(ctx context.Context, userNID types.EventStateKeyNID, roomNID types.RoomNID) (ed25519.PublicKey, error)
	// BulkSelectUserNIDs selects all userIDs for the requested senderKeys. Returns a map from publicKey -> types.UserRoomKeyPair.
	// If a senderKey can't be found, it is omitted in the result.
	BulkSelectUserNIDs(ctx context.Context, senderKeys map[types.RoomNID][]ed25519.PublicKey) (map[string]types.UserRoomKeyPair, error)
	// SelectAllPublicKeysForUser returns all known public keys for a user. Returns a map from room NID -> public key
	SelectAllPublicKeysForUser(ctx context.Context, userNID types.EventStateKeyNID) (map[types.RoomNID]ed25519.PublicKey, error)
}

// StrippedEvent represents a stripped event for returning extracted content values.
type StrippedEvent struct {
	RoomID       string
	EventType    string
	StateKey     string
	ContentValue string
}

// ExtractContentValue from the given state event. For example, given an m.room.name event with:
// content: { name: "Foo" }
// this returns "Foo".
func ExtractContentValue(ev *types.HeaderedEvent) string {
	content := ev.Content()
	key := ""
	switch ev.Type() {
	case spec.MRoomCreate:
		key = "creator"
	case spec.MRoomCanonicalAlias:
		key = "alias"
	case spec.MRoomHistoryVisibility:
		key = "history_visibility"
	case spec.MRoomJoinRules:
		key = "join_rule"
	case spec.MRoomMember:
		key = "membership"
	case spec.MRoomName:
		key = "name"
	case "m.room.avatar":
		key = "url"
	case "m.room.topic":
		key = "topic"
	case "m.room.guest_access":
		key = "guest_access"
	case "m.room.server_acl":
		// We need the entire content and not only one key, so we can use it
		// on startup to generate the ACLs. This is merely a workaround.
		return string(content)
	}
	result := gjson.GetBytes(content, key)
	if !result.Exists() {
		return ""
	}
	// this returns the empty string if this is not a string type
	return result.Str
}
