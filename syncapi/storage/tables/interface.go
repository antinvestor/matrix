// Copyright 2020 The Global.org Foundation C.I.C.
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

package tables

import (
	"context"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"

	"github.com/antinvestor/matrix/internal/eventutil"
	"github.com/antinvestor/matrix/roomserver/api"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
)

// AccountData keeps track of account data for users, both global and per-room.
//
// InsertAccountData stores account data for a user, optionally scoped to a room.
// SelectAccountDataInRange returns a map of room ID to a list of `dataType` for the given user in the given range.
// SelectMaxAccountDataID returns the maximum stream position for account data.
type AccountData interface {
	// InsertAccountData stores account data for a user, optionally scoped to a room.
	InsertAccountData(ctx context.Context, userID, roomID, dataType string) (pos types.StreamPosition, err error)
	// SelectAccountDataInRange returns a map of room ID to a list of `dataType` for the given user in the given range.
	SelectAccountDataInRange(ctx context.Context, userID string, r types.Range, accountDataEventFilter *synctypes.EventFilter) (data map[string][]string, pos types.StreamPosition, err error)
	// SelectMaxAccountDataID returns the maximum stream position for account data.
	SelectMaxAccountDataID(ctx context.Context) (id int64, err error)
}

// Invites manages the storage of invite events for users.
//
// InsertInviteEvent stores a new invite event for a user.
// DeleteInviteEvent removes an invite event by its ID.
// SelectInviteEventsInRange returns invites and retired invites for a user in a given range.
// SelectMaxInviteID returns the maximum stream position for invites.
// PurgeInvites removes all invites for a given room.
type Invites interface {
	InsertInviteEvent(ctx context.Context, inviteEvent *rstypes.HeaderedEvent) (streamPos types.StreamPosition, err error)
	DeleteInviteEvent(ctx context.Context, inviteEventID string) (types.StreamPosition, error)
	SelectInviteEventsInRange(ctx context.Context, targetUserID string, r types.Range) (invites map[string]*rstypes.HeaderedEvent, retired map[string]*rstypes.HeaderedEvent, maxID types.StreamPosition, err error)
	SelectMaxInviteID(ctx context.Context) (id int64, err error)
	PurgeInvites(ctx context.Context, roomID string) error
}

// Peeks tracks peeks (temporary room joins) for users and devices.
//
// InsertPeek records a new peek for a user/device.
// DeletePeek removes a peek for a user/device.
// DeletePeeks removes all peeks for a user in a room.
// SelectPeeksInRange returns peeks for a user/device in a given range.
// SelectPeekingDevices returns all peeking devices.
// SelectMaxPeekID returns the maximum stream position for peeks.
// PurgePeeks removes all peeks for a given room.
type Peeks interface {
	InsertPeek(ctx context.Context, roomID, userID, deviceID string) (streamPos types.StreamPosition, err error)
	DeletePeek(ctx context.Context, roomID, userID, deviceID string) (streamPos types.StreamPosition, err error)
	DeletePeeks(ctx context.Context, roomID, userID string) (streamPos types.StreamPosition, err error)
	SelectPeeksInRange(ctx context.Context, userID, deviceID string, r types.Range) (peeks []types.Peek, err error)
	SelectPeekingDevices(ctx context.Context) (peekingDevices map[string][]types.PeekingDevice, err error)
	SelectMaxPeekID(ctx context.Context) (id int64, err error)
	PurgePeeks(ctx context.Context, roomID string) error
}

// Events manages the storage of events in the database.
//
// SelectStateInRange returns state events in a given range.
// SelectMaxEventID returns the maximum stream position for events.
// InsertEvent stores a new event in the database.
// SelectRecentEvents returns recent events for a given set of rooms.
// SelectEvents returns events by their IDs.
// UpdateEventJSON updates the JSON of an event.
// DeleteEventsForRoom removes all events for a given room.
//
// SearchEvents searches for events based on a search term.
// ExcludeEventsFromSearchIndex removes events from the search index.
//
// SelectContextEvent returns the context event for a given event.
// SelectContextBeforeEvent returns events before a given event in a room.
// SelectContextAfterEvent returns events after a given event in a room.
//
// PurgeEvents removes all events for a given room.
// ReIndex reindexes events in the database.
type Events interface {
	// SelectStateInRange returns state events in a given range.
	SelectStateInRange(ctx context.Context, r types.Range, stateFilter *synctypes.StateFilter, roomIDs []string) (map[string]map[string]bool, map[string]types.StreamEvent, error)
	// SelectMaxEventID returns the maximum stream position for events.
	SelectMaxEventID(ctx context.Context) (id int64, err error)
	// InsertEvent stores a new event in the database.
	InsertEvent(
		ctx context.Context,
		event *rstypes.HeaderedEvent,
		addState, removeState []string,
		transactionID *api.TransactionID,
		excludeFromSync bool,
		historyVisibility gomatrixserverlib.HistoryVisibility,
	) (streamPos types.StreamPosition, err error)
	// SelectRecentEvents returns recent events for a given set of rooms.
	SelectRecentEvents(ctx context.Context, roomIDs []string, r types.Range, eventFilter *synctypes.RoomEventFilter, chronologicalOrder bool, onlySyncEvents bool) (map[string]types.RecentEvents, error)
	// SelectEvents returns events by their IDs.
	SelectEvents(ctx context.Context, eventIDs []string, filter *synctypes.RoomEventFilter, preserveOrder bool) ([]types.StreamEvent, error)
	// UpdateEventJSON updates the JSON of an event.
	UpdateEventJSON(ctx context.Context, event *rstypes.HeaderedEvent) error
	// DeleteEventsForRoom removes all events for a given room.
	DeleteEventsForRoom(ctx context.Context, roomID string) (err error)

	// SearchEvents searches for events based on a search term.
	SearchEvents(ctx context.Context, searchTerm string, roomIDs []string, keys []string, limit, offset int) (*types.SearchResult, error)
	// ExcludeEventsFromSearchIndex removes events from the search index.
	ExcludeEventsFromSearchIndex(ctx context.Context, eventIDs []string) error

	// SelectContextEvent returns the context event for a given event.
	SelectContextEvent(ctx context.Context, roomID, eventID string) (int, rstypes.HeaderedEvent, error)
	// SelectContextBeforeEvent returns events before a given event in a room.
	SelectContextBeforeEvent(ctx context.Context, id int, roomID string, filter *synctypes.RoomEventFilter) ([]*rstypes.HeaderedEvent, error)
	// SelectContextAfterEvent returns events after a given event in a room.
	SelectContextAfterEvent(ctx context.Context, id int, roomID string, filter *synctypes.RoomEventFilter) (int, []*rstypes.HeaderedEvent, error)

	// PurgeEvents removes all events for a given room.
	PurgeEvents(ctx context.Context, roomID string) error
	// ReIndex reindexes events in the database.
	ReIndex(ctx context.Context, limit, offset int64, types []string) (map[int64]rstypes.HeaderedEvent, error)
}

// Topology manages the storage of event topology in the database.
//
// InsertEventInTopology stores a new event in the topology.
// SelectEventIDsInRange returns event IDs in a given range.
// SelectPositionInTopology returns the position of an event in the topology.
// SelectStreamToTopologicalPosition returns the topological position of a stream position.
// PurgeEventsTopology removes all events from the topology for a given room.
type Topology interface {
	// InsertEventInTopology stores a new event in the topology.
	InsertEventInTopology(ctx context.Context, event *rstypes.HeaderedEvent, pos types.StreamPosition) (topoPos types.StreamPosition, err error)
	// SelectEventIDsInRange returns event IDs in a given range.
	SelectEventIDsInRange(ctx context.Context, roomID string, minDepth, maxDepth, maxStreamPos types.StreamPosition, limit int, chronologicalOrder bool) (eventIDs []string, start, end types.TopologyToken, err error)
	// SelectPositionInTopology returns the position of an event in the topology.
	SelectPositionInTopology(ctx context.Context, eventID string) (depth, spos types.StreamPosition, err error)
	// SelectStreamToTopologicalPosition returns the topological position of a stream position.
	SelectStreamToTopologicalPosition(ctx context.Context, roomID string, streamPos types.StreamPosition, forward bool) (topoPos types.StreamPosition, err error)
	// PurgeEventsTopology removes all events from the topology for a given room.
	PurgeEventsTopology(ctx context.Context, roomID string) error
}

// CurrentRoomState manages the storage of current room state in the database.
//
// SelectStateEvent returns the state event for a given room and event type.
// SelectEventsWithEventIDs returns events by their IDs.
// UpsertRoomState stores a new state event in the database.
// DeleteRoomStateByEventID removes a state event by its ID.
// DeleteRoomStateForRoom removes all state events for a given room.
// SelectCurrentState returns the current state of a room.
// SelectRoomIDsWithMembership returns room IDs with a given membership for a user.
// SelectRoomIDsWithAnyMembership returns room IDs with any membership for a user.
// SelectJoinedUsers returns joined users in a room.
// SelectJoinedUsersInRoom returns joined users in a given set of rooms.
// SelectSharedUsers returns shared users between a user and other users.
//
// SelectRoomHeroes returns heroes (users with a specific power level) in a room.
// SelectMembershipCount returns the count of users with a given membership in a room.
type CurrentRoomState interface {
	// SelectStateEvent returns the state event for a given room and event type.
	SelectStateEvent(ctx context.Context, roomID, evType, stateKey string) (*rstypes.HeaderedEvent, error)
	// SelectEventsWithEventIDs returns events by their IDs.
	SelectEventsWithEventIDs(ctx context.Context, eventIDs []string) ([]types.StreamEvent, error)
	// UpsertRoomState stores a new state event in the database.
	UpsertRoomState(ctx context.Context, event *rstypes.HeaderedEvent, membership *string, addedAt types.StreamPosition) error
	// DeleteRoomStateByEventID removes a state event by its ID.
	DeleteRoomStateByEventID(ctx context.Context, eventID string) error
	// DeleteRoomStateForRoom removes all state events for a given room.
	DeleteRoomStateForRoom(ctx context.Context, roomID string) error
	// SelectCurrentState returns the current state of a room.
	SelectCurrentState(ctx context.Context, roomID string, stateFilter *synctypes.StateFilter, excludeEventIDs []string) ([]*rstypes.HeaderedEvent, error)
	// SelectRoomIDsWithMembership returns room IDs with a given membership for a user.
	SelectRoomIDsWithMembership(ctx context.Context, userID string, membership string) ([]string, error)
	// SelectRoomIDsWithAnyMembership returns room IDs with any membership for a user.
	SelectRoomIDsWithAnyMembership(ctx context.Context, userID string) (map[string]string, error)
	// SelectJoinedUsers returns joined users in a room.
	SelectJoinedUsers(ctx context.Context) (map[string][]string, error)
	// SelectJoinedUsersInRoom returns joined users in a given set of rooms.
	SelectJoinedUsersInRoom(ctx context.Context, roomIDs []string) (map[string][]string, error)
	// SelectSharedUsers returns shared users between a user and other users.
	SelectSharedUsers(ctx context.Context, userID string, otherUserIDs []string) ([]string, error)

	// SelectRoomHeroes returns heroes (users with a specific power level) in a room.
	SelectRoomHeroes(ctx context.Context, roomID, excludeUserID string, memberships []string) ([]string, error)
	// SelectMembershipCount returns the count of users with a given membership in a room.
	SelectMembershipCount(ctx context.Context, roomID, membership string) (int, error)
}

// BackwardsExtremities manages the storage of backwards extremities in the database.
//
// InsertsBackwardExtremity stores a new backwards extremity.
// SelectBackwardExtremitiesForRoom returns backwards extremities for a given room.
// DeleteBackwardExtremity removes a backwards extremity.
// PurgeBackwardExtremities removes all backwards extremities for a given room.
type BackwardsExtremities interface {
	// InsertsBackwardExtremity stores a new backwards extremity.
	InsertsBackwardExtremity(ctx context.Context, roomID, eventID, prevEventID string) (err error)
	// SelectBackwardExtremitiesForRoom returns backwards extremities for a given room.
	SelectBackwardExtremitiesForRoom(ctx context.Context, roomID string) (bwExtrems map[string][]string, err error)
	// DeleteBackwardExtremity removes a backwards extremity.
	DeleteBackwardExtremity(ctx context.Context, roomID, knownEventID string) (err error)
	// PurgeBackwardExtremities removes all backwards extremities for a given room.
	PurgeBackwardExtremities(ctx context.Context, roomID string) error
}

// SendToDevice manages the storage of send-to-device messages in the database.
//
// InsertSendToDeviceMessage stores a new send-to-device message.
// SelectSendToDeviceMessages returns send-to-device messages for a user and device.
// DeleteSendToDeviceMessages removes send-to-device messages for a user and device.
// SelectMaxSendToDeviceMessageID returns the maximum stream position for send-to-device messages.
type SendToDevice interface {
	// InsertSendToDeviceMessage stores a new send-to-device message.
	InsertSendToDeviceMessage(ctx context.Context, userID, deviceID, content string) (pos types.StreamPosition, err error)
	// SelectSendToDeviceMessages returns send-to-device messages for a user and device.
	SelectSendToDeviceMessages(ctx context.Context, userID, deviceID string, from, to types.StreamPosition) (lastPos types.StreamPosition, events []types.SendToDeviceEvent, err error)
	// DeleteSendToDeviceMessages removes send-to-device messages for a user and device.
	DeleteSendToDeviceMessages(ctx context.Context, userID, deviceID string, from types.StreamPosition) (err error)
	// SelectMaxSendToDeviceMessageID returns the maximum stream position for send-to-device messages.
	SelectMaxSendToDeviceMessageID(ctx context.Context) (id int64, err error)
}

// Filter manages the storage of filters in the database.
//
// SelectFilter returns a filter by its ID.
// InsertFilter stores a new filter.
type Filter interface {
	// SelectFilter returns a filter by its ID.
	SelectFilter(ctx context.Context, target *synctypes.Filter, localpart string, filterID string) error
	// InsertFilter stores a new filter.
	InsertFilter(ctx context.Context, filter *synctypes.Filter, localpart string) (filterID string, err error)
}

// Receipts manages the storage of receipts in the database.
//
// UpsertReceipt stores a new receipt.
// SelectRoomReceiptsAfter returns receipts for a given room after a certain stream position.
// SelectMaxReceiptID returns the maximum stream position for receipts.
// PurgeReceipts removes all receipts for a given room.
type Receipts interface {
	// UpsertReceipt stores a new receipt.
	UpsertReceipt(ctx context.Context, roomId, receiptType, userId, eventId string, timestamp spec.Timestamp) (pos types.StreamPosition, err error)
	// SelectRoomReceiptsAfter returns receipts for a given room after a certain stream position.
	SelectRoomReceiptsAfter(ctx context.Context, roomIDs []string, streamPos types.StreamPosition) (types.StreamPosition, []types.OutputReceiptEvent, error)
	// SelectMaxReceiptID returns the maximum stream position for receipts.
	SelectMaxReceiptID(ctx context.Context) (id int64, err error)
	// PurgeReceipts removes all receipts for a given room.
	PurgeReceipts(ctx context.Context, roomID string) error
}

// Memberships manages the storage of memberships in the database.
//
// UpsertMembership stores a new membership.
// SelectMembershipCount returns the count of users with a given membership in a room.
// SelectMembershipForUser returns the membership of a user in a room.
// PurgeMemberships removes all memberships for a given room.
// SelectMemberships returns event IDs with a given membership in a room.
type Memberships interface {
	// UpsertMembership stores a new membership.
	UpsertMembership(ctx context.Context, event *rstypes.HeaderedEvent, streamPos, topologicalPos types.StreamPosition) error
	// SelectMembershipCount returns the count of users with a given membership in a room.
	SelectMembershipCount(ctx context.Context, roomID, membership string, pos types.StreamPosition) (count int, err error)
	// SelectMembershipForUser returns the membership of a user in a room.
	SelectMembershipForUser(ctx context.Context, roomID, userID string, pos int64) (membership string, topologicalPos int64, err error)
	// PurgeMemberships removes all memberships for a given room.
	PurgeMemberships(ctx context.Context, roomID string) error
	// SelectMemberships returns event IDs with a given membership in a room.
	SelectMemberships(
		ctx context.Context,
		roomID string, pos types.TopologyToken,
		membership, notMembership *string,
	) (eventIDs []string, err error)
}

// NotificationData manages the storage of notification data in the database.
//
// UpsertRoomUnreadCounts stores unread counts for a room.
// SelectUserUnreadCountsForRooms returns unread counts for a user in a given set of rooms.
// SelectMaxID returns the maximum stream position for notification data.
// PurgeNotificationData removes all notification data for a given room.
type NotificationData interface {
	// UpsertRoomUnreadCounts stores unread counts for a room.
	UpsertRoomUnreadCounts(ctx context.Context, userID, roomID string, notificationCount, highlightCount int) (types.StreamPosition, error)
	// SelectUserUnreadCountsForRooms returns unread counts for a user in a given set of rooms.
	SelectUserUnreadCountsForRooms(ctx context.Context, userID string, roomIDs []string) (map[string]*eventutil.NotificationData, error)
	// SelectMaxID returns the maximum stream position for notification data.
	SelectMaxID(ctx context.Context) (int64, error)
	// PurgeNotificationData removes all notification data for a given room.
	PurgeNotificationData(ctx context.Context, roomID string) error
}

// Ignores manages the storage of ignores in the database.
//
// SelectIgnores returns ignores for a user.
// UpsertIgnores stores ignores for a user.
type Ignores interface {
	// SelectIgnores returns ignores for a user.
	SelectIgnores(ctx context.Context, userID string) (*types.IgnoredUsers, error)
	// UpsertIgnores stores ignores for a user.
	UpsertIgnores(ctx context.Context, userID string, ignores *types.IgnoredUsers) error
}

// Presence manages the storage of presence in the database.
//
// UpsertPresence stores presence for a user.
// GetPresenceForUsers returns presence for a given set of users.
// GetMaxPresenceID returns the maximum stream position for presence.
// GetPresenceAfter returns presence after a certain stream position.
type Presence interface {
	// UpsertPresence stores presence for a user.
	UpsertPresence(ctx context.Context, userID string, statusMsg *string, presence types.Presence, lastActiveTS spec.Timestamp, fromSync bool) (pos types.StreamPosition, err error)
	// GetPresenceForUsers returns presence for a given set of users.
	GetPresenceForUsers(ctx context.Context, userIDs []string) (presence []*types.PresenceInternal, err error)
	// GetMaxPresenceID returns the maximum stream position for presence.
	GetMaxPresenceID(ctx context.Context) (pos types.StreamPosition, err error)
	// GetPresenceAfter returns presence after a certain stream position.
	GetPresenceAfter(ctx context.Context, after types.StreamPosition, filter synctypes.EventFilter) (presences map[string]*types.PresenceInternal, err error)
}

// Relations manages the storage of relations in the database.
//
// InsertRelation stores a new relation.
// DeleteRelation removes a relation.
// SelectRelationsInRange returns relations in a given range.
// SelectMaxRelationID returns the maximum stream position for relations.
type Relations interface {
	// InsertRelation stores a new relation.
	InsertRelation(ctx context.Context, roomID, eventID, childEventID, childEventType, relType string) (err error)
	// DeleteRelation removes a relation.
	DeleteRelation(ctx context.Context, roomID, childEventID string) error
	// SelectRelationsInRange returns relations in a given range.
	SelectRelationsInRange(ctx context.Context, roomID, eventID, relType, eventType string, r types.Range, limit int) (map[string][]types.RelationEntry, types.StreamPosition, error)
	// SelectMaxRelationID returns the maximum stream position for relations.
	SelectMaxRelationID(ctx context.Context) (id int64, err error)
}
