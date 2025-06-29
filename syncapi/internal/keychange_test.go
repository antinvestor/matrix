package internal

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/roomserver/api"
	"github.com/antinvestor/matrix/syncapi/synctypes"
	"github.com/antinvestor/matrix/syncapi/types"
	"github.com/antinvestor/matrix/test/testrig"
	userapi "github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

var (
	syncingUser = "@alice:localhost"
	emptyToken  = types.StreamPosition(0)
)

type mockKeyAPI struct{}

func (k *mockKeyAPI) PerformMarkAsStaleIfNeeded(ctx context.Context, req *userapi.PerformMarkAsStaleRequest, res *struct{}) error {
	return nil
}

func (k *mockKeyAPI) PerformUploadKeys(ctx context.Context, req *userapi.PerformUploadKeysRequest, res *userapi.PerformUploadKeysResponse) error {
	return nil
}

func (k *mockKeyAPI) SetUserAPI(i userapi.UserInternalAPI) {}

// PerformClaimKeys claims one-time keys for use in pre-key messages
func (k *mockKeyAPI) PerformClaimKeys(ctx context.Context, req *userapi.PerformClaimKeysRequest, res *userapi.PerformClaimKeysResponse) {
}
func (k *mockKeyAPI) PerformDeleteKeys(ctx context.Context, req *userapi.PerformDeleteKeysRequest, res *userapi.PerformDeleteKeysResponse) error {
	return nil
}
func (k *mockKeyAPI) PerformUploadDeviceKeys(ctx context.Context, req *userapi.PerformUploadDeviceKeysRequest, res *userapi.PerformUploadDeviceKeysResponse) {
}
func (k *mockKeyAPI) PerformUploadDeviceSignatures(ctx context.Context, req *userapi.PerformUploadDeviceSignaturesRequest, res *userapi.PerformUploadDeviceSignaturesResponse) {
}
func (k *mockKeyAPI) QueryKeys(ctx context.Context, req *userapi.QueryKeysRequest, res *userapi.QueryKeysResponse) {
}
func (k *mockKeyAPI) QueryKeyChanges(ctx context.Context, req *userapi.QueryKeyChangesRequest, res *userapi.QueryKeyChangesResponse) error {
	return nil
}
func (k *mockKeyAPI) QueryOneTimeKeys(ctx context.Context, req *userapi.QueryOneTimeKeysRequest, res *userapi.QueryOneTimeKeysResponse) error {
	return nil

}
func (k *mockKeyAPI) QueryDeviceMessages(ctx context.Context, req *userapi.QueryDeviceMessagesRequest, res *userapi.QueryDeviceMessagesResponse) error {
	return nil

}
func (k *mockKeyAPI) QuerySignatures(ctx context.Context, req *userapi.QuerySignaturesRequest, res *userapi.QuerySignaturesResponse) {
}

type keyChangeMockRoomserverAPI struct {
	api.RoomserverInternalAPI
	roomIDToJoinedMembers map[string][]string
}

func (s *keyChangeMockRoomserverAPI) QueryUserIDForSender(ctx context.Context, roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(senderID), true)
}

// QueryRoomsForUser retrieves a list of room IDs matching the given query.
func (s *keyChangeMockRoomserverAPI) QueryRoomsForUser(ctx context.Context, userID spec.UserID, desiredMembership string) ([]spec.RoomID, error) {
	return nil, nil
}

// QueryBulkStateContent does a bulk query for state event content in the given rooms.
func (s *keyChangeMockRoomserverAPI) QueryBulkStateContent(ctx context.Context, req *api.QueryBulkStateContentRequest, res *api.QueryBulkStateContentResponse) error {
	res.Rooms = make(map[string]map[gomatrixserverlib.StateKeyTuple]string)
	if req.AllowWildcards && len(req.StateTuples) == 1 && req.StateTuples[0].EventType == spec.MRoomMember && req.StateTuples[0].StateKey == "*" {
		for _, roomID := range req.RoomIDs {
			res.Rooms[roomID] = make(map[gomatrixserverlib.StateKeyTuple]string)
			for _, userID := range s.roomIDToJoinedMembers[roomID] {
				res.Rooms[roomID][gomatrixserverlib.StateKeyTuple{
					EventType: spec.MRoomMember,
					StateKey:  userID,
				}] = "join"
			}
		}
	}
	return nil
}

// QuerySharedUsers returns a list of users who share at least 1 room in common with the given user.
func (s *keyChangeMockRoomserverAPI) QuerySharedUsers(ctx context.Context, req *api.QuerySharedUsersRequest, res *api.QuerySharedUsersResponse) error {
	roomsToQuery := req.IncludeRoomIDs
	for roomID, members := range s.roomIDToJoinedMembers {
		exclude := false
		for _, excludeRoomID := range req.ExcludeRoomIDs {
			if roomID == excludeRoomID {
				exclude = true
				break
			}
		}
		if exclude {
			continue
		}
		for _, userID := range members {
			if userID == req.UserID {
				roomsToQuery = append(roomsToQuery, roomID)
				break
			}
		}
	}

	res.UserIDsToCount = make(map[string]int)
	for _, roomID := range roomsToQuery {
		for _, userID := range s.roomIDToJoinedMembers[roomID] {
			res.UserIDsToCount[userID]++
		}
	}
	return nil
}

// This is actually a database function, but seeing as we track the state inside the
// *mockRoomserverAPI, we'll just comply with the interface here instead.
func (s *keyChangeMockRoomserverAPI) SharedUsers(ctx context.Context, userID string, otherUserIDs []string) ([]string, error) {
	commonUsers := []string{}
	for _, members := range s.roomIDToJoinedMembers {
		for _, member := range members {
			for _, userID := range otherUserIDs {
				if member == userID {
					commonUsers = append(commonUsers, userID)
				}
			}
		}
	}
	return util.UniqueStrings(commonUsers), nil
}

type wantCatchup struct {
	hasNew  bool
	changed []string
	left    []string
}

func assertCatchup(t *testing.T, hasNew bool, syncResponse *types.Response, want wantCatchup) {
	t.Helper()
	if hasNew != want.hasNew {
		t.Errorf("got hasNew=%v want %v", hasNew, want.hasNew)
	}
	sort.Strings(syncResponse.DeviceLists.Left)
	if !reflect.DeepEqual(syncResponse.DeviceLists.Left, want.left) {
		t.Errorf("device_lists.left got %v want %v", syncResponse.DeviceLists.Left, want.left)
	}
	sort.Strings(syncResponse.DeviceLists.Changed)
	if !reflect.DeepEqual(syncResponse.DeviceLists.Changed, want.changed) {
		t.Errorf("device_lists.changed got %v want %v", syncResponse.DeviceLists.Changed, want.changed)
	}
}

func joinResponseWithRooms(syncResponse *types.Response, userID string, roomIDs []string) *types.Response {
	for _, roomID := range roomIDs {
		roomEvents := []synctypes.ClientEvent{
			{
				Type:     "m.room.member",
				StateKey: &userID,
				EventID:  "$something:here",
				Sender:   userID,
				RoomID:   roomID,
				Content:  []byte(`{"membership":"join"}`),
			},
		}
		jr, ok := syncResponse.Rooms.Join[roomID]
		if !ok {
			jr = types.NewJoinResponse()
		}
		jr.Timeline = &types.Timeline{}
		jr.State = &types.ClientEvents{Events: roomEvents}
		syncResponse.Rooms.Join[roomID] = jr
	}
	return syncResponse
}

func leaveResponseWithRooms(syncResponse *types.Response, userID string, roomIDs []string) *types.Response {
	for _, roomID := range roomIDs {
		roomEvents := []synctypes.ClientEvent{
			{
				Type:     "m.room.member",
				StateKey: &userID,
				EventID:  "$something:here",
				Sender:   userID,
				RoomID:   roomID,
				Content:  []byte(`{"membership":"leave"}`),
			},
		}

		lr, ok := syncResponse.Rooms.Leave[roomID]
		if !ok {
			lr = types.NewLeaveResponse()
		}
		lr.Timeline = &types.Timeline{Events: roomEvents}
		syncResponse.Rooms.Leave[roomID] = lr
	}
	return syncResponse
}

// tests that joining a room which results in sharing a new user includes that user in `changed`
func TestKeyChangeCatchupOnJoinShareNewUser(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	newShareUser := "@bill:localhost"
	newlyJoinedRoom := "!TestKeyChangeCatchupOnJoinShareNewUser:bar"
	syncResponse := types.NewResponse()
	syncResponse = joinResponseWithRooms(syncResponse, syncingUser, []string{newlyJoinedRoom})

	rsAPI := &keyChangeMockRoomserverAPI{
		roomIDToJoinedMembers: map[string][]string{
			newlyJoinedRoom: {syncingUser, newShareUser},
			"!another:room": {syncingUser},
		},
	}
	_, hasNew, err := DeviceListCatchup(ctx, rsAPI, &mockKeyAPI{}, rsAPI, syncingUser, syncResponse, emptyToken, emptyToken)
	if err != nil {
		t.Fatalf("DeviceListCatchup returned an error: %s", err)
	}
	assertCatchup(t, hasNew, syncResponse, wantCatchup{
		hasNew:  true,
		changed: []string{newShareUser},
	})
}

// tests that leaving a room which results in sharing no rooms with a user includes that user in `left`
func TestKeyChangeCatchupOnLeaveShareLeftUser(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)
	removeUser := "@bill:localhost"
	newlyLeftRoom := "!TestKeyChangeCatchupOnLeaveShareLeftUser:bar"
	syncResponse := types.NewResponse()
	syncResponse = leaveResponseWithRooms(syncResponse, syncingUser, []string{newlyLeftRoom})

	rsAPI := &keyChangeMockRoomserverAPI{
		roomIDToJoinedMembers: map[string][]string{
			newlyLeftRoom:   {removeUser},
			"!another:room": {syncingUser},
		},
	}
	_, hasNew, err := DeviceListCatchup(ctx, rsAPI, &mockKeyAPI{}, rsAPI, syncingUser, syncResponse, emptyToken, emptyToken)
	if err != nil {
		t.Fatalf("DeviceListCatchup returned an error: %s", err)
	}
	assertCatchup(t, hasNew, syncResponse, wantCatchup{
		hasNew: true,
		left:   []string{removeUser},
	})
}

// tests that joining a room which doesn't result in sharing a new user results in no changes.
func TestKeyChangeCatchupOnJoinShareNoNewUsers(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	existingUser := "@bob:localhost"
	newlyJoinedRoom := "!TestKeyChangeCatchupOnJoinShareNoNewUsers:bar"
	syncResponse := types.NewResponse()
	syncResponse = joinResponseWithRooms(syncResponse, syncingUser, []string{newlyJoinedRoom})

	rsAPI := &keyChangeMockRoomserverAPI{
		roomIDToJoinedMembers: map[string][]string{
			newlyJoinedRoom: {syncingUser, existingUser},
			"!another:room": {syncingUser, existingUser},
		},
	}
	_, hasNew, err := DeviceListCatchup(ctx, rsAPI, &mockKeyAPI{}, rsAPI, syncingUser, syncResponse, emptyToken, emptyToken)
	if err != nil {
		t.Fatalf("Catchup returned an error: %s", err)
	}
	assertCatchup(t, hasNew, syncResponse, wantCatchup{
		hasNew: false,
	})
}

// tests that leaving a room which doesn't result in sharing no rooms with a user results in no changes.
func TestKeyChangeCatchupOnLeaveShareNoUsers(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	existingUser := "@bob:localhost"
	newlyLeftRoom := "!TestKeyChangeCatchupOnLeaveShareNoUsers:bar"
	syncResponse := types.NewResponse()
	syncResponse = leaveResponseWithRooms(syncResponse, syncingUser, []string{newlyLeftRoom})

	rsAPI := &keyChangeMockRoomserverAPI{
		roomIDToJoinedMembers: map[string][]string{
			newlyLeftRoom:   {existingUser},
			"!another:room": {syncingUser, existingUser},
		},
	}
	_, hasNew, err := DeviceListCatchup(ctx, rsAPI, &mockKeyAPI{}, rsAPI, syncingUser, syncResponse, emptyToken, emptyToken)
	if err != nil {
		t.Fatalf("DeviceListCatchup returned an error: %s", err)
	}
	assertCatchup(t, hasNew, syncResponse, wantCatchup{
		hasNew: false,
	})
}

// tests that not joining any rooms (but having messages in the response) do not result in changes.
func TestKeyChangeCatchupNoNewJoinsButMessages(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	existingUser := "@bob1:localhost"
	roomID := "!TestKeyChangeCatchupNoNewJoinsButMessages:bar"
	syncResponse := types.NewResponse()
	empty := ""
	roomStateEvents := []synctypes.ClientEvent{
		{
			Type:     "m.room.name",
			StateKey: &empty,
			EventID:  "$something:here",
			Sender:   existingUser,
			RoomID:   roomID,
			Content:  []byte(`{"name":"The Room Name"}`),
		},
	}
	roomTimelineEvents := []synctypes.ClientEvent{
		{
			Type:    "m.room.message",
			EventID: "$something1:here",
			Sender:  existingUser,
			RoomID:  roomID,
			Content: []byte(`{"body":"Message 1"}`),
		},
		{
			Type:    "m.room.message",
			EventID: "$something2:here",
			Sender:  syncingUser,
			RoomID:  roomID,
			Content: []byte(`{"body":"Message 2"}`),
		},
		{
			Type:    "m.room.message",
			EventID: "$something3:here",
			Sender:  existingUser,
			RoomID:  roomID,
			Content: []byte(`{"body":"Message 3"}`),
		},
	}

	jr, ok := syncResponse.Rooms.Join[roomID]
	if !ok {
		jr = types.NewJoinResponse()
	}

	jr.State = &types.ClientEvents{Events: roomStateEvents}
	jr.Timeline = &types.Timeline{Events: roomTimelineEvents}
	syncResponse.Rooms.Join[roomID] = jr

	rsAPI := &keyChangeMockRoomserverAPI{
		roomIDToJoinedMembers: map[string][]string{
			roomID: {syncingUser, existingUser},
		},
	}
	_, hasNew, err := DeviceListCatchup(ctx, rsAPI, &mockKeyAPI{}, rsAPI, syncingUser, syncResponse, emptyToken, emptyToken)
	if err != nil {
		t.Fatalf("DeviceListCatchup returned an error: %s", err)
	}
	assertCatchup(t, hasNew, syncResponse, wantCatchup{
		hasNew: false,
	})
}

// tests that joining/leaving multiple rooms can result in both `changed` and `left` and they are not duplicated.
func TestKeyChangeCatchupChangeAndLeft(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	newShareUser := "@berta:localhost"
	newShareUser2 := "@bobby:localhost"
	newlyLeftUser := "@charlie:localhost"
	newlyLeftUser2 := "@debra:localhost"
	newlyJoinedRoom := "!join:bar"
	newlyLeftRoom := "!left:bar"
	syncResponse := types.NewResponse()
	syncResponse = joinResponseWithRooms(syncResponse, syncingUser, []string{newlyJoinedRoom})
	syncResponse = leaveResponseWithRooms(syncResponse, syncingUser, []string{newlyLeftRoom})

	rsAPI := &keyChangeMockRoomserverAPI{
		roomIDToJoinedMembers: map[string][]string{
			newlyJoinedRoom: {syncingUser, newShareUser, newShareUser2},
			newlyLeftRoom:   {newlyLeftUser, newlyLeftUser2},
			"!another:room": {syncingUser},
		},
	}
	_, hasNew, err := DeviceListCatchup(ctx, rsAPI, &mockKeyAPI{}, rsAPI, syncingUser, syncResponse, emptyToken, emptyToken)
	if err != nil {
		t.Fatalf("Catchup returned an error: %s", err)
	}
	assertCatchup(t, hasNew, syncResponse, wantCatchup{
		hasNew:  true,
		changed: []string{newShareUser, newShareUser2},
		left:    []string{newlyLeftUser, newlyLeftUser2},
	})
}

// tests that joining/leaving the SAME room puts users in `left` if the final state is leave.
// NB: Consider the case:
//   - Alice and Bob are in a room.
//   - Alice goes offline, Charlie joins, sends encrypted messages then leaves the room.
//   - Alice comes back online. Technically nothing has changed in the set of users between those two points in time,
//     it's still just (Alice,Bob) but then we won't be tracking Charlie -- is this okay though? It's device keys
//     which are only relevant when actively sending events I think? And if Alice does need the keys she knows
//     charlie's (user_id, device_id) so can just hit /keys/query - no need to keep updated about it because she
//     doesn't share any rooms with him.
//
// Ergo, we put them in `left` as it is simpler.
func TestKeyChangeCatchupChangeAndLeftSameRoom(t *testing.T) {

	ctx, svc, _ := testrig.Init(t)
	defer svc.Stop(ctx)

	newShareUser := "@berta:localhost"
	newShareUser2 := "@bobby:localhost"
	roomID := "!join:bar"
	syncResponse := types.NewResponse()
	roomEvents := []synctypes.ClientEvent{
		{
			Type:     "m.room.member",
			StateKey: &syncingUser,
			EventID:  "$something:here",
			Sender:   syncingUser,
			RoomID:   roomID,
			Content:  []byte(`{"membership":"join"}`),
		},
		{
			Type:    "m.room.message",
			EventID: "$something2:here",
			Sender:  syncingUser,
			RoomID:  roomID,
			Content: []byte(`{"body":"now I leave you"}`),
		},
		{
			Type:     "m.room.member",
			StateKey: &syncingUser,
			EventID:  "$something3:here",
			Sender:   syncingUser,
			RoomID:   roomID,
			Content:  []byte(`{"membership":"leave"}`),
		},
		{
			Type:     "m.room.member",
			StateKey: &syncingUser,
			EventID:  "$something4:here",
			Sender:   syncingUser,
			RoomID:   roomID,
			Content:  []byte(`{"membership":"join"}`),
		},
		{
			Type:    "m.room.message",
			EventID: "$something5:here",
			Sender:  syncingUser,
			RoomID:  roomID,
			Content: []byte(`{"body":"now I am back, and I leave you for good"}`),
		},
		{
			Type:     "m.room.member",
			StateKey: &syncingUser,
			EventID:  "$something6:here",
			Sender:   syncingUser,
			RoomID:   roomID,
			Content:  []byte(`{"membership":"leave"}`),
		},
	}

	lr, ok := syncResponse.Rooms.Leave[roomID]
	if !ok {
		lr = types.NewLeaveResponse()
	}
	lr.Timeline = &types.Timeline{Events: roomEvents}
	syncResponse.Rooms.Leave[roomID] = lr

	rsAPI := &keyChangeMockRoomserverAPI{
		roomIDToJoinedMembers: map[string][]string{
			roomID:          {newShareUser, newShareUser2},
			"!another:room": {syncingUser},
		},
	}
	_, hasNew, err := DeviceListCatchup(
		ctx, rsAPI, &mockKeyAPI{}, rsAPI, syncingUser, syncResponse, emptyToken, emptyToken,
	)
	if err != nil {
		t.Fatalf("DeviceListCatchup returned an error: %s", err)
	}
	assertCatchup(t, hasNew, syncResponse, wantCatchup{
		hasNew: true,
		left:   []string{newShareUser, newShareUser2},
	})
}
