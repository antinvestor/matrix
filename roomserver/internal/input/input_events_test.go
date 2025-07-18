package input

import (
	"testing"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/test"
)

func Test_EventAuth(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)

	// create two rooms, so we can craft "illegal" auth events
	room1 := test.NewRoom(t, alice)
	room2 := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))

	authEventIDs := make([]string, 0, 4)
	authEvents := []gomatrixserverlib.PDU{}

	// Add the legal auth events from room2
	for _, x := range room2.Events() {
		if x.Type() == spec.MRoomCreate {
			authEventIDs = append(authEventIDs, x.EventID())
			authEvents = append(authEvents, x.PDU)
		}
		if x.Type() == spec.MRoomPowerLevels {
			authEventIDs = append(authEventIDs, x.EventID())
			authEvents = append(authEvents, x.PDU)
		}
		if x.Type() == spec.MRoomJoinRules {
			authEventIDs = append(authEventIDs, x.EventID())
			authEvents = append(authEvents, x.PDU)
		}
	}

	// Add the illegal auth event from room1 (rooms are different)
	for _, x := range room1.Events() {
		if x.Type() == spec.MRoomMember {
			authEventIDs = append(authEventIDs, x.EventID())
			authEvents = append(authEvents, x.PDU)
		}
	}

	// Craft the illegal join event, with auth events from different rooms
	ev := room2.CreateEvent(t, bob, "m.room.member", map[string]interface{}{
		"membership": "join",
	}, test.WithStateKey(bob.ID), test.WithAuthIDs(authEventIDs))

	// Add the auth events to the allower
	allower, err := gomatrixserverlib.NewAuthEvents(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range authEvents {
		if err = allower.AddEvent(a); err != nil {
			t.Fatalf("allower.AddEvent failed: %v", err)
		}
	}

	// Finally check that the event is NOT allowed
	err = gomatrixserverlib.Allowed(ev.PDU, allower, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return spec.NewUserID(string(senderID), true)
	})
	if err == nil {
		t.Fatalf("event should not be allowed, but it was")
	}
}
