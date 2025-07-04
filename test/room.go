// Copyright 2022 The Global.org Foundation C.I.C.
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

package test

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/eventutil"
	rstypes "github.com/antinvestor/matrix/roomserver/types"
)

type Preset int

var (
	PresetNone               Preset = 0
	PresetPrivateChat        Preset = 1
	PresetPublicChat         Preset = 2
	PresetTrustedPrivateChat Preset = 3

	roomIDCounter = int64(0)
)

func UserIDForSender(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(senderID), true)
}

type Room struct {
	ID           string
	Version      gomatrixserverlib.RoomVersion
	preset       Preset
	guestCanJoin bool
	visibility   gomatrixserverlib.HistoryVisibility
	creator      *User

	authEvents   *gomatrixserverlib.AuthEvents
	currentState map[string]*rstypes.HeaderedEvent
	events       []*rstypes.HeaderedEvent
}

// NewRoom Create a new test room. Automatically creates the initial create events.
func NewRoom(t *testing.T, creator *User, modifiers ...roomModifier) *Room {
	t.Helper()
	counter := atomic.AddInt64(&roomIDCounter, 1)
	if creator.srvName == "" {
		t.Fatalf("NewRoom: creator doesn't belong to a server: %+v", *creator)
	}

	authEnvents, err := gomatrixserverlib.NewAuthEvents(nil)
	if err != nil {
		t.Fatalf("NewAuthEvents: %v", err)
	}
	r := &Room{
		ID:           fmt.Sprintf("!%d:%s", counter, creator.srvName),
		creator:      creator,
		authEvents:   authEnvents,
		preset:       PresetPublicChat,
		Version:      gomatrixserverlib.RoomVersionV9,
		currentState: make(map[string]*rstypes.HeaderedEvent),
		visibility:   gomatrixserverlib.HistoryVisibilityShared,
	}
	for _, m := range modifiers {
		m(t, r)
	}
	r.insertCreateEvents(t)
	return r
}

func (r *Room) MustGetAuthEventRefsForEvent(t *testing.T, needed gomatrixserverlib.StateNeeded) []string {
	t.Helper()
	a, err := needed.AuthEventReferences(r.authEvents)
	if err != nil {
		t.Fatalf("MustGetAuthEvents: %v", err)
	}
	return a
}

func (r *Room) ForwardExtremities() []string {
	if len(r.events) == 0 {
		return nil
	}
	return []string{
		r.events[len(r.events)-1].EventID(),
	}
}

func (r *Room) insertCreateEvents(t *testing.T) {
	t.Helper()
	var joinRule gomatrixserverlib.JoinRuleContent
	var hisVis gomatrixserverlib.HistoryVisibilityContent
	plContent := eventutil.InitialPowerLevelsContent(r.creator.ID)
	switch r.preset {
	case PresetTrustedPrivateChat:
		fallthrough
	case PresetPrivateChat:
		joinRule.JoinRule = "invite"
		hisVis.HistoryVisibility = gomatrixserverlib.HistoryVisibilityShared
	case PresetPublicChat:
		joinRule.JoinRule = "public"
		hisVis.HistoryVisibility = gomatrixserverlib.HistoryVisibilityShared
	}

	if r.visibility != "" {
		hisVis.HistoryVisibility = r.visibility
	}

	r.CreateAndInsert(t, r.creator, spec.MRoomCreate, map[string]interface{}{
		"creator":      r.creator.ID,
		"room_version": r.Version,
	}, WithStateKey(""))
	r.CreateAndInsert(t, r.creator, spec.MRoomMember, map[string]interface{}{
		"membership": "join",
	}, WithStateKey(r.creator.ID))
	r.CreateAndInsert(t, r.creator, spec.MRoomPowerLevels, plContent, WithStateKey(""))
	r.CreateAndInsert(t, r.creator, spec.MRoomJoinRules, joinRule, WithStateKey(""))
	r.CreateAndInsert(t, r.creator, spec.MRoomHistoryVisibility, hisVis, WithStateKey(""))
	if r.guestCanJoin {
		r.CreateAndInsert(t, r.creator, spec.MRoomGuestAccess, map[string]string{
			"guest_access": "can_join",
		}, WithStateKey(""))
	}
}

// CreateEvent an event in this room but do not insert it. Does not modify the room in any way (depth, fwd extremities, etc) so is thread-safe.
func (r *Room) CreateEvent(t *testing.T, creator *User, eventType string, content interface{}, mods ...eventModifier) *rstypes.HeaderedEvent {
	t.Helper()
	depth := 1 + len(r.events) // depth starts at 1

	// possible event modifiers (optional fields)
	mod := &eventMods{}
	for _, m := range mods {
		m(mod)
	}

	if mod.privKey == nil {
		mod.privKey = creator.privKey
	}
	if mod.keyID == "" {
		mod.keyID = creator.keyID
	}
	if mod.originServerTS.IsZero() {
		mod.originServerTS = time.Now()
	}
	if mod.origin == "" {
		mod.origin = creator.srvName
	}

	var unsigned json.RawMessage
	var err error
	if mod.unsigned != nil {
		unsigned, err = json.Marshal(mod.unsigned)
		if err != nil {
			t.Fatalf("CreateEvent[%s]: failed to marshal unsigned field: %s", eventType, err)
		}
	}

	builder := gomatrixserverlib.MustGetRoomVersion(r.Version).NewEventBuilderFromProtoEvent(&gomatrixserverlib.ProtoEvent{
		SenderID: creator.ID,
		RoomID:   r.ID,
		Type:     eventType,
		StateKey: mod.stateKey,
		Depth:    int64(depth),
		Unsigned: unsigned,
	})
	err = builder.SetContent(content)
	if err != nil {
		t.Fatalf("CreateEvent[%s]: failed to SetContent: %s", eventType, err)
	}
	if depth > 1 {
		builder.PrevEvents = []string{r.events[len(r.events)-1].EventID()}
	}

	err = builder.AddAuthEvents(r.authEvents)
	if err != nil {
		t.Fatalf("CreateEvent[%s]: failed to AuthEventReferences: %s", eventType, err)
	}

	if len(mod.authEvents) > 0 {
		builder.AuthEvents = mod.authEvents
	}

	ev, err := builder.Build(
		mod.originServerTS, mod.origin, mod.keyID,
		mod.privKey,
	)
	if err != nil {
		t.Fatalf("CreateEvent[%s]: failed to build event: %s", eventType, err)
	}
	if err = gomatrixserverlib.Allowed(ev, r.authEvents, UserIDForSender); err != nil {
		t.Fatalf("CreateEvent[%s]: failed to verify event was allowed: %s", eventType, err)
	}
	headeredEvent := &rstypes.HeaderedEvent{PDU: ev}
	headeredEvent.Visibility = r.visibility
	return headeredEvent
}

// InsertEvent Add a new event to this room DAG. Not thread-safe.
func (r *Room) InsertEvent(t *testing.T, he *rstypes.HeaderedEvent) {
	t.Helper()
	// Add the event to the list of auth/state events
	r.events = append(r.events, he)
	if he.StateKey() != nil {
		err := r.authEvents.AddEvent(he.PDU)
		if err != nil {
			t.Fatalf("InsertEvent: failed to add event to auth events: %s", err)
		}
		r.currentState[he.Type()+" "+*he.StateKey()] = he
	}
}

func (r *Room) Events() []*rstypes.HeaderedEvent {
	return r.events
}

func (r *Room) CurrentState() []*rstypes.HeaderedEvent {
	events := make([]*rstypes.HeaderedEvent, len(r.currentState))
	i := 0
	for _, e := range r.currentState {
		events[i] = e
		i++
	}
	return events
}

func (r *Room) CreateAndInsert(t *testing.T, creator *User, eventType string, content interface{}, mods ...eventModifier) *rstypes.HeaderedEvent {
	t.Helper()
	he := r.CreateEvent(t, creator, eventType, content, mods...)
	r.InsertEvent(t, he)
	return he
}

// All room modifiers are below

type roomModifier func(t *testing.T, r *Room)

func RoomPreset(p Preset) roomModifier {
	return func(t *testing.T, r *Room) {
		switch p {
		case PresetPrivateChat:
			fallthrough
		case PresetPublicChat:
			fallthrough
		case PresetTrustedPrivateChat:
			fallthrough
		case PresetNone:
			r.preset = p
		default:
			t.Errorf("invalid RoomPreset: %v", p)
		}
	}
}

func RoomHistoryVisibility(vis gomatrixserverlib.HistoryVisibility) roomModifier {
	return func(t *testing.T, r *Room) {
		r.visibility = vis
	}
}

func RoomVersion(ver gomatrixserverlib.RoomVersion) roomModifier {
	return func(t *testing.T, r *Room) {
		r.Version = ver
	}
}

func GuestsCanJoin(canJoin bool) roomModifier {
	return func(t *testing.T, r *Room) {
		r.guestCanJoin = canJoin
	}
}
