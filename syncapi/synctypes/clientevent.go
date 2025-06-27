/* Copyright 2017 Vector Creations Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package synctypes

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/pitabwire/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// PrevEventRef represents a reference to a previous event in a state event upgrade
type PrevEventRef struct {
	PrevContent   json.RawMessage `json:"prev_content"`
	ReplacesState string          `json:"replaces_state"`
	PrevSenderID  string          `json:"prev_sender"`
}

type ClientEventFormat int

const (
	// FormatAll will include all client event keys
	FormatAll ClientEventFormat = iota
	// FormatSync will include only the event keys required by the /sync API. Notably, this
	// means the 'room_id' will be missing from the events.
	FormatSync
	// FormatSyncFederation will include all event keys normally included in federated events.
	// This allows clients to request federated formatted events via the /sync API.
	FormatSyncFederation
)

// ClientFederationFields extends a ClientEvent to contain the additional fields present in a
// federation event. Used when the client requests `event_format` of type `federation`.
type ClientFederationFields struct {
	Depth      int64           `json:"depth,omitempty"`
	PrevEvents []string        `json:"prev_events,omitempty"`
	AuthEvents []string        `json:"auth_events,omitempty"`
	Signatures json.RawMessage `json:"signatures,omitempty"`
	Hashes     json.RawMessage `json:"hashes,omitempty"`
}

// ClientEvent is an event which is fit for consumption by clients, in accordance with the specification.
type ClientEvent struct {
	Content        json.RawMessage `json:"content"`
	EventID        string          `json:"event_id,omitempty"`         // EventID is omitted on receipt events
	OriginServerTS spec.Timestamp  `json:"origin_server_ts,omitempty"` // OriginServerTS is omitted on receipt events
	RoomID         string          `json:"room_id,omitempty"`          // RoomID is omitted on /sync responses
	Sender         string          `json:"sender,omitempty"`           // Sender is omitted on receipt events
	SenderKey      spec.SenderID   `json:"sender_key,omitempty"`       // The SenderKey for events in pseudo ID rooms
	StateKey       *string         `json:"state_key,omitempty"`
	Type           string          `json:"type"`
	Unsigned       json.RawMessage `json:"unsigned,omitempty"`
	Redacts        string          `json:"redacts,omitempty"`

	// Only sent to clients when `event_format` == `federation`.
	ClientFederationFields
}

// ToClientEvents converts server events to client events.
func ToClientEvents(ctx context.Context, serverEvs []gomatrixserverlib.PDU, format ClientEventFormat, userIDForSender spec.UserIDForSender) []ClientEvent {
	evs := make([]ClientEvent, 0, len(serverEvs))
	for _, se := range serverEvs {
		if se == nil {
			continue // TODO: shouldn't happen?
		}
		ev, err := ToClientEvent(se, format, userIDForSender)
		if err != nil {
			util.Log(ctx).WithError(err).Warn("Failed converting event to ClientEvent")
			continue
		}
		evs = append(evs, *ev)
	}
	return evs
}

// ToClientEventDefault converts a single server event to a client event.
// It provides default logic for event.SenderID & event.StateKey -> userID conversions.
func ToClientEventDefault(userIDQuery spec.UserIDForSender, event gomatrixserverlib.PDU) ClientEvent {
	ev, err := ToClientEvent(event, FormatAll, userIDQuery)
	if err != nil {
		return ClientEvent{}
	}
	return *ev
}

// If provided state key is a user ID (state keys beginning with @ are reserved for this purpose)
// fetch it's associated sender ID and use that instead. Otherwise returns the same state key back.
//
// # This function either returns the state key that should be used, or an error
//
// TODO: handle failure cases better (e.g. no sender ID)
func FromClientStateKey(roomID spec.RoomID, stateKey string, senderIDQuery spec.SenderIDForUser) (*string, error) {
	if len(stateKey) >= 1 && stateKey[0] == '@' {
		parsedStateKey, err := spec.NewUserID(stateKey, true)
		if err != nil {
			// If invalid user ID, then there is no associated state event.
			return nil, fmt.Errorf("provided state key begins with @ but is not a valid user ID: %w", err)
		}
		senderID, err := senderIDQuery(roomID, *parsedStateKey)
		if err != nil {
			return nil, fmt.Errorf("failed to query sender ID: %w", err)
		}
		if senderID == nil {
			// If no sender ID, then there is no associated state event.
			return nil, fmt.Errorf("no associated sender ID found")
		}
		newStateKey := string(*senderID)
		return &newStateKey, nil
	} else {
		return &stateKey, nil
	}
}

// ToClientEvent converts a single server event to a client event.
func ToClientEvent(se gomatrixserverlib.PDU, format ClientEventFormat, userIDForSender spec.UserIDForSender) (*ClientEvent, error) {
	ce := ClientEvent{
		Content:        se.Content(),
		Sender:         string(se.SenderID()),
		Type:           se.Type(),
		StateKey:       se.StateKey(),
		Unsigned:       se.Unsigned(),
		OriginServerTS: se.OriginServerTS(),
		EventID:        se.EventID(),
		Redacts:        se.Redacts(),
	}

	switch format {
	case FormatAll:
		ce.RoomID = se.RoomID().String()
	case FormatSync:
	case FormatSyncFederation:
		ce.RoomID = se.RoomID().String()
		ce.AuthEvents = se.AuthEventIDs()
		ce.PrevEvents = se.PrevEventIDs()
		ce.Depth = se.Depth()
		// TODO: Set Signatures & Hashes fields
	}

	if format != FormatSyncFederation && se.Version() == gomatrixserverlib.RoomVersionPseudoIDs {
		err := updatePseudoIDs(&ce, se, userIDForSender, format)
		if err != nil {
			return nil, err
		}
	}

	return &ce, nil
}

func updatePseudoIDs(ce *ClientEvent, se gomatrixserverlib.PDU, userIDForSender spec.UserIDForSender, format ClientEventFormat) error {
	ce.SenderKey = se.SenderID()

	userID, err := userIDForSender(se.RoomID(), se.SenderID())
	if err == nil && userID != nil {
		ce.Sender = userID.String()
	}

	sk := se.StateKey()
	if sk != nil && *sk != "" {
		skUserID, err0 := userIDForSender(se.RoomID(), spec.SenderID(*sk))
		if err0 == nil && skUserID != nil {
			skString := skUserID.String()
			ce.StateKey = &skString
		}
	}

	var prev PrevEventRef
	err = json.Unmarshal(se.Unsigned(), &prev)
	if err == nil && prev.PrevSenderID != "" {
		prevUserID, err := userIDForSender(se.RoomID(), spec.SenderID(prev.PrevSenderID))
		if err == nil && userID != nil {
			prev.PrevSenderID = prevUserID.String()
		} else {
			errString := "userID unknown"
			if err != nil {
				errString = err.Error()
			}
			util.Log(context.TODO()).Warn("Failed to find userID for prev_sender in ClientEvent: %s", errString)
			// NOTE: Not much can be done here, so leave the previous value in place.
		}
		ce.Unsigned, err = json.Marshal(prev)
		if err != nil {
			err = fmt.Errorf("failed to marshal unsigned content for ClientEvent: %w", err)
			return err
		}
	}

	switch se.Type() {
	case spec.MRoomCreate:
		updatedContent, err := updateCreateEvent(se.Content(), userIDForSender, se.RoomID())
		if err != nil {
			err = fmt.Errorf("failed to update m.room.create event for ClientEvent: %w", err)
			return err
		}
		ce.Content = updatedContent
	case spec.MRoomMember:
		updatedEvent, err := updateInviteEvent(userIDForSender, se, format)
		if err != nil {
			err = fmt.Errorf("failed to update m.room.member event for ClientEvent: %w", err)
			return err
		}
		if updatedEvent != nil {
			ce.Unsigned = updatedEvent.Unsigned()
		}
	case spec.MRoomPowerLevels:
		updatedEvent, err := updatePowerLevelEvent(userIDForSender, se, format)
		if err != nil {
			err = fmt.Errorf("failed update m.room.power_levels event for ClientEvent: %w", err)
			return err
		}
		if updatedEvent != nil {
			ce.Content = updatedEvent.Content()
			ce.Unsigned = updatedEvent.Unsigned()
		}
	}

	return nil
}

func updateCreateEvent(content json.RawMessage, userIDForSender spec.UserIDForSender, roomID spec.RoomID) (json.RawMessage, error) {
	if creator := gjson.GetBytes(content, "creator"); creator.Exists() {
		oldCreator := creator.Str
		userID, err := userIDForSender(roomID, spec.SenderID(oldCreator))
		if err != nil {
			err = fmt.Errorf("failed to find userID for creator in ClientEvent: %w", err)
			return nil, err
		}

		if userID != nil {
			var newCreatorBytes, newContent []byte
			newCreatorBytes, err = json.Marshal(userID.String())
			if err != nil {
				err = fmt.Errorf("failed to marshal new creator for ClientEvent: %w", err)
				return nil, err
			}

			newContent, err = sjson.SetRawBytes(content, "creator", newCreatorBytes)
			if err != nil {
				err = fmt.Errorf("failed to set new creator for ClientEvent: %w", err)
				return nil, err
			}

			return newContent, nil
		}
	}

	return content, nil
}

func updateInviteEvent(userIDForSender spec.UserIDForSender, ev gomatrixserverlib.PDU, eventFormat ClientEventFormat) (gomatrixserverlib.PDU, error) {
	if inviteRoomState := gjson.GetBytes(ev.Unsigned(), "invite_room_state"); inviteRoomState.Exists() {
		userID, err := userIDForSender(ev.RoomID(), ev.SenderID())
		if err != nil || userID == nil {
			if err != nil {
				err = fmt.Errorf("invalid userID found when updating invite_room_state: %w", err)
			}
			return nil, err
		}

		newState, err := GetUpdatedInviteRoomState(userIDForSender, inviteRoomState, ev, ev.RoomID(), eventFormat)
		if err != nil {
			return nil, err
		}

		var newEv []byte
		newEv, err = sjson.SetRawBytes(ev.JSON(), "unsigned.invite_room_state", newState)
		if err != nil {
			return nil, err
		}

		return gomatrixserverlib.MustGetRoomVersion(ev.Version()).NewEventFromTrustedJSON(newEv, false)
	}

	return ev, nil
}

type InviteRoomStateEvent struct {
	Content  json.RawMessage `json:"content"`
	SenderID string          `json:"sender"`
	StateKey *string         `json:"state_key"`
	Type     string          `json:"type"`
}

func GetUpdatedInviteRoomState(userIDForSender spec.UserIDForSender, inviteRoomState gjson.Result, event gomatrixserverlib.PDU, roomID spec.RoomID, eventFormat ClientEventFormat) (json.RawMessage, error) {
	var res json.RawMessage
	inviteStateEvents := []InviteRoomStateEvent{}
	err := json.Unmarshal([]byte(inviteRoomState.Raw), &inviteStateEvents)
	if err != nil {
		return nil, err
	}

	if event.Version() == gomatrixserverlib.RoomVersionPseudoIDs && eventFormat != FormatSyncFederation {
		for i, ev := range inviteStateEvents {
			userID, userIDErr := userIDForSender(roomID, spec.SenderID(ev.SenderID))
			if userIDErr != nil {
				return nil, userIDErr
			}
			if userID != nil {
				inviteStateEvents[i].SenderID = userID.String()
			}

			if ev.StateKey != nil && *ev.StateKey != "" {
				userID, senderErr := userIDForSender(roomID, spec.SenderID(*ev.StateKey))
				if senderErr != nil {
					return nil, senderErr
				}
				if userID != nil {
					user := userID.String()
					inviteStateEvents[i].StateKey = &user
				}
			}

			updatedContent, updateErr := updateCreateEvent(ev.Content, userIDForSender, roomID)
			if updateErr != nil {
				updateErr = fmt.Errorf("failed to update m.room.create event for ClientEvent: %w", userIDErr)
				return nil, updateErr
			}
			inviteStateEvents[i].Content = updatedContent
		}
	}

	res, err = json.Marshal(inviteStateEvents)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func updatePowerLevelEvent(userIDForSender spec.UserIDForSender, se gomatrixserverlib.PDU, eventFormat ClientEventFormat) (gomatrixserverlib.PDU, error) {
	if !se.StateKeyEquals("") {
		return se, nil
	}

	newEv := se.JSON()

	usersField := gjson.GetBytes(se.JSON(), "content.users")
	if usersField.Exists() {
		pls, err := gomatrixserverlib.NewPowerLevelContentFromEvent(se)
		if err != nil {
			return nil, err
		}

		newPls := make(map[string]int64)
		var userID *spec.UserID
		for user, level := range pls.Users {
			if eventFormat != FormatSyncFederation {
				userID, err = userIDForSender(se.RoomID(), spec.SenderID(user))
				if err != nil {
					return nil, err
				}
				user = userID.String()
			}
			newPls[user] = level
		}

		var newPlBytes []byte
		newPlBytes, err = json.Marshal(newPls)
		if err != nil {
			return nil, err
		}
		newEv, err = sjson.SetRawBytes(se.JSON(), "content.users", newPlBytes)
		if err != nil {
			return nil, err
		}
	}

	// do the same for prev content
	prevUsersField := gjson.GetBytes(se.JSON(), "unsigned.prev_content.users")
	if prevUsersField.Exists() {
		prevContent := gjson.GetBytes(se.JSON(), "unsigned.prev_content")
		if !prevContent.Exists() {
			evNew, err := gomatrixserverlib.MustGetRoomVersion(se.Version()).NewEventFromTrustedJSON(newEv, false)
			if err != nil {
				return nil, err
			}

			return evNew, err
		}
		pls := gomatrixserverlib.PowerLevelContent{}
		err := json.Unmarshal([]byte(prevContent.Raw), &pls)
		if err != nil {
			return nil, err
		}

		newPls := make(map[string]int64)
		for user, level := range pls.Users {
			if eventFormat != FormatSyncFederation {
				userID, userErr := userIDForSender(se.RoomID(), spec.SenderID(user))
				if userErr != nil {
					return nil, userErr
				}
				user = userID.String()
			}
			newPls[user] = level
		}

		var newPlBytes []byte
		newPlBytes, err = json.Marshal(newPls)
		if err != nil {
			return nil, err
		}
		newEv, err = sjson.SetRawBytes(newEv, "unsigned.prev_content.users", newPlBytes)
		if err != nil {
			return nil, err
		}
	}

	evNew, err := gomatrixserverlib.MustGetRoomVersion(se.Version()).NewEventFromTrustedJSONWithEventID(se.EventID(), newEv, false)
	if err != nil {
		return nil, err
	}

	return evNew, err
}
