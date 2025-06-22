package constants

import (
	"github.com/antinvestor/gomatrixserverlib/spec"
	"go.mau.fi/util/base58"
)

const (
	UserID        = "user_id"
	RoomID        = "room_id"
	EventID       = "event_id"
	RoomEventType = "output_room_event_type"

	AppServiceIDToken = "appservice_id_token"
)

func EncodeRoomID(roomID *spec.RoomID) string {
	return base58.Encode([]byte(roomID.String()))
}

func DecodeRoomID(roomID string) (*spec.RoomID, error) {
	decodedStr := base58.Decode(roomID)
	return spec.NewRoomID(string(decodedStr))
}
