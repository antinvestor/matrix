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

func EncodeUserID(userID *spec.UserID) string {
	return base58.Encode([]byte(userID.String()))
}

func DecodeUserID(userID string) (*spec.UserID, error) {
	decodedStr := base58.Decode(userID)
	return spec.NewUserID(string(decodedStr), false)
}

func EncodeRoomID(roomID *spec.RoomID) string {
	return base58.Encode([]byte(roomID.String()))
}

func DecodeRoomID(roomID string) (*spec.RoomID, error) {
	decodedStr := base58.Decode(roomID)
	return spec.NewRoomID(string(decodedStr))
}
