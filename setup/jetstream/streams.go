package jetstream

import (
	"regexp"
)

const (
	UserID        = "user_id"
	RoomID        = "room_id"
	EventID       = "event_id"
	RoomEventType = "output_room_event_type"

	AppServiceIDToken = "appservice_id_token"
)

var (
	InputRoomEvent          = "InputRoomEvent"
	InputDeviceListUpdate   = "InputDeviceListUpdate"
	InputSigningKeyUpdate   = "InputSigningKeyUpdate"
	OutputRoomEvent         = "OutputRoomEvent"
	OutputAppserviceEvent   = "OutputAppserviceEvent"
	OutputSendToDeviceEvent = "OutputSendToDeviceEvent"
	OutputKeyChangeEvent    = "OutputKeyChangeEvent"
	OutputTypingEvent       = "OutputTypingEvent"
	OutputClientData        = "OutputClientData"
	OutputNotificationData  = "OutputNotificationData"
	OutputReceiptEvent      = "OutputReceiptEvent"
	OutputStreamEvent       = "OutputStreamEvent"
	OutputReadUpdate        = "OutputReadUpdate"
	RequestPresence         = "GetPresence"
	OutputPresenceEvent     = "OutputPresenceEvent"
	InputFulltextReindex    = "InputFulltextReindex"
)

var safeCharacters = regexp.MustCompile("[^A-Za-z0-9$]+")

func Tokenise(str string) string {
	return safeCharacters.ReplaceAllString(str, "_")
}
