package constants

const (
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

// Constants related to extending a subject via headers
const (
	QueueHeaderToExtendSubject = "header_to_extended_subject"
	SynchronousReplyMsgID      = "sync_reply_msg_id"
)
