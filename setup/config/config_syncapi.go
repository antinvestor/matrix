package config

import (
	"fmt"

	"github.com/antinvestor/matrix/setup/constants"
)

type SyncAPI struct {
	Global *Global `yaml:"-"`

	Database DatabaseOptions `yaml:"database,omitempty"`

	RealIPHeader string `yaml:"real_ip_header"`

	Fulltext Fulltext `yaml:"search"`

	Queues SyncQueues `yaml:"queues"`
}

func (c *SyncAPI) Defaults(opts DefaultOpts) {
	c.Fulltext.Defaults(opts)
	c.Database.Reference = "SyncAPI"
	c.Database.Prefix = opts.RandomnessPrefix
	c.Database.DatabaseURI = opts.DSDatabaseConn
	c.Queues.Defaults(opts)
}

func (c *SyncAPI) Verify(configErrs *Errors) {
	c.Fulltext.Verify(configErrs)
	if c.Database.DatabaseURI == "" {
		checkNotEmpty(configErrs, "sync_api.database", string(c.Database.DatabaseURI))
	}
}

type Fulltext struct {
	Enabled   bool   `yaml:"enabled"`
	IndexPath Path   `yaml:"index_path"`
	InMemory  bool   `yaml:"in_memory"` // only useful in tests
	Language  string `yaml:"language"`  // the language to use when analysing content
}

func (f *Fulltext) Defaults(opts DefaultOpts) {
	f.Enabled = false
	f.IndexPath = "./searchindex"
	f.Language = "en"
}

func (f *Fulltext) Verify(configErrs *Errors) {
	if !f.Enabled {
		return
	}
	checkNotEmpty(configErrs, "syncapi.search.index_path", string(f.IndexPath))
	checkNotEmpty(configErrs, "syncapi.search.language", f.Language)
}

type SyncQueues struct {

	// durable - "SyncAPIAccountDataConsumer"
	OutputClientData     QueueOptions `yaml:"output_client_data"`
	InputFulltextReindex QueueOptions `yaml:"input_fulltext_reindex"`
	// durable - "SyncAPIKeyChangeConsumer"
	OutputKeyChangeEvent QueueOptions `yaml:"output_key_change_event"`
	// durable - "SyncAPIRoomServerConsumer"
	OutputRoomEvent QueueOptions `yaml:"output_room_event"`

	// durable - "SyncAPISendToDeviceConsumer"
	OutputSendToDeviceEvent QueueOptions `yaml:"output_send_to_device_event"`
	// durable - "SyncAPITypingConsumer"
	OutputTypingEvent QueueOptions `yaml:"output_typing_event"`
	// durable - "SyncAPIReceiptConsumer"
	OutputReceiptEvent QueueOptions `yaml:"output_receipt_event"`
	// durable - "SyncAPIRoomServerConsumer"
	OutputStreamEvent QueueOptions `yaml:"output_stream_event"`

	// durable - SyncAPINotificationDataConsumer
	OutputNotificationData QueueOptions `yaml:"output_notification_data"`

	// durable - SyncAPIPresenceConsumer
	OutputPresenceEvent QueueOptions `yaml:"output_presence_event"`
}

func (q *SyncQueues) Defaults(opts DefaultOpts) {

	q.OutputRoomEvent = opts.defaultQ(constants.OutputRoomEvent,
		KVOpt{K: "stream_retention", V: "interest"},
		KVOpt{K: "stream_subjects", V: fmt.Sprintf("%s.*", constants.OutputRoomEvent)},
		KVOpt{K: "consumer_filter_subject", V: fmt.Sprintf("%s.*", constants.OutputRoomEvent)},
		KVOpt{K: "consumer_durable_name", V: "CnsDurable_SyncAPIOutputRoomEvent"},
		KVOpt{K: "consumer_headers_only", V: "true"},
		KVOpt{K: constants.QueueHeaderToExtendSubject, V: constants.RoomID})
	q.OutputRoomEvent.QReference = fmt.Sprintf("SyncAPI_%s", q.OutputRoomEvent.QReference)

	q.OutputClientData = opts.defaultQ(constants.OutputClientData, KVOpt{K: "consumer_durable_name", V: "CnsDurable_SyncAPIOutputClientDataEvent"})
	q.OutputKeyChangeEvent = opts.defaultQ(constants.OutputKeyChangeEvent, KVOpt{K: "consumer_durable_name", V: "CnsDurable_SyncAPIOutputKeyChangeEvent"})
	q.OutputSendToDeviceEvent = opts.defaultQ(constants.OutputSendToDeviceEvent, KVOpt{K: "consumer_durable_name", V: "CnsDurable_SyncAPIOutputSendToDeviceEvent"})

	q.OutputTypingEvent = opts.defaultQ(constants.OutputTypingEvent, KVOpt{K: "stream_storage", V: "memory"}, KVOpt{K: "consumer_durable_name", V: "CnsDurable_SyncAPIOutputTypingEvent"})

	q.OutputReceiptEvent = opts.defaultQ(constants.OutputReceiptEvent, KVOpt{K: "consumer_durable_name", V: "CnsDurable_SyncAPIOutputReceiptEvent"})
	q.OutputStreamEvent = opts.defaultQ(constants.OutputStreamEvent, KVOpt{K: "consumer_durable_name", V: "CnsDurable_SyncAPIOutputStreamEvent"})
	q.OutputNotificationData = opts.defaultQ(constants.OutputNotificationData, KVOpt{K: "consumer_durable_name", V: "CnsDurable_SyncAPIOutputNotificationEvent"})
	q.OutputPresenceEvent = opts.defaultQ(constants.OutputPresenceEvent, KVOpt{K: "consumer_durable_name", V: "CnsDurable_SyncAPIOutputPresenceEvent"})
}

func (q *SyncQueues) Verify(configErrs *Errors) {

	checkNotEmpty(configErrs, "syncapi.queues.output_room_event", string(q.OutputRoomEvent.DS))
	checkNotEmpty(configErrs, "syncapi.queues.output_client_data", string(q.OutputClientData.DS))
	checkNotEmpty(configErrs, "syncapi.queues.output_key_change_event", string(q.OutputKeyChangeEvent.DS))
	checkNotEmpty(configErrs, "syncapi.queues.output_send_to_device_event", string(q.OutputSendToDeviceEvent.DS))
	checkNotEmpty(configErrs, "syncapi.queues.output_typing_event", string(q.OutputTypingEvent.DS))
	checkNotEmpty(configErrs, "syncapi.queues.output_receipt_event", string(q.OutputReceiptEvent.DS))
	checkNotEmpty(configErrs, "syncapi.queues.output_stream_event", string(q.OutputStreamEvent.DS))
	checkNotEmpty(configErrs, "syncapi.queues.output_notification_data", string(q.OutputNotificationData.DS))
	checkNotEmpty(configErrs, "syncapi.queues.output_presence_event", string(q.OutputPresenceEvent.DS))
}
