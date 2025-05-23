package config

type SyncAPI struct {
	Global *Global `yaml:"-"`

	Database DatabaseOptions `yaml:"database,omitempty"`

	RealIPHeader string `yaml:"real_ip_header"`

	Fulltext Fulltext `yaml:"search"`

	Queues SyncQueues `yaml:"queues"`
}

func (c *SyncAPI) Defaults(opts DefaultOpts) {
	c.Fulltext.Defaults(opts)
	c.Database.ConnectionString = opts.DatabaseConnectionStr
	c.Queues.Defaults(opts)
}

func (c *SyncAPI) Verify(configErrs *ConfigErrors) {
	c.Fulltext.Verify(configErrs)
	if c.Database.ConnectionString == "" {
		checkNotEmpty(configErrs, "sync_api.database", string(c.Database.ConnectionString))
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

func (f *Fulltext) Verify(configErrs *ConfigErrors) {
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

	q.OutputRoomEvent = QueueOptions{Prefix: opts.QueuePrefix, QReference: "SyncAPIRoomServerConsumer", DS: "mem://SyncAPIOutputRoomEvent"}
	q.OutputClientData = QueueOptions{Prefix: opts.QueuePrefix, QReference: "SyncAPIAccountDataConsumer", DS: "mem://SyncAPIOutputClientData"}
	q.OutputKeyChangeEvent = QueueOptions{Prefix: opts.QueuePrefix, QReference: "SyncAPIKeyChangeConsumer", DS: "mem://SyncAPIOutputKeyChangeEvent"}
	q.OutputSendToDeviceEvent = QueueOptions{Prefix: opts.QueuePrefix, QReference: "SyncAPISendToDeviceConsumer", DS: "mem://SyncAPIOutputSendToDeviceEvent"}
	q.OutputTypingEvent = QueueOptions{Prefix: opts.QueuePrefix, QReference: "SyncAPITypingConsumer", DS: "mem://SyncAPIOutputTypingEvent"}
	q.OutputReceiptEvent = QueueOptions{Prefix: opts.QueuePrefix, QReference: "SyncAPIReceiptConsumer", DS: "mem://SyncAPIOutputReceiptEvent"}
	q.OutputStreamEvent = QueueOptions{Prefix: opts.QueuePrefix, QReference: "SyncAPIRoomServerConsumer", DS: "mem://SyncAPIOutputStreamEvent"}
	q.OutputNotificationData = QueueOptions{Prefix: opts.QueuePrefix, QReference: "SyncAPINotificationDataConsumer", DS: "mem://SyncAPIOutputNotificationData"}
	q.OutputPresenceEvent = QueueOptions{Prefix: opts.QueuePrefix, QReference: "SyncAPIPresenceConsumer", DS: "mem://SyncAPIOutputPresenceEvent"}
}

func (q *SyncQueues) Verify(configErrs *ConfigErrors) {

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
