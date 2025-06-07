package config

type KeyServer struct {
	Global *Global `yaml:"-"`

	Database DatabaseOptions `yaml:"database,omitempty"`

	Queues KeyServerQueues `yaml:"queues"`
}

func (c *KeyServer) Defaults(opts DefaultOpts) {
	c.Database.ConnectionString = opts.DSDatabaseConn
	c.Queues.Defaults(opts)
}

func (c *KeyServer) Verify(configErrs *ConfigErrors) {
	if c.Database.ConnectionString == "" {
		checkNotEmpty(configErrs, "key_server.database.connection_string", string(c.Database.ConnectionString))
	}
}

type KeyServerQueues struct {

	// durable - FederationAPIKeyChangeConsumer
	OutputKeyChangeEvent QueueOptions `yaml:"output_key_change_event"`
}

func (q *KeyServerQueues) Defaults(opts DefaultOpts) {
	q.OutputKeyChangeEvent = QueueOptions{Prefix: opts.QueuePrefix, QReference: "KeyServerKeyChangeConsumer", DS: opts.DSQueueConn.ExtendPath(OutputKeyChangeEvent).ExtendQuery("stream_name", OutputKeyChangeEvent)}
}

func (q *KeyServerQueues) Verify(configErrs *ConfigErrors) {
	checkNotEmpty(configErrs, "key_server.queues.output_key_change_event", string(q.OutputKeyChangeEvent.DS))
}
