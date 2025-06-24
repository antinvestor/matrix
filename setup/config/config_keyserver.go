package config

import (
	"github.com/antinvestor/matrix/setup/constants"
)

type KeyServer struct {
	Global *Global `yaml:"-"`

	Database DatabaseOptions `yaml:"database,omitempty"`

	Queues KeyServerQueues `yaml:"queues"`
}

func (c *KeyServer) Defaults(opts DefaultOpts) {
	c.Database.Reference = "KeyServer"
	c.Database.Prefix = opts.RandomnessPrefix
	c.Database.DatabaseURI = opts.DSDatabaseConn
	c.Queues.Defaults(opts)
}

func (c *KeyServer) Verify(configErrs *Errors) {
	if c.Database.DatabaseURI == "" {
		checkNotEmpty(configErrs, "key_server.database.database_uri", string(c.Database.DatabaseURI))
	}
}

type KeyServerQueues struct {

	// durable - FederationAPIKeyChangeConsumer
	OutputKeyChangeEvent QueueOptions `yaml:"output_key_change_event"`
}

func (q *KeyServerQueues) Defaults(opts DefaultOpts) {
	q.OutputKeyChangeEvent = opts.defaultQ(constants.OutputKeyChangeEvent)
}

func (q *KeyServerQueues) Verify(configErrs *Errors) {
	checkNotEmpty(configErrs, "key_server.queues.output_key_change_event", string(q.OutputKeyChangeEvent.DS))
}
