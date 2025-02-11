package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/pitabwire/frame"

	log "github.com/sirupsen/logrus"
)

type JetStream struct {

	// A list of NATS addresses to connect to. If none are specified, an
	// internal NATS server will be used when running in monolith mode only.
	Addresses []string `env:"QUEUE_URI" yaml:"addresses"`
	// The prefix to use for stream names for this homeserver - really only
	// useful if running more than one Dendrite on the same NATS deployment.
	TopicPrefix string `yaml:"topic_prefix"`

	// A credentials file to be used for authentication, example:
	// https://docs.nats.io/using-nats/developer/connecting/creds
	Credentials Path `yaml:"credentials_path"`
}

func (c *JetStream) Prefixed(name string) string {
	return fmt.Sprintf("%s%s", c.TopicPrefix, name)
}

func (c *JetStream) Durable(name string) string {
	return c.Prefixed(name)
}

func (c *JetStream) LoadEnv() error {

	err := frame.ConfigFillFromEnv(c)
	if err != nil {
		queueUriStr := os.Getenv("QUEUE_URI")
		c.Addresses = strings.Split(queueUriStr, ",")

	}

	for _, natsUri := range c.Addresses {
		dataSourceUri := DataSource(natsUri)
		if !dataSourceUri.IsNats() {
			log.WithField("queue_uri", natsUri).Warn("Invalid queue uri in the config")
		}
	}
	return nil
}

func (c *JetStream) Defaults(opts DefaultOpts) {

	c.TopicPrefix = "Matrix"
	c.Credentials = ""

	connectionUris := opts.QueueConnectionStr.ToArray()
	for _, ds := range connectionUris {
		if ds.IsNats() {
			c.Addresses = append(c.Addresses, string(ds))
		}
	}

}

func (c *JetStream) Verify(configErrs *ConfigErrors) {
	checkNotEmpty(configErrs, "global.jetstream.addresses", strings.Join(c.Addresses, ","))
}
