package config

import (
	"fmt"
	"os"
	"strings"
)

type JetStream struct {
	Matrix *Global `yaml:"-"`

	// A list of NATS addresses to connect to. If none are specified, an
	// internal NATS server will be used when running in monolith mode only.
	Addresses []string `yaml:"addresses"`
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

func (c *JetStream) LoadEnv() {
	queueUriStr := os.Getenv("QUEUE_URI")
	if queueUriStr != "" {

		connectionUris := strings.Split(queueUriStr, ",")
		for _, natsUri := range connectionUris {
			dataSourceUri := DataSource(natsUri)
			if dataSourceUri.IsNats() {
				c.Addresses = append(c.Addresses, natsUri)
			}
		}
	}

}

func (c *JetStream) Defaults(opts DefaultOpts) {

	c.TopicPrefix = "Matrix"
	c.Credentials = ""

	connectionUriStr := opts.QueueConnectionStr
	connectionUris := strings.Split(connectionUriStr, ",")
	for _, natsUri := range connectionUris {
		dataSourceUri := DataSource(natsUri)
		if dataSourceUri.IsNats() {
			c.Addresses = append(c.Addresses, natsUri)
		}
	}

}

func (c *JetStream) Verify(configErrs *ConfigErrors) {
	checkNotEmpty(configErrs, "global.jetstream.addresses", strings.Join(c.Addresses, ","))
}
