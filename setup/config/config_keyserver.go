package config

type KeyServer struct {
	Matrix *Global `yaml:"-"`

	Database DatabaseOptions `yaml:"database,omitempty"`
}

func (c *KeyServer) Defaults(opts DefaultOpts) {
	c.Database.ConnectionString = opts.DatabaseConnectionStr
}

func (c *KeyServer) Verify(configErrs *ConfigErrors) {
	if c.Database.ConnectionString == "" {
		checkNotEmpty(configErrs, "key_server.database.connection_string", string(c.Database.ConnectionString))
	}
}
