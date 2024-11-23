// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

type RelayAPI struct {
	Matrix *Global `yaml:"-"`

	// The database stores information used by the relay queue to
	// forward transactions to remote servers.
	Database DatabaseOptions `yaml:"database,omitempty"`
}

func (c *RelayAPI) Defaults(opts DefaultOpts) {
	c.Database.ConnectionString = opts.DatabaseConnectionStr
}

func (c *RelayAPI) Verify(configErrs *ConfigErrors) {
	if c.Database.ConnectionString == "" {
		checkNotEmpty(configErrs, "relay_api.database.connection_string", string(c.Database.ConnectionString))
	}
}
