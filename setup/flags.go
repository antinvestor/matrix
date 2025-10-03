// Copyright 2025 Ant Investor Ltd.
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

package setup

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/antinvestor/matrix/internal"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
)

var (
	configPath                            = flag.String("config", "matrix.yaml", "The path to the config file. For more information, see the config file in this repository.")
	version                               = flag.Bool("version", false, "Shows the current version and exits immediately.")
	enableRegistrationWithoutVerification = flag.Bool("really-enable-open-registration", false, "This allows open registration without secondary verification (reCAPTCHA). This is NOT RECOMMENDED and will SIGNIFICANTLY increase the risk that your server will be used to send spam or conduct attacks, which may result in your server being banned from rooms.")
)

// ParseFlags parses the commandline flags and uses them to create a config.
func ParseFlags(ctx context.Context) *config.Matrix {
	flag.Parse()

	if *version {
		fmt.Println(internal.VersionString())
		os.Exit(0)
	}

	log := util.Log(ctx)
	if *configPath == "" {
		log.Fatal("--config must be supplied")
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.WithError(err).Fatal("Invalid config file")
	}

	if cfg.Global.GetOauth2ServiceURI() != "" {

		err = cfg.Global.LoadOauth2Config(ctx)
		if err != nil {
			log.WithError(err).Fatal("failed to load oauth2 settings")
		}
	}

	if *enableRegistrationWithoutVerification {
		cfg.ClientAPI.OpenRegistrationWithoutVerificationEnabled = true
	}

	return cfg
}
