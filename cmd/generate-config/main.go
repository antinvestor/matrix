package main

import (
	"flag"
	"fmt"
	"path/filepath"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/setup/config"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

func main() {
	defaultsForCI := flag.Bool("ci", false, "Populate the configuration with sane defaults for use in CI")
	serverName := flag.String("server", "", "The domain name of the server if not 'localhost'")
	databaseURI := flag.String("database_uri", "", "The Cm URI to use for all components (PostgreSQL only)")
	cacheURI := flag.String("cache_uri", "", "The Cache URI to use for all components")
	queueURI := flag.String("queue_uri", "", "The Queue URI to use for all components")
	dirPath := flag.String("dir", "./", "The folder to use for paths ( media storage)")
	normalise := flag.String("normalise", "", "Normalise an existing configuration file by adding new/missing options and defaults")
	flag.Parse()

	var cfg *config.Matrix
	if *normalise == "" {
		cfg = &config.Matrix{
			Version: config.Version,
		}
		cfg.Defaults(config.DefaultOpts{
			DSDatabaseConn: config.DataSource(*databaseURI),
			DSCacheConn:    config.DataSource(*cacheURI),
			DSQueueConn:    config.DataSource(*queueURI),
		})
		if *serverName != "" {
			cfg.Global.ServerName = spec.ServerName(*serverName)
		}

		cfg.MediaAPI.BasePath = config.Path(filepath.Join(*dirPath, "media"))
		cfg.SyncAPI.Fulltext.IndexPath = config.Path(filepath.Join(*dirPath, "searchindex"))

		if *defaultsForCI {
			cfg.AppServiceAPI.DisableTLSValidation = true
			cfg.ClientAPI.RateLimiting.Enabled = false
			cfg.FederationAPI.DisableTLSValidation = false
			cfg.FederationAPI.DisableHTTPKeepalives = true
			// don't hit matrix.org when running tests!!!
			cfg.FederationAPI.KeyPerspectives = config.KeyPerspectives{}
			cfg.MediaAPI.BasePath = config.Path(filepath.Join(*dirPath, "media"))
			cfg.MSCs.MSCs = []string{"msc2836", "msc2444", "msc2753"}
			cfg.UserAPI.BCryptCost = bcrypt.MinCost
			cfg.ClientAPI.RegistrationDisabled = false
			cfg.ClientAPI.OpenRegistrationWithoutVerificationEnabled = true
			cfg.ClientAPI.RegistrationSharedSecret = "complement"
			cfg.Global.Presence = config.PresenceOptions{
				EnableInbound:  true,
				EnableOutbound: true,
			}
			cfg.SyncAPI.Fulltext = config.Fulltext{
				Enabled:   true,
				IndexPath: config.Path(filepath.Join(*dirPath, "searchindex")),
				InMemory:  true,
				Language:  "en",
			}
		}
	} else {
		var err error
		if cfg, err = config.Load(*normalise); err != nil {
			panic(err)
		}
	}

	j, err := yaml.Marshal(cfg)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(j))
}
