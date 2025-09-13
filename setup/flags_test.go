package setup

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFlags_DatabaseURIPrecedence(t *testing.T) {
	tests := []struct {
		name         string
		configFileDB string
		envVarDB     string
		expectedDB   string
		description  string
	}{
		{
			name:         "env_var_overrides_config_file",
			configFileDB: "postgres://config:password@config-host/config-db?sslmode=disable",
			envVarDB:     "postgres://env:password@env-host/env-db?sslmode=disable",
			expectedDB:   "postgres://env:password@env-host/env-db?sslmode=disable",
			description:  "Environment variable should take precedence over config file",
		},
		{
			name:         "config_file_used_when_no_env_var",
			configFileDB: "postgres://config:password@config-host/config-db?sslmode=disable",
			envVarDB:     "",
			expectedDB:   "postgres://config:password@config-host/config-db?sslmode=disable",
			description:  "Config file value should be used when no environment variable is set",
		},
		{
			name:         "env_var_used_when_config_file_empty",
			configFileDB: "",
			envVarDB:     "postgres://env:password@env-host/env-db?sslmode=disable",
			expectedDB:   "postgres://env:password@env-host/env-db?sslmode=disable",
			description:  "Environment variable should be used when config file has empty database URI",
		},
		{
			name:         "both_empty_results_in_empty",
			configFileDB: "",
			envVarDB:     "",
			expectedDB:   "",
			description:  "Both empty should result in empty database URI",
		},
		{
			name:         "env_var_overrides_with_different_protocols",
			configFileDB: "postgres://config:password@config-host/config-db?sslmode=disable",
			envVarDB:     "postgres://env:different@env-host/env-db?sslmode=require",
			expectedDB:   "postgres://env:different@env-host/env-db?sslmode=require",
			description:  "Environment variable with different connection parameters should override config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original environment
			originalEnv := os.Getenv("DATABASE_URL")
			defer func() {
				if originalEnv != "" {
					os.Setenv("DATABASE_URL", originalEnv)
				} else {
					os.Unsetenv("DATABASE_URL")
				}
			}()

			// Create temporary config file
			configContent := createTestConfig(tt.configFileDB)
			configFile := createTempConfigFile(t, configContent)
			defer os.Remove(configFile)

			// Set environment variable
			if tt.envVarDB != "" {
				err := os.Setenv("DATABASE_URL", tt.envVarDB)
				require.NoError(t, err)
			} else {
				os.Unsetenv("DATABASE_URL")
			}

			// Reset flags for testing
			resetFlags()

			// Set config path flag
			err := flag.Set("config", configFile)
			require.NoError(t, err)

			// Call ParseFlags
			ctx := context.Background()
			cfg := ParseFlags(ctx)

			// Verify the database URI
			var actualDB string
			if len(cfg.Global.DatabasePrimaryURL) > 0 {
				actualDB = cfg.Global.DatabasePrimaryURL[0]
			}
			assert.Equal(t, tt.expectedDB, actualDB, tt.description)
		})
	}
}

func TestParseFlags_CacheURIPrecedence(t *testing.T) {
	tests := []struct {
		name            string
		configFileCache string
		envVarCache     string
		expectedCache   string
		description     string
	}{
		{
			name:            "env_var_overrides_config_cache",
			configFileCache: "redis://config:password@config-cache:6379/0",
			envVarCache:     "redis://env:password@env-cache:6379/1",
			expectedCache:   "redis://env:password@env-cache:6379/1",
			description:     "Environment variable should take precedence over config file for cache URI",
		},
		{
			name:            "config_cache_used_when_no_env_var",
			configFileCache: "redis://config:password@config-cache:6379/0",
			envVarCache:     "",
			expectedCache:   "redis://config:password@config-cache:6379/0",
			description:     "Config file cache URI should be used when no environment variable is set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original environment
			originalEnv := os.Getenv("CACHE_URI")
			defer func() {
				if originalEnv != "" {
					os.Setenv("CACHE_URI", originalEnv)
				} else {
					os.Unsetenv("CACHE_URI")
				}
			}()

			// Create temporary config file
			configContent := createTestConfigWithCache(tt.configFileCache)
			configFile := createTempConfigFile(t, configContent)
			defer os.Remove(configFile)

			// Set environment variable
			if tt.envVarCache != "" {
				err := os.Setenv("CACHE_URI", tt.envVarCache)
				require.NoError(t, err)
			} else {
				os.Unsetenv("CACHE_URI")
			}

			// Reset flags for testing
			resetFlags()

			// Set config path flag
			err := flag.Set("config", configFile)
			require.NoError(t, err)

			// Call ParseFlags
			ctx := context.Background()
			cfg := ParseFlags(ctx)

			// Verify the cache URI
			assert.Equal(t, tt.expectedCache, string(cfg.Global.Cache.CacheURI), tt.description)
		})
	}
}

func TestParseFlags_MultipleEnvironmentVariables(t *testing.T) {
	// Save original environment
	originalDBEnv := os.Getenv("DATABASE_URL")
	originalCacheEnv := os.Getenv("CACHE_URI")
	originalQueueEnv := os.Getenv("QUEUE_URI")
	defer func() {
		restoreEnv("DATABASE_URL", originalDBEnv)
		restoreEnv("CACHE_URI", originalCacheEnv)
		restoreEnv("QUEUE_URI", originalQueueEnv)
	}()

	// Create config with all URIs
	configContent := `
version: 2
global:
  server_name: test.localhost
  private_key: matrix_key.pem
  database:
    database_uri: postgres://config:password@config-db/config?sslmode=disable
  cache:
    cache_uri: redis://config:password@config-cache:6379/0
  queue:
    queue_uri: nats://config-queue:4222
`

	configFile := createTempConfigFile(t, configContent)
	defer os.Remove(configFile)

	// Set all environment variables
	err := os.Setenv("DATABASE_URL", "postgres://env:password@env-db/env?sslmode=disable")
	require.NoError(t, err)
	err = os.Setenv("CACHE_URI", "redis://env:password@env-cache:6379/1")
	require.NoError(t, err)
	err = os.Setenv("QUEUE_URI", "nats://env-queue:4222")
	require.NoError(t, err)

	// Reset flags for testing
	resetFlags()

	// Set config path flag
	err = flag.Set("config", configFile)
	require.NoError(t, err)

	// Call ParseFlags
	ctx := context.Background()
	cfg := ParseFlags(ctx)

	// Verify all URIs use environment variables
	var actualDB string
	if len(cfg.Global.DatabasePrimaryURL) > 0 {
		actualDB = cfg.Global.DatabasePrimaryURL[0]
	}
	assert.Equal(t, "postgres://env:password@env-db/env?sslmode=disable", actualDB)
	assert.Equal(t, "redis://env:password@env-cache:6379/1", string(cfg.Global.Cache.CacheURI))
	assert.Equal(t, "nats://env-queue:4222", string(cfg.Global.Queue.DS))
}

func TestParseFlags_RegistrationFlag(t *testing.T) {
	// Create minimal config
	configContent := createMinimalTestConfig()
	configFile := createTempConfigFile(t, configContent)
	defer os.Remove(configFile)

	tests := []struct {
		name                     string
		setRegistrationFlag      bool
		expectedRegistrationOpen bool
	}{
		{
			name:                     "registration_flag_enables_open_registration",
			setRegistrationFlag:      true,
			expectedRegistrationOpen: true,
		},
		{
			name:                     "no_registration_flag_keeps_default",
			setRegistrationFlag:      false,
			expectedRegistrationOpen: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset flags for testing
			resetFlags()

			// Set config path flag
			err := flag.Set("config", configFile)
			require.NoError(t, err)

			// Set registration flag if needed
			if tt.setRegistrationFlag {
				err = flag.Set("really-enable-open-registration", "true")
				require.NoError(t, err)
			}

			// Call ParseFlags
			ctx := context.Background()
			cfg := ParseFlags(ctx)

			// Verify registration setting
			assert.Equal(t, tt.expectedRegistrationOpen, cfg.ClientAPI.OpenRegistrationWithoutVerificationEnabled)
		})
	}
}

// Helper functions

func createTestConfig(databaseURI string) string {
	return createTestConfigWithPrivateKey(databaseURI, "matrix_key.pem")
}

func createTestConfigWithPrivateKey(databaseURI, privateKeyPath string) string {
	config := `
version: 2
global:
  server_name: test.localhost
  private_key: ` + privateKeyPath
	if databaseURI != "" {
		config += `
  embedded_config:
    database_url: 
      - ` + databaseURI
	}
	config += `
client_api:
  registration_disabled: true
`
	return config
}

func createTestConfigWithCache(cacheURI string) string {
	return `
version: 2
global:
  server_name: test.localhost
  private_key: matrix_key.pem
  cache:
    cache_uri: ` + cacheURI + `
client_api:
  registration_disabled: true
`
}

func createMinimalTestConfig() string {
	return `
version: 2
global:
  server_name: test.localhost
  private_key: matrix_key.pem
client_api:
  registration_disabled: true
`
}

func createTempConfigFile(t *testing.T, content string) string {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "test_config.yaml")

	// Create a temporary private key file
	privateKeyFile := filepath.Join(tmpDir, "matrix_key.pem")
	privateKeyContent := `-----BEGIN MATRIX PRIVATE KEY-----
Key-ID: ed25519:test
MCowBQYDK2VwAyEAGb9ECWmEzf6FQbrBb21E9CRhxtfLOWJYeGrTMuF0UG4=
-----END MATRIX PRIVATE KEY-----`

	err := os.WriteFile(privateKeyFile, []byte(privateKeyContent), 0644)
	require.NoError(t, err)

	// Replace relative path with absolute path in config content
	if content != "" {
		// Replace the private_key path in the config content with absolute path
		content = strings.ReplaceAll(content, "private_key: matrix_key.pem", "private_key: "+privateKeyFile)
	}

	err = os.WriteFile(configFile, []byte(content), 0644)
	require.NoError(t, err)

	return configFile
}

func resetFlags() {
	// Reset flag values to defaults
	flag.Set("config", "matrix.yaml")
	flag.Set("version", "false")
	flag.Set("really-enable-open-registration", "false")
}

func restoreEnv(key, value string) {
	if value != "" {
		os.Setenv(key, value)
	} else {
		os.Unsetenv(key)
	}
}
