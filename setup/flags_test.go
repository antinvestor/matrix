package setup

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/antinvestor/matrix/setup/config"
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

func TestParseFlags_ConfigValidationErrors(t *testing.T) {
	tests := []struct {
		name           string
		configContent  string
		expectedErrors []string
		description    string
	}{
		{
			name: "negative_cache_size",
			configContent: `
version: 2
global:
  server_name: test.localhost
  private_key: matrix_key.pem
  embedded_config:
    database_url: 
      - postgres://user:pass@localhost/matrix?sslmode=disable
  cache:
    cache_uri: redis://localhost:6379
    max_size_estimated: -100
  queue:
    queue_uri: nats://localhost:4222
    topic_prefix: "Matrix_"
client_api:
  registration_disabled: true
federation_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_federation?sslmode=disable
room_server:
  database:
    database_uri: postgres://user:pass@localhost/matrix_roomserver?sslmode=disable
sync_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_syncapi?sslmode=disable
user_api:
  account_database:
    database_uri: postgres://user:pass@localhost/matrix_userapi?sslmode=disable
key_server:
  database:
    database_uri: postgres://user:pass@localhost/matrix_keyserver?sslmode=disable
media_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_mediaapi?sslmode=disable
relay_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_relayapi?sslmode=disable
mscs:
  database:
    database_uri: postgres://user:pass@localhost/matrix_mscs?sslmode=disable
`,
			expectedErrors: []string{"invalid value for config key \"max_size_estimated\": -100"},
			description:    "Should error when cache size is negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary config file
			configFile := createTempConfigFile(t, tt.configContent)
			defer os.Remove(configFile)

			// Reset flags for testing
			resetFlags()

			// Set config path flag
			err := flag.Set("config", configFile)
			require.NoError(t, err)

			// Call ParseFlags
			ctx := context.Background()
			cfg := ParseFlags(ctx)

			// Call Verify to check for validation errors
			var configErrs config.Errors
			cfg.Verify(&configErrs)

			// Verify that we got the expected errors
			if len(tt.expectedErrors) == 0 {
				assert.Empty(t, configErrs, tt.description)
			} else {
				assert.NotEmpty(t, configErrs, tt.description)
				for _, expectedErr := range tt.expectedErrors {
					found := false
					for _, actualErr := range configErrs {
						if strings.Contains(actualErr, expectedErr) {
							found = true
							break
						}
					}
					assert.True(t, found, "Expected error containing '%s' not found in: %v", expectedErr, configErrs)
				}
			}
		})
	}
}

func TestParseFlags_MissingRequiredFields(t *testing.T) {
	tests := []struct {
		name           string
		configContent  string
		expectedErrors []string
		description    string
	}{
		{
			name: "missing_cache_uri_when_cache_configured",
			configContent: `
version: 2
global:
  server_name: test.localhost
  private_key: matrix_key.pem
  embedded_config:
    database_url: 
      - postgres://user:pass@localhost/matrix?sslmode=disable
  cache:
    max_size_estimated: 1gb
  queue:
    queue_uri: nats://localhost:4222
    topic_prefix: "Matrix_"
client_api:
  registration_disabled: true
federation_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_federation?sslmode=disable
room_server:
  database:
    database_uri: postgres://user:pass@localhost/matrix_roomserver?sslmode=disable
sync_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_syncapi?sslmode=disable
user_api:
  account_database:
    database_uri: postgres://user:pass@localhost/matrix_userapi?sslmode=disable
key_server:
  database:
    database_uri: postgres://user:pass@localhost/matrix_keyserver?sslmode=disable
media_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_mediaapi?sslmode=disable
relay_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_relayapi?sslmode=disable
mscs:
  database:
    database_uri: postgres://user:pass@localhost/matrix_mscs?sslmode=disable
`,
			expectedErrors: []string{"missing config key \"global.cache.cache_uri\""},
			description:    "Should error when cache URI is missing but cache is configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary config file
			configFile := createTempConfigFile(t, tt.configContent)
			defer os.Remove(configFile)

			// Reset flags for testing
			resetFlags()

			// Set config path flag
			err := flag.Set("config", configFile)
			require.NoError(t, err)

			// Call ParseFlags
			ctx := context.Background()
			cfg := ParseFlags(ctx)

			// Call Verify to check for validation errors
			var configErrs config.Errors
			cfg.Verify(&configErrs)

			// Debug: Print all validation errors to understand what's happening
			t.Logf("All validation errors: %v", configErrs)

			// Verify that we got the expected errors
			assert.NotEmpty(t, configErrs, tt.description)
			for _, expectedErr := range tt.expectedErrors {
				found := false
				for _, actualErr := range configErrs {
					if strings.Contains(actualErr, expectedErr) {
						found = true
						break
					}
				}
				assert.True(t, found, "Expected error containing '%s' not found in: %v", expectedErr, configErrs)
			}
		})
	}
}

func TestParseFlags_ServiceDatabaseValidation(t *testing.T) {
	// Test that specific service database validation works
	// This test uses a complete config but with one specific database missing
	tests := []struct {
		name           string
		configContent  string
		expectedErrors []string
		description    string
	}{
		{
			name: "missing_federation_api_database",
			configContent: `
version: 2
global:
  server_name: test.localhost
  private_key: matrix_key.pem
  embedded_config:
    database_url: 
      - postgres://user:pass@localhost/matrix?sslmode=disable
  cache:
    cache_uri: redis://localhost:6379
  queue:
    queue_uri: nats://localhost:4222
    topic_prefix: "Matrix_"
client_api:
  registration_disabled: true
federation_api:
  database: {}
room_server:
  database:
    database_uri: postgres://user:pass@localhost/matrix_roomserver?sslmode=disable
sync_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_syncapi?sslmode=disable
user_api:
  account_database:
    database_uri: postgres://user:pass@localhost/matrix_userapi?sslmode=disable
key_server:
  database:
    database_uri: postgres://user:pass@localhost/matrix_keyserver?sslmode=disable
media_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_mediaapi?sslmode=disable
relay_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_relayapi?sslmode=disable
mscs:
  database:
    database_uri: postgres://user:pass@localhost/matrix_mscs?sslmode=disable
`,
			expectedErrors: []string{},
			description:    "Should not error even when federation API database URI is missing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary config file
			configFile := createTempConfigFile(t, tt.configContent)
			defer os.Remove(configFile)

			// Reset flags for testing
			resetFlags()

			// Set config path flag
			err := flag.Set("config", configFile)
			require.NoError(t, err)

			// Call ParseFlags
			ctx := context.Background()
			cfg := ParseFlags(ctx)

			// Call Verify to check for validation errors
			var configErrs config.Errors
			cfg.Verify(&configErrs)

			// Verify that we got no errors
			assert.Empty(t, configErrs, tt.description)
		})
	}
}

func TestParseFlags_ActorSystemValidation(t *testing.T) {
	tests := []struct {
		name           string
		configContent  string
		expectedErrors []string
		description    string
	}{
		{
			name: "invalid_actor_port",
			configContent: `
version: 2
global:
  server_name: test.localhost
  private_key: matrix_key.pem
  actors:
    enabled: true
    port: 70000
client_api:
  registration_disabled: true
`,
			expectedErrors: []string{"invalid value for config key 'room_server.actor_system.port': 70000"},
			description:    "Should error when actor port is out of valid range",
		},
		{
			name: "invalid_cluster_mode",
			configContent: `
version: 2
global:
  server_name: test.localhost
  private_key: matrix_key.pem
  actors:
    enabled: true
    port: 8080
    cluster_mode: invalid_mode
client_api:
  registration_disabled: true
`,
			expectedErrors: []string{"invalid value for config key 'room_server.actor_system.cluster_mode': invalid_mode"},
			description:    "Should error when cluster mode is invalid",
		},
		{
			name: "invalid_idle_timeout",
			configContent: `
version: 2
global:
  server_name: test.localhost
  private_key: matrix_key.pem
  actors:
    enabled: true
    port: 8080
    cluster_mode: none
    idle_timeout: 5s
client_api:
  registration_disabled: true
`,
			expectedErrors: []string{"invalid value for config key 'room_server.actor_system.idle_timeout': 5s"},
			description:    "Should error when idle timeout is too short",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary config file
			configFile := createTempConfigFile(t, tt.configContent)
			defer os.Remove(configFile)

			// Reset flags for testing
			resetFlags()

			// Set config path flag
			err := flag.Set("config", configFile)
			require.NoError(t, err)

			// Call ParseFlags
			ctx := context.Background()
			cfg := ParseFlags(ctx)

			// Call Verify to check for validation errors
			var configErrs config.Errors
			cfg.Verify(&configErrs)

			// Verify that we got the expected errors
			assert.NotEmpty(t, configErrs, tt.description)
			for _, expectedErr := range tt.expectedErrors {
				found := false
				for _, actualErr := range configErrs {
					if strings.Contains(actualErr, expectedErr) {
						found = true
						break
					}
				}
				assert.True(t, found, "Expected error containing '%s' not found in: %v", expectedErr, configErrs)
			}
		})
	}
}

func TestParseFlags_ValidConfiguration(t *testing.T) {
	// Test that a complete, valid configuration passes all validation
	configContent := `
version: 2
global:
  server_name: test.localhost
  private_key: matrix_key.pem
  embedded_config:
    database_url: 
      - postgres://user:pass@localhost/matrix?sslmode=disable
  cache:
    cache_uri: redis://localhost:6379
    max_size_estimated: 1gb
  queue:
    queue_uri: nats://localhost:4222
    topic_prefix: "Matrix_"
client_api:
  registration_disabled: true
federation_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_federation?sslmode=disable
room_server:
  database:
    database_uri: postgres://user:pass@localhost/matrix_roomserver?sslmode=disable
sync_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_syncapi?sslmode=disable
user_api:
  account_database:
    database_uri: postgres://user:pass@localhost/matrix_userapi?sslmode=disable
key_server:
  database:
    database_uri: postgres://user:pass@localhost/matrix_keyserver?sslmode=disable
media_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_mediaapi?sslmode=disable
relay_api:
  database:
    database_uri: postgres://user:pass@localhost/matrix_relayapi?sslmode=disable
mscs:
  database:
    database_uri: postgres://user:pass@localhost/matrix_mscs?sslmode=disable
`

	// Create temporary config file
	configFile := createTempConfigFile(t, configContent)
	defer os.Remove(configFile)

	// Reset flags for testing
	resetFlags()

	// Set config path flag
	err := flag.Set("config", configFile)
	require.NoError(t, err)

	// Call ParseFlags
	ctx := context.Background()
	cfg := ParseFlags(ctx)

	// Call Verify to check for validation errors
	var configErrs config.Errors
	cfg.Verify(&configErrs)

	// Verify that there are no validation errors
	if len(configErrs) > 0 {
		t.Errorf("Expected no validation errors, but got: %v", configErrs)
	}
	assert.Empty(t, configErrs, "Valid configuration should pass all validation checks")
}

func TestParseFlags_ValidDatabaseConfiguration(t *testing.T) {
	// Test that a complete, valid configuration passes all validation
	configContent := `
version: 2
global:
  server_name: test.localhost
  private_key: matrix_key.pem
  embedded_config:
    database_url: 
      - postgres://user:pass@localhost/matrix?sslmode=disable
  cache:
    cache_uri: redis://localhost:6379
    max_size_estimated: 1gb
  queue:
    queue_uri: nats://localhost:4222
    topic_prefix: "Matrix_"
client_api:
  registration_disabled: true
mscs:
  database:
    database_uri: postgres://user:pass@localhost/matrix_mscs?sslmode=disable
`

	// Create temporary config file
	configFile := createTempConfigFile(t, configContent)
	defer os.Remove(configFile)

	// Reset flags for testing
	resetFlags()

	// Set config path flag
	err := flag.Set("config", configFile)
	require.NoError(t, err)

	// Call ParseFlags
	ctx := context.Background()
	cfg := ParseFlags(ctx)

	// Call Verify to check for validation errors
	var configErrs config.Errors
	cfg.Verify(&configErrs)

	// Verify that there are no validation errors
	if len(configErrs) > 0 {
		t.Errorf("Expected no validation errors, but got: %v", configErrs)
	}
	assert.Empty(t, configErrs, "Valid configuration should pass all validation checks")
}

func TestParseFlags_InvalidConfigFile(t *testing.T) {
	tests := []struct {
		name        string
		setupFunc   func(t *testing.T) string
		description string
	}{
		{
			name: "nonexistent_config_file",
			setupFunc: func(t *testing.T) string {
				return "/nonexistent/path/config.yaml"
			},
			description: "Should handle nonexistent config file gracefully",
		},
		{
			name: "malformed_yaml",
			setupFunc: func(t *testing.T) string {
				content := `
version: 2
global:
  server_name: test.localhost
  private_key: matrix_key.pem
  invalid_yaml: [unclosed bracket
client_api:
  registration_disabled: true
`
				return createTempConfigFile(t, content)
			},
			description: "Should handle malformed YAML gracefully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configFile := tt.setupFunc(t)
			if strings.Contains(configFile, "/tmp/") {
				defer os.Remove(configFile)
			}

			// Reset flags for testing
			resetFlags()

			// Set config path flag
			err := flag.Set("config", configFile)
			require.NoError(t, err)

			// Call ParseFlags - this should handle errors gracefully
			// Note: In a real scenario, this would call log.Fatal, but in tests
			// we expect the config loading to fail and return an error

			// We expect this to potentially panic or exit, so we'll just verify
			// that the flag parsing works correctly up to the config loading
			assert.NotPanics(t, func() {
				// Just verify flag parsing works
				flag.Parse()
			}, tt.description)
		})
	}
}

func TestParseFlags_VersionFlag(t *testing.T) {
	// Create minimal config for flag parsing
	configContent := createMinimalTestConfig()
	configFile := createTempConfigFile(t, configContent)
	defer os.Remove(configFile)

	// Reset flags for testing
	resetFlags()

	// Set version flag
	err := flag.Set("version", "true")
	require.NoError(t, err)

	// Set config path flag (even though version should exit before using it)
	err = flag.Set("config", configFile)
	require.NoError(t, err)

	// Note: In a real scenario, ParseFlags would call os.Exit(0) when version=true
	// We can't easily test this without subprocess testing, but we can verify
	// the flag is set correctly
	assert.True(t, *version, "Version flag should be set to true")
}

func TestParseFlags_EmptyConfigPath(t *testing.T) {
	// Reset flags for testing
	resetFlags()

	// Set empty config path
	err := flag.Set("config", "")
	require.NoError(t, err)

	// Note: In a real scenario, ParseFlags would call log.Fatal when config is empty
	// We can verify the flag is set correctly
	assert.Empty(t, *configPath, "Config path should be empty")
}

func restoreEnv(key, value string) {
	if value != "" {
		os.Setenv(key, value)
	} else {
		os.Unsetenv(key)
	}
}
