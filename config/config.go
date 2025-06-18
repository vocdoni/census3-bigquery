package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

// getDefaultDataDir returns the default data directory
func getDefaultDataDir() string {
	// Try to use $HOME/.bigcensus3
	if homeDir, err := os.UserHomeDir(); err == nil && homeDir != "" {
		return filepath.Join(homeDir, ".bigcensus3")
	}

	// Fall back to temp directory
	return filepath.Join(os.TempDir(), "bigcensus3")
}

// QueryConfig represents a single query configuration
type QueryConfig struct {
	Name       string                 `yaml:"name" json:"name"`   // User-defined name for this query instance
	Query      string                 `yaml:"query" json:"query"` // BigQuery query name from registry
	Period     time.Duration          `yaml:"period" json:"period"`
	Parameters map[string]interface{} `yaml:"parameters" json:"parameters"`
}

// QueriesFile represents the structure of the queries YAML file
type QueriesFile struct {
	Queries []QueryConfig `yaml:"queries" json:"queries"`
}

// Config holds all configuration for the service
type Config struct {
	// Service configuration
	APIPort int    `mapstructure:"api-port"`
	DataDir string `mapstructure:"data-dir"`
	Project string `mapstructure:"project"`

	// Query configurations
	Queries []QueryConfig `mapstructure:"queries"`

	// Census configuration
	BatchSize int `mapstructure:"batch-size"`

	// Internal fields
	QueriesFile string `mapstructure:"queries-file"`
}

// Load loads configuration from flags, environment variables, and YAML file
func Load() (*Config, error) {
	// Get default data directory
	defaultDataDir := getDefaultDataDir()

	// Define flags
	pflag.Int("api-port", 8080, "API server port")
	pflag.String("data-dir", defaultDataDir, "Data directory for storage (default: $HOME/.bigcensus3 or temp dir)")
	pflag.String("project", "", "GCP project ID for BigQuery (required)")
	pflag.Int("batch-size", 10000, "Batch size for census creation")
	pflag.String("queries-file", "./queries.yaml", "Path to queries configuration file")

	pflag.Parse()

	// Set default values in viper
	viper.SetDefault("api-port", 8080)
	viper.SetDefault("data-dir", defaultDataDir)
	viper.SetDefault("project", "")
	viper.SetDefault("batch-size", 10000)
	viper.SetDefault("queries-file", "./queries.yaml")

	// Bind flags to viper
	for _, flag := range []string{"api-port", "data-dir", "project", "batch-size", "queries-file"} {
		if err := viper.BindPFlag(flag, pflag.CommandLine.Lookup(flag)); err != nil {
			return nil, fmt.Errorf("failed to bind flag %s: %w", flag, err)
		}
	}

	// Set environment variable prefix
	viper.SetEnvPrefix("CENSUS3")
	viper.AutomaticEnv()

	// Map environment variables to config keys
	_ = viper.BindEnv("api-port", "CENSUS3_API_PORT")
	_ = viper.BindEnv("data-dir", "CENSUS3_DATA_DIR")
	_ = viper.BindEnv("project", "CENSUS3_PROJECT")
	_ = viper.BindEnv("batch-size", "CENSUS3_BATCH_SIZE")
	_ = viper.BindEnv("queries-file", "CENSUS3_QUERIES_FILE")

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Load queries from YAML file
	if err := cfg.loadQueries(); err != nil {
		return nil, fmt.Errorf("failed to load queries: %w", err)
	}

	// Validate required fields
	if cfg.Project == "" {
		return nil, fmt.Errorf("project is required")
	}

	if len(cfg.Queries) == 0 {
		return nil, fmt.Errorf("at least one query must be configured in %s", cfg.QueriesFile)
	}

	return &cfg, nil
}

// loadQueries loads query configurations from the YAML file
func (c *Config) loadQueries() error {
	// Check if queries file exists
	if _, err := os.Stat(c.QueriesFile); os.IsNotExist(err) {
		return fmt.Errorf("queries file not found: %s (copy from queries.yaml.example)", c.QueriesFile)
	}

	// Read the YAML file
	data, err := os.ReadFile(c.QueriesFile)
	if err != nil {
		return fmt.Errorf("failed to read queries file %s: %w", c.QueriesFile, err)
	}

	// Parse the YAML
	var queriesFile QueriesFile
	if err := yaml.Unmarshal(data, &queriesFile); err != nil {
		return fmt.Errorf("failed to parse queries file %s: %w", c.QueriesFile, err)
	}

	// Validate and process queries
	if len(queriesFile.Queries) == 0 {
		return fmt.Errorf("no queries defined in %s", c.QueriesFile)
	}

	for i, query := range queriesFile.Queries {
		// Validate query name
		if query.Name == "" {
			return fmt.Errorf("query %d: name is required", i+1)
		}

		// Validate query field
		if query.Query == "" {
			return fmt.Errorf("query %d (%s): query is required", i+1, query.Name)
		}

		// Validate period
		if query.Period <= 0 {
			return fmt.Errorf("query %d (%s): period must be positive", i+1, query.Name)
		}

		// Initialize parameters map if nil
		if query.Parameters == nil {
			query.Parameters = make(map[string]interface{})
		}

		// Update the query in the slice
		queriesFile.Queries[i] = query
	}

	c.Queries = queriesFile.Queries
	return nil
}

// GetQueryID returns a unique identifier for a query configuration
// This is used to distinguish between different instances of the same query
func (qc *QueryConfig) GetQueryID() string {
	// Create a simple hash-like identifier from query name and parameters
	id := qc.Name

	// Add key parameters to make it unique
	if minBalance, ok := qc.Parameters["min_balance"]; ok {
		// Convert to float64 to ensure consistent formatting
		switch v := minBalance.(type) {
		case float64:
			id += fmt.Sprintf("_mb%.2f", v)
		case int:
			id += fmt.Sprintf("_mb%.2f", float64(v))
		case int64:
			id += fmt.Sprintf("_mb%.2f", float64(v))
		default:
			id += fmt.Sprintf("_mb%v", v)
		}
	}
	if tokenAddress, ok := qc.Parameters["token_address"]; ok {
		id += fmt.Sprintf("_token%s", tokenAddress)
	}

	return id
}

// GetMinBalance returns the min_balance parameter as float64, or 0 if not set
func (qc *QueryConfig) GetMinBalance() float64 {
	if minBalance, ok := qc.Parameters["min_balance"]; ok {
		switch v := minBalance.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	}
	return 0
}
