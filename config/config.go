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

const (
	DefaultBigQueryPricePerTB = 5
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

// WeightConfig represents weight calculation configuration
type WeightConfig struct {
	Strategy        string   `yaml:"strategy" json:"strategy"` // "constant", "proportional_auto", "proportional_manual"
	ConstantWeight  *int     `yaml:"constant_weight,omitempty" json:"constant_weight,omitempty"`
	TargetMinWeight *int     `yaml:"target_min_weight,omitempty" json:"target_min_weight,omitempty"`
	Multiplier      *float64 `yaml:"multiplier,omitempty" json:"multiplier,omitempty"`
	MaxWeight       *int     `yaml:"max_weight,omitempty" json:"max_weight,omitempty"`
}

// BigQueryPricing holds pricing information for cost estimation (internal use only)
type BigQueryPricing struct {
	PricePerTBProcessed float64 `yaml:"price_per_tb_processed" json:"price_per_tb_processed"` // Default: $5.00 per TB
}

// QueryConfig represents a single query configuration
type QueryConfig struct {
	Name          string                 `yaml:"name" json:"name"`   // User-defined name for this query instance
	Query         string                 `yaml:"query" json:"query"` // BigQuery query name from registry
	Period        time.Duration          `yaml:"period" json:"period"`
	Disabled      *bool                  `yaml:"disabled,omitempty" json:"disabled,omitempty"`       // Disables synchronization but keeps existing snapshots accessible
	SyncOnStart   *bool                  `yaml:"syncOnStart,omitempty" json:"syncOnStart,omitempty"` // If false, respects period timing; if true, syncs immediately on startup
	Decimals      *int                   `yaml:"decimals,omitempty" json:"decimals,omitempty"`       // Token decimals (18 for ETH, 6 for USDC, etc.)
	Parameters    map[string]interface{} `yaml:"parameters" json:"parameters"`
	Weight        *WeightConfig          `yaml:"weight,omitempty" json:"weight,omitempty"`
	EstimateFirst *bool                  `yaml:"estimate_first,omitempty" json:"estimate_first,omitempty"` // Whether to estimate query cost before execution
	CostPreset    *string                `yaml:"cost_preset,omitempty" json:"cost_preset,omitempty"`       // Simple cost preset: "conservative", "default", "high_volume", "none"
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

		// Validate weight configuration if provided
		if query.Weight != nil {
			if err := query.Weight.Validate(); err != nil {
				return fmt.Errorf("query %d (%s): invalid weight configuration: %w", i+1, query.Name, err)
			}
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

// GetDecimals returns the token decimals with smart defaults
func (qc *QueryConfig) GetDecimals() int {
	if qc.Decimals != nil {
		return *qc.Decimals
	}

	// Smart defaults based on query type
	switch qc.Query {
	case "ethereum_balances", "ethereum_balances_recent":
		return 18 // ETH has 18 decimals
	default:
		return 18 // Default to 18 decimals for unknown tokens
	}
}

// GetWeightConfig returns the weight configuration with defaults
func (qc *QueryConfig) GetWeightConfig() WeightConfig {
	if qc.Weight != nil {
		return *qc.Weight
	}

	// Default to proportional_manual with multiplier 100 for backwards compatibility
	multiplier := 100.0
	return WeightConfig{
		Strategy:   "proportional_manual",
		Multiplier: &multiplier,
	}
}

// IsDisabled returns true if the query is disabled
func (qc *QueryConfig) IsDisabled() bool {
	return qc.Disabled != nil && *qc.Disabled
}

// GetSyncOnStart returns the syncOnStart setting with default false
func (qc *QueryConfig) GetSyncOnStart() bool {
	return qc.SyncOnStart != nil && *qc.SyncOnStart
}

// GetEstimateFirst returns the estimate_first setting with default false
func (qc *QueryConfig) GetEstimateFirst() bool {
	return qc.EstimateFirst != nil && *qc.EstimateFirst
}

// GetCostPreset returns the cost preset with default "default"
func (qc *QueryConfig) GetCostPreset() string {
	if qc.CostPreset != nil {
		return *qc.CostPreset
	}
	return "default" // Default preset
}

// GetBigQueryPricing returns the BigQuery pricing configuration with defaults (internal use)
func (qc *QueryConfig) GetBigQueryPricing() *BigQueryPricing {
	// Always return default pricing (simplified - no YAML configuration)
	return &BigQueryPricing{
		PricePerTBProcessed: DefaultBigQueryPricePerTB,
	}
}

// ValidateWeightConfig validates the weight configuration
func (wc *WeightConfig) Validate() error {
	switch wc.Strategy {
	case "constant":
		if wc.ConstantWeight == nil {
			return fmt.Errorf("constant_weight is required for constant strategy")
		}
		if *wc.ConstantWeight <= 0 {
			return fmt.Errorf("constant_weight must be positive")
		}
	case "proportional_auto":
		if wc.TargetMinWeight == nil {
			return fmt.Errorf("target_min_weight is required for proportional_auto strategy")
		}
		if *wc.TargetMinWeight <= 0 {
			return fmt.Errorf("target_min_weight must be positive")
		}
	case "proportional_manual":
		if wc.Multiplier == nil {
			return fmt.Errorf("multiplier is required for proportional_manual strategy")
		}
		if *wc.Multiplier <= 0 {
			return fmt.Errorf("multiplier must be positive")
		}
	default:
		return fmt.Errorf("invalid weight strategy: %s (must be 'constant', 'proportional_auto', or 'proportional_manual')", wc.Strategy)
	}

	if wc.MaxWeight != nil && *wc.MaxWeight <= 0 {
		return fmt.Errorf("max_weight must be positive if specified")
	}

	return nil
}
