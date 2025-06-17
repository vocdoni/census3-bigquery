package config

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Config holds all configuration for the service
type Config struct {
	// Service configuration
	Period      time.Duration `mapstructure:"period"`
	APIPort     int           `mapstructure:"api-port"`
	StoragePath string        `mapstructure:"storage-path"`

	// BigQuery configuration
	Project     string    `mapstructure:"project"`
	MinBalances []float64 `mapstructure:"min-balances"`
	QueryName   string    `mapstructure:"query-name"`

	// Census configuration
	Host      string `mapstructure:"host"`
	BatchSize int    `mapstructure:"batch-size"`

	// GitHub configuration
	GitHubEnabled      bool   `mapstructure:"github-enabled"`
	GitHubPAT          string `mapstructure:"github-pat"`
	GitHubRepo         string `mapstructure:"github-repo"`
	MaxSnapshotsToKeep int    `mapstructure:"max-snapshots-to-keep"`
	SkipCSVUpload      bool   `mapstructure:"skip-csv-upload"`
}

// Load loads configuration from flags and environment variables
func Load() (*Config, error) {
	// Define flags
	pflag.Duration("period", time.Hour, "Sync period (e.g., 1h, 30m)")
	pflag.Int("api-port", 8080, "API server port")
	pflag.String("storage-path", "./snapshots.json", "Path to store snapshots")
	pflag.String("project", "", "GCP project ID for BigQuery (required)")
	pflag.Float64Slice("min-balances", []float64{0.25}, "Minimum ETH balances (can specify multiple)")
	pflag.String("query-name", "ethereum_balances", "BigQuery query to execute")
	pflag.String("host", "http://localhost:8080", "Vocdoni node host URL")
	pflag.Int("batch-size", 5000, "Batch size for census creation")
	pflag.Bool("github-enabled", false, "Enable GitHub storage mode")
	pflag.String("github-pat", "", "GitHub Personal Access Token")
	pflag.String("github-repo", "", "GitHub repository URL")
	pflag.Int("max-snapshots-to-keep", 5, "Maximum number of snapshots to keep in GitHub repository")
	pflag.Bool("skip-csv-upload", false, "Skip uploading CSV files to Git repository (metadata only)")

	pflag.Parse()

	// Set default values in viper (excluding min-balances which we handle manually)
	viper.SetDefault("period", time.Hour)
	viper.SetDefault("api-port", 8080)
	viper.SetDefault("storage-path", "./snapshots.json")
	viper.SetDefault("project", "")
	viper.SetDefault("query-name", "ethereum_balances")
	viper.SetDefault("host", "http://localhost:8080")
	viper.SetDefault("batch-size", 5000)
	viper.SetDefault("github-enabled", false)
	viper.SetDefault("github-pat", "")
	viper.SetDefault("github-repo", "")
	viper.SetDefault("max-snapshots-to-keep", 5)
	viper.SetDefault("skip-csv-upload", false)

	// Bind flags to viper (excluding min-balances which we handle manually)
	for _, flag := range []string{"period", "api-port", "storage-path", "project", "query-name", "host", "batch-size", "github-enabled", "github-pat", "github-repo", "max-snapshots-to-keep", "skip-csv-upload"} {
		if err := viper.BindPFlag(flag, pflag.CommandLine.Lookup(flag)); err != nil {
			return nil, fmt.Errorf("failed to bind flag %s: %w", flag, err)
		}
	}

	// Set environment variable prefix
	viper.SetEnvPrefix("CENSUS3")
	viper.AutomaticEnv()

	// Map environment variables to config keys
	_ = viper.BindEnv("period", "CENSUS3_PERIOD")
	_ = viper.BindEnv("api-port", "CENSUS3_API_PORT")
	_ = viper.BindEnv("storage-path", "CENSUS3_STORAGE_PATH")
	_ = viper.BindEnv("project", "CENSUS3_PROJECT")
	_ = viper.BindEnv("min-balances", "CENSUS3_MIN_BALANCES")
	_ = viper.BindEnv("query-name", "CENSUS3_QUERY_NAME")
	_ = viper.BindEnv("host", "CENSUS3_HOST")
	_ = viper.BindEnv("batch-size", "CENSUS3_BATCH_SIZE")
	_ = viper.BindEnv("github-enabled", "CENSUS3_GITHUB_ENABLED")
	_ = viper.BindEnv("github-pat", "CENSUS3_GITHUB_PAT")
	_ = viper.BindEnv("github-repo", "CENSUS3_GITHUB_REPO")
	_ = viper.BindEnv("max-snapshots-to-keep", "CENSUS3_MAX_SNAPSHOTS_TO_KEEP")
	_ = viper.BindEnv("skip-csv-upload", "CENSUS3_SKIP_CSV_UPLOAD")

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Handle min-balances manually since viper has issues with float64 slices
	if minBalancesFlag, err := pflag.CommandLine.GetFloat64Slice("min-balances"); err == nil && len(minBalancesFlag) > 0 {
		cfg.MinBalances = minBalancesFlag
	} else {
		// If no flag was provided, use default
		cfg.MinBalances = []float64{0.25}
	}

	// Validate required fields
	if cfg.Project == "" {
		return nil, fmt.Errorf("project is required")
	}

	// Validate GitHub configuration if enabled
	if cfg.GitHubEnabled {
		if cfg.GitHubPAT == "" {
			return nil, fmt.Errorf("github-pat is required when github-enabled is true")
		}
		if cfg.GitHubRepo == "" {
			return nil, fmt.Errorf("github-repo is required when github-enabled is true")
		}
	}

	return &cfg, nil
}

// GetFirstMinBalance returns the first minimum balance for backward compatibility
func (c *Config) GetFirstMinBalance() float64 {
	if len(c.MinBalances) == 0 {
		return 0.25 // default value
	}
	return c.MinBalances[0]
}
