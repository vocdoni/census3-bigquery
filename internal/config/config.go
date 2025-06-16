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
	APIPort     int           `mapstructure:"api_port"`
	StoragePath string        `mapstructure:"storage_path"`

	// BigQuery configuration
	Project     string    `mapstructure:"project"`
	MinBalances []float64 `mapstructure:"min_balances"`
	MaxCount    int       `mapstructure:"max_count"`

	// Census configuration
	Host      string `mapstructure:"host"`
	BatchSize int    `mapstructure:"batch_size"`

	// GitHub configuration
	GitHubEnabled bool   `mapstructure:"github-enabled"`
	GitHubPAT     string `mapstructure:"github-pat"`
	GitHubRepo    string `mapstructure:"github-repo"`
}

// Load loads configuration from flags and environment variables
func Load() (*Config, error) {
	// Define flags
	pflag.Duration("period", time.Hour, "Sync period (e.g., 1h, 30m)")
	pflag.Int("api-port", 8080, "API server port")
	pflag.String("storage-path", "./snapshots.json", "Path to store snapshots")
	pflag.String("project", "", "GCP project ID for BigQuery (required)")
	pflag.Float64Slice("min-balances", []float64{0.25}, "Minimum ETH balances (can specify multiple)")
	pflag.Int("max-count", 100, "Maximum number of addresses to fetch")
	pflag.String("host", "http://localhost:8080", "Vocdoni node host URL")
	pflag.Int("batch-size", 5000, "Batch size for census creation")
	pflag.Bool("github-enabled", false, "Enable GitHub storage mode")
	pflag.String("github-pat", "", "GitHub Personal Access Token")
	pflag.String("github-repo", "", "GitHub repository URL")

	pflag.Parse()

	// Set default values in viper
	viper.SetDefault("period", time.Hour)
	viper.SetDefault("api_port", 8080)
	viper.SetDefault("storage_path", "./snapshots.json")
	viper.SetDefault("project", "")
	viper.SetDefault("min_balances", []float64{0.25})
	viper.SetDefault("max_count", 100)
	viper.SetDefault("host", "http://localhost:8080")
	viper.SetDefault("batch_size", 5000)
	viper.SetDefault("github_enabled", false)
	viper.SetDefault("github_pat", "")
	viper.SetDefault("github_repo", "")

	// Bind flags to viper
	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		return nil, fmt.Errorf("failed to bind flags: %w", err)
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
	_ = viper.BindEnv("max-count", "CENSUS3_MAX_COUNT")
	_ = viper.BindEnv("host", "CENSUS3_HOST")
	_ = viper.BindEnv("batch-size", "CENSUS3_BATCH_SIZE")
	_ = viper.BindEnv("github-enabled", "CENSUS3_GITHUB_ENABLED")
	_ = viper.BindEnv("github-pat", "CENSUS3_GITHUB_PAT")
	_ = viper.BindEnv("github-repo", "CENSUS3_GITHUB_REPO")

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
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
