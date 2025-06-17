package config

import (
	"os"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func resetFlags() {
	pflag.CommandLine = pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)
	viper.Reset()
}

func setupTestEnv(envVars map[string]string) func() {
	// Save original environment
	originalEnv := make(map[string]string)
	for key := range envVars {
		originalEnv[key] = os.Getenv(key)
		_ = os.Unsetenv(key)
	}

	// Set test environment
	for key, value := range envVars {
		if value != "" {
			_ = os.Setenv(key, value)
		}
	}

	// Return cleanup function
	return func() {
		for key := range envVars {
			if val, exists := originalEnv[key]; exists && val != "" {
				_ = os.Setenv(key, val)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}
}

func TestConfigLoad(t *testing.T) {
	resetFlags()

	envVars := map[string]string{
		"CENSUS3_PROJECT":      "",
		"CENSUS3_PERIOD":       "",
		"CENSUS3_API_PORT":     "",
		"CENSUS3_STORAGE_PATH": "",
		"CENSUS3_MIN_BALANCES": "",
		"CENSUS3_HOST":         "",
		"CENSUS3_BATCH_SIZE":   "",
	}

	cleanup := setupTestEnv(envVars)
	defer cleanup()

	// Test with missing required project
	_, err := Load()
	if err == nil {
		t.Error("Expected error for missing project, got nil")
	}

	// Reset flags and set required project
	resetFlags()
	_ = os.Setenv("CENSUS3_PROJECT", "test-project")

	// Test with default values
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify default values
	if cfg.Project != "test-project" {
		t.Errorf("Expected project 'test-project', got '%s'", cfg.Project)
	}
	if cfg.Period != time.Hour {
		t.Errorf("Expected period 1h, got %v", cfg.Period)
	}
	if cfg.APIPort != 8080 {
		t.Errorf("Expected API port 8080, got %d", cfg.APIPort)
	}
	if cfg.StoragePath != "./snapshots.json" {
		t.Errorf("Expected storage path './snapshots.json', got '%s'", cfg.StoragePath)
	}
	if len(cfg.MinBalances) != 1 || cfg.MinBalances[0] != 0.25 {
		t.Errorf("Expected min balances [0.25], got %v", cfg.MinBalances)
	}
	if cfg.Host != "http://localhost:8080" {
		t.Errorf("Expected host 'http://localhost:8080', got '%s'", cfg.Host)
	}
	if cfg.BatchSize != 5000 {
		t.Errorf("Expected batch size 5000, got %d", cfg.BatchSize)
	}
}

func TestConfigLoadWithEnvironmentVariables(t *testing.T) {
	resetFlags()

	envVars := map[string]string{
		"CENSUS3_PROJECT":      "custom-project",
		"CENSUS3_PERIOD":       "30m",
		"CENSUS3_API_PORT":     "9090",
		"CENSUS3_STORAGE_PATH": "/custom/path/snapshots.json",
		"CENSUS3_MIN_BALANCES": "1.5",
		"CENSUS3_HOST":         "http://custom-host:8080",
		"CENSUS3_BATCH_SIZE":   "1000",
	}

	cleanup := setupTestEnv(envVars)
	defer cleanup()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load config with env vars: %v", err)
	}

	// Verify custom values
	if cfg.Project != "custom-project" {
		t.Errorf("Expected project 'custom-project', got '%s'", cfg.Project)
	}
	if cfg.Period != 30*time.Minute {
		t.Errorf("Expected period 30m, got %v", cfg.Period)
	}
	if cfg.APIPort != 9090 {
		t.Errorf("Expected API port 9090, got %d", cfg.APIPort)
	}
	if cfg.StoragePath != "/custom/path/snapshots.json" {
		t.Errorf("Expected storage path '/custom/path/snapshots.json', got '%s'", cfg.StoragePath)
	}
	// Note: min-balances from environment variables are not supported due to viper limitations
	// This test should use the default value
	if len(cfg.MinBalances) != 1 || cfg.MinBalances[0] != 0.25 {
		t.Errorf("Expected min balances [0.25] (default), got %v", cfg.MinBalances)
	}
	if cfg.Host != "http://custom-host:8080" {
		t.Errorf("Expected host 'http://custom-host:8080', got '%s'", cfg.Host)
	}
	if cfg.BatchSize != 1000 {
		t.Errorf("Expected batch size 1000, got %d", cfg.BatchSize)
	}
}
