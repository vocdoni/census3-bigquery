package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/frankban/quicktest"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// resetFlags resets pflag and viper state between tests
func resetFlags() {
	pflag.CommandLine = pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)
	viper.Reset()
}

func TestLoadConfig(t *testing.T) {
	c := quicktest.New(t)
	resetFlags()

	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Create a test queries.yaml file
	queriesFile := filepath.Join(tempDir, "queries.yaml")
	queriesContent := `
queries:
  - name: ethereum_balances_test
    query: ethereum_balances
    period: 1h
    parameters:
      min_balance: 1.0
  - name: erc20_holders_test
    query: erc20_holders
    period: 30m
    parameters:
      token_address: "0x1234567890123456789012345678901234567890"
      min_balance: 100
`
	err := os.WriteFile(queriesFile, []byte(queriesContent), 0o644)
	c.Assert(err, quicktest.IsNil)

	// Set environment variables for testing
	c.Assert(os.Setenv("CENSUS3_PROJECT", "test-project"), quicktest.IsNil)
	c.Assert(os.Setenv("CENSUS3_API_PORT", "9090"), quicktest.IsNil)
	c.Assert(os.Setenv("CENSUS3_DATA_DIR", tempDir), quicktest.IsNil)
	c.Assert(os.Setenv("CENSUS3_BATCH_SIZE", "5000"), quicktest.IsNil)
	c.Assert(os.Setenv("CENSUS3_QUERIES_FILE", queriesFile), quicktest.IsNil)
	defer func() {
		_ = os.Unsetenv("CENSUS3_PROJECT")
		_ = os.Unsetenv("CENSUS3_API_PORT")
		_ = os.Unsetenv("CENSUS3_DATA_DIR")
		_ = os.Unsetenv("CENSUS3_BATCH_SIZE")
		_ = os.Unsetenv("CENSUS3_QUERIES_FILE")
	}()

	// Load configuration
	cfg, err := Load()
	c.Assert(err, quicktest.IsNil)
	c.Assert(cfg, quicktest.Not(quicktest.IsNil))

	// Verify basic configuration
	c.Assert(cfg.Project, quicktest.Equals, "test-project")
	c.Assert(cfg.APIPort, quicktest.Equals, 9090)
	c.Assert(cfg.DataDir, quicktest.Equals, tempDir)
	c.Assert(cfg.BatchSize, quicktest.Equals, 5000)
	c.Assert(cfg.QueriesFile, quicktest.Equals, queriesFile)

	// Verify queries were loaded
	c.Assert(len(cfg.Queries), quicktest.Equals, 2)

	// Verify first query
	query1 := cfg.Queries[0]
	c.Assert(query1.Name, quicktest.Equals, "ethereum_balances_test")
	c.Assert(query1.Query, quicktest.Equals, "ethereum_balances")
	c.Assert(query1.Period, quicktest.Equals, time.Hour)
	c.Assert(query1.Parameters["min_balance"], quicktest.Equals, 1.0)

	// Verify second query
	query2 := cfg.Queries[1]
	c.Assert(query2.Name, quicktest.Equals, "erc20_holders_test")
	c.Assert(query2.Query, quicktest.Equals, "erc20_holders")
	c.Assert(query2.Period, quicktest.Equals, 30*time.Minute)
	c.Assert(query2.Parameters["token_address"], quicktest.Equals, "0x1234567890123456789012345678901234567890")
	c.Assert(query2.Parameters["min_balance"], quicktest.Equals, 100)
}

func TestLoadConfigDefaults(t *testing.T) {
	c := quicktest.New(t)
	resetFlags()

	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Create a minimal test queries.yaml file
	queriesFile := filepath.Join(tempDir, "queries.yaml")
	queriesContent := `
queries:
  - name: ethereum_balances_test
    query: ethereum_balances
    period: 2h
    parameters:
      min_balance: 0.5
`
	err := os.WriteFile(queriesFile, []byte(queriesContent), 0o644)
	c.Assert(err, quicktest.IsNil)

	// Set only required environment variables
	c.Assert(os.Setenv("CENSUS3_PROJECT", "test-project"), quicktest.IsNil)
	c.Assert(os.Setenv("CENSUS3_QUERIES_FILE", queriesFile), quicktest.IsNil)
	defer func() {
		_ = os.Unsetenv("CENSUS3_PROJECT")
		_ = os.Unsetenv("CENSUS3_QUERIES_FILE")
	}()

	// Load configuration
	cfg, err := Load()
	c.Assert(err, quicktest.IsNil)
	c.Assert(cfg, quicktest.Not(quicktest.IsNil))

	// Verify defaults
	c.Assert(cfg.Project, quicktest.Equals, "test-project")
	c.Assert(cfg.APIPort, quicktest.Equals, 8080)
	c.Assert(cfg.BatchSize, quicktest.Equals, 10000)
	c.Assert(cfg.QueriesFile, quicktest.Equals, queriesFile)

	// Verify query was loaded
	c.Assert(len(cfg.Queries), quicktest.Equals, 1)
	query := cfg.Queries[0]
	c.Assert(query.Name, quicktest.Equals, "ethereum_balances_test")
	c.Assert(query.Query, quicktest.Equals, "ethereum_balances")
	c.Assert(query.Period, quicktest.Equals, 2*time.Hour)
	c.Assert(query.Parameters["min_balance"], quicktest.Equals, 0.5)
}

func TestLoadConfigMissingProject(t *testing.T) {
	c := quicktest.New(t)
	resetFlags()

	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Create a test queries.yaml file
	queriesFile := filepath.Join(tempDir, "queries.yaml")
	queriesContent := `
queries:
  - name: ethereum_balances_test
    query: ethereum_balances
    period: 1h
    parameters:
      min_balance: 1.0
`
	err := os.WriteFile(queriesFile, []byte(queriesContent), 0o644)
	c.Assert(err, quicktest.IsNil)

	// Set queries file but not project
	_ = os.Setenv("CENSUS3_QUERIES_FILE", queriesFile)
	defer func() { _ = os.Unsetenv("CENSUS3_QUERIES_FILE") }()

	// Load configuration should fail
	_, err = Load()
	c.Assert(err, quicktest.Not(quicktest.IsNil))
	c.Assert(err.Error(), quicktest.Contains, "project is required")
}

func TestLoadConfigMissingQueriesFile(t *testing.T) {
	c := quicktest.New(t)
	resetFlags()

	// Set project but point to non-existent queries file
	_ = os.Setenv("CENSUS3_PROJECT", "test-project")
	_ = os.Setenv("CENSUS3_QUERIES_FILE", "/non/existent/queries.yaml")
	defer func() {
		_ = os.Unsetenv("CENSUS3_PROJECT")
		_ = os.Unsetenv("CENSUS3_QUERIES_FILE")
	}()

	// Load configuration should fail
	_, err := Load()
	c.Assert(err, quicktest.Not(quicktest.IsNil))
	c.Assert(err.Error(), quicktest.Contains, "queries file not found")
}

func TestLoadConfigInvalidQueriesFile(t *testing.T) {
	c := quicktest.New(t)
	resetFlags()

	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Create an invalid queries.yaml file
	queriesFile := filepath.Join(tempDir, "queries.yaml")
	queriesContent := `
invalid yaml content [
`
	err := os.WriteFile(queriesFile, []byte(queriesContent), 0o644)
	c.Assert(err, quicktest.IsNil)

	// Set environment variables
	_ = os.Setenv("CENSUS3_PROJECT", "test-project")
	_ = os.Setenv("CENSUS3_QUERIES_FILE", queriesFile)
	defer func() {
		_ = os.Unsetenv("CENSUS3_PROJECT")
		_ = os.Unsetenv("CENSUS3_QUERIES_FILE")
	}()

	// Load configuration should fail
	_, err = Load()
	c.Assert(err, quicktest.Not(quicktest.IsNil))
	c.Assert(err.Error(), quicktest.Contains, "failed to parse queries file")
}

func TestLoadConfigEmptyQueries(t *testing.T) {
	c := quicktest.New(t)
	resetFlags()

	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Create an empty queries.yaml file
	queriesFile := filepath.Join(tempDir, "queries.yaml")
	queriesContent := `
queries: []
`
	err := os.WriteFile(queriesFile, []byte(queriesContent), 0o644)
	c.Assert(err, quicktest.IsNil)

	// Set environment variables
	_ = os.Setenv("CENSUS3_PROJECT", "test-project")
	_ = os.Setenv("CENSUS3_QUERIES_FILE", queriesFile)
	defer func() {
		_ = os.Unsetenv("CENSUS3_PROJECT")
		_ = os.Unsetenv("CENSUS3_QUERIES_FILE")
	}()

	// Load configuration should fail
	_, err = Load()
	c.Assert(err, quicktest.Not(quicktest.IsNil))
	c.Assert(err.Error(), quicktest.Contains, "no queries defined")
}

func TestLoadConfigInvalidQueryPeriod(t *testing.T) {
	c := quicktest.New(t)
	resetFlags()

	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Create a queries.yaml file with invalid period
	queriesFile := filepath.Join(tempDir, "queries.yaml")
	queriesContent := `
queries:
  - name: ethereum_balances_test
    query: ethereum_balances
    period: 0s
    parameters:
      min_balance: 1.0
`
	err := os.WriteFile(queriesFile, []byte(queriesContent), 0o644)
	c.Assert(err, quicktest.IsNil)

	// Set environment variables
	_ = os.Setenv("CENSUS3_PROJECT", "test-project")
	_ = os.Setenv("CENSUS3_QUERIES_FILE", queriesFile)
	defer func() {
		_ = os.Unsetenv("CENSUS3_PROJECT")
		_ = os.Unsetenv("CENSUS3_QUERIES_FILE")
	}()

	// Load configuration should fail
	_, err = Load()
	c.Assert(err, quicktest.Not(quicktest.IsNil))
	c.Assert(err.Error(), quicktest.Contains, "period must be positive")
}

func TestQueryConfigGetQueryID(t *testing.T) {
	c := quicktest.New(t)

	// Test basic query ID generation
	queryConfig := QueryConfig{
		Name: "ethereum_balances",
		Parameters: map[string]interface{}{
			"min_balance": 1.0,
		},
	}

	queryID := queryConfig.GetQueryID()
	c.Assert(queryID, quicktest.Equals, "ethereum_balances_mb1.00")

	// Test with token address
	queryConfig2 := QueryConfig{
		Name: "erc20_holders",
		Parameters: map[string]interface{}{
			"min_balance":   100.0,
			"token_address": "0x1234567890123456789012345678901234567890",
		},
	}

	queryID2 := queryConfig2.GetQueryID()
	c.Assert(queryID2, quicktest.Equals, "erc20_holders_mb100.00_token0x1234567890123456789012345678901234567890")

	// Test without parameters
	queryConfig3 := QueryConfig{
		Name:       "custom_query",
		Parameters: map[string]interface{}{},
	}

	queryID3 := queryConfig3.GetQueryID()
	c.Assert(queryID3, quicktest.Equals, "custom_query")
}

func TestQueryConfigGetMinBalance(t *testing.T) {
	c := quicktest.New(t)

	// Test with float64
	queryConfig := QueryConfig{
		Parameters: map[string]interface{}{
			"min_balance": 1.5,
		},
	}
	c.Assert(queryConfig.GetMinBalance(), quicktest.Equals, 1.5)

	// Test with int
	queryConfig2 := QueryConfig{
		Parameters: map[string]interface{}{
			"min_balance": 2,
		},
	}
	c.Assert(queryConfig2.GetMinBalance(), quicktest.Equals, 2.0)

	// Test with int64
	queryConfig3 := QueryConfig{
		Parameters: map[string]interface{}{
			"min_balance": int64(3),
		},
	}
	c.Assert(queryConfig3.GetMinBalance(), quicktest.Equals, 3.0)

	// Test without min_balance
	queryConfig4 := QueryConfig{
		Parameters: map[string]interface{}{},
	}
	c.Assert(queryConfig4.GetMinBalance(), quicktest.Equals, 0.0)
}
