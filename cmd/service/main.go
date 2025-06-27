package main

import (
	"os"

	"github.com/spf13/pflag"

	"census3-bigquery/bigquery"
	"census3-bigquery/config"
	"census3-bigquery/log"
	"census3-bigquery/service"
)

func main() {

	// Add list-queries flag to the config flags
	pflag.Bool("list-queries", false, "List available BigQuery queries and exit")

	// Load configuration (this will parse all flags)
	cfg, err := config.Load()

	// Check for list-queries flag after parsing
	if listQueries, _ := pflag.CommandLine.GetBool("list-queries"); listQueries {
		bigquery.PrintAvailableQueries()
		os.Exit(0)
	}

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// Set log level from configuration
	if err := log.SetLevelFromString(cfg.LogLevel); err != nil {
		log.Fatal().Err(err).Msg("Failed to set log level")
	}
	log.Info().Str("log_level", cfg.LogLevel).Msg("Log level configured")

	// Create and start service
	svc, err := service.New(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create service")
	}

	// Start service (blocks until shutdown)
	if err := svc.Start(); err != nil {
		log.Fatal().Err(err).Msg("Service error")
	}

	os.Exit(0)
}
