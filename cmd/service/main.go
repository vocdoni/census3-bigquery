package main

import (
	"os"

	"github.com/vocdoni/census3-bigquery/bigquery"
	"github.com/vocdoni/census3-bigquery/config"
	"github.com/vocdoni/census3-bigquery/service"
	"github.com/vocdoni/davinci-node/log"

	"github.com/spf13/pflag"
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
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize log with the configured level
	log.Init(cfg.LogLevel, "stderr", nil)
	log.Infof("Log level configured: %s", cfg.LogLevel)

	// Create and start service
	svc, err := service.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create service: %v", err)
	}

	// Start service (blocks until shutdown)
	if err := svc.Start(); err != nil {
		log.Fatalf("Service error: %v", err)
	}

	os.Exit(0)
}
