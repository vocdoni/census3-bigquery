package main

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"census3-bigquery/internal/config"
	"census3-bigquery/internal/service"
)

func main() {
	// Configure zerolog for nice console output
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

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
