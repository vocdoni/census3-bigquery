package service

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"census3-bigquery/internal/api"
	"census3-bigquery/internal/bigquery"
	"census3-bigquery/internal/census"
	"census3-bigquery/internal/config"
	"census3-bigquery/internal/storage"
)

// Service represents the main service that coordinates all operations
type Service struct {
	config         *config.Config
	localStorage   *storage.SnapshotStorage
	gitStorage     *storage.GitStorage
	bigqueryClient *bigquery.Client
	censusClient   *census.Client
	apiServer      *api.Server
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

// New creates a new service instance
func New(cfg *config.Config) (*Service, error) {
	ctx, cancel := context.WithCancel(context.Background())

	var localStorage *storage.SnapshotStorage
	var gitStorage *storage.GitStorage
	var apiServer *api.Server

	// Initialize storage based on configuration
	if cfg.GitHubEnabled {
		// Initialize Git storage
		var err error
		gitStorage, err = storage.NewGitStorage(cfg.GitHubRepo, cfg.GitHubPAT, "./git-repo", cfg.MaxSnapshotsToKeep, cfg.SkipCSVUpload)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create Git storage: %w", err)
		}
		if err := gitStorage.Load(); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to load Git storage: %w", err)
		}
		log.Info().Msg("Using Git storage mode - API server disabled")
	} else {
		// Initialize local storage
		localStorage = storage.NewSnapshotStorage(cfg.StoragePath)
		if err := localStorage.Load(); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to load local storage: %w", err)
		}
		// Initialize API server only for local storage
		apiServer = api.NewServer(localStorage, cfg.APIPort)
		log.Info().Msg("Using local storage mode - API server enabled")
	}

	// Initialize BigQuery client
	bqClient, err := bigquery.NewClient(ctx, cfg.Project)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	// Initialize Census client
	censusClient, err := census.NewClient(cfg.Host)
	if err != nil {
		cancel()
		if closeErr := bqClient.Close(); closeErr != nil {
			log.Warn().Err(closeErr).Msg("Failed to close BigQuery client")
		}
		return nil, fmt.Errorf("failed to create census client: %w", err)
	}

	return &Service{
		config:         cfg,
		localStorage:   localStorage,
		gitStorage:     gitStorage,
		bigqueryClient: bqClient,
		censusClient:   censusClient,
		apiServer:      apiServer,
		ctx:            ctx,
		cancel:         cancel,
	}, nil
}

// Start starts the service
func (s *Service) Start() error {
	log.Info().Msg("Starting census3-bigquery service")

	// Log all configuration values
	log.Info().
		Dur("period", s.config.Period).
		Int("api_port", s.config.APIPort).
		Str("storage_path", s.config.StoragePath).
		Str("project", s.config.Project).
		Interface("min_balances", s.config.MinBalances).
		Str("query_name", s.config.QueryName).
		Str("host", s.config.Host).
		Int("batch_size", s.config.BatchSize).
		Bool("github_enabled", s.config.GitHubEnabled).
		Str("github_repo", s.config.GitHubRepo).
		Int("max_snapshots_to_keep", s.config.MaxSnapshotsToKeep).
		Bool("skip_csv_upload", s.config.SkipCSVUpload).
		Msg("Service configuration loaded")

	// Start API server in a goroutine (only if not using Git storage)
	if s.apiServer != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.apiServer.Start(); err != nil {
				log.Error().Err(err).Msg("API server error")
			}
		}()
	}

	// Start periodic sync in a goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runPeriodicSync()
	}()

	// Wait for shutdown signal
	s.waitForShutdown()

	return nil
}

// Stop stops the service gracefully
func (s *Service) Stop() {
	log.Info().Msg("Stopping census3-bigquery service")
	s.cancel()
	s.wg.Wait()

	if err := s.bigqueryClient.Close(); err != nil {
		log.Error().Err(err).Msg("Error closing BigQuery client")
	}

	log.Info().Msg("Service stopped")
}

// runPeriodicSync runs the periodic synchronization process
func (s *Service) runPeriodicSync() {
	// Run initial sync immediately
	log.Info().Msg("Running initial sync")
	if err := s.performSync(); err != nil {
		log.Error().Err(err).Msg("Initial sync failed")
	}

	// Create ticker for periodic sync
	ticker := time.NewTicker(s.config.Period)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			log.Info().Msg("Periodic sync stopped")
			return
		case <-ticker.C:
			log.Info().Msg("Running periodic sync")
			if err := s.performSync(); err != nil {
				log.Error().Err(err).Msg("Periodic sync failed")
			}
		}
	}
}

// performSync performs a single synchronization cycle
func (s *Service) performSync() error {
	snapshotDate := time.Now().Truncate(time.Minute)
	log.Info().Time("snapshot_date", snapshotDate).Msg("Starting sync")

	// Process each minimum balance sequentially
	for i, minBalance := range s.config.MinBalances {
		log.Info().Float64("min_balance", minBalance).Int("balance_index", i+1).Int("total_balances", len(s.config.MinBalances)).Msg("Processing balance threshold")

		// Generate CSV filename with timestamp
		csvPath := bigquery.GenerateCSVFileName()
		defer func(path string) {
			// Clean up CSV file after processing
			if err := os.Remove(path); err != nil {
				log.Warn().Err(err).Str("csv_path", path).Msg("Failed to remove CSV file")
			}
		}(csvPath)

		// Step 1: Fetch data from BigQuery and save to CSV
		log.Info().Msg("Fetching data from BigQuery...")
		bqConfig := bigquery.Config{
			Project:     s.config.Project,
			MinBalance:  minBalance,
			QueryName:   s.config.QueryName,
			QueryParams: make(map[string]interface{}), // Additional parameters can be added here
		}

		participantCount, err := s.bigqueryClient.FetchBalancesToCSV(s.ctx, bqConfig, csvPath)
		if err != nil {
			return fmt.Errorf("failed to fetch BigQuery data for balance %.2f: %w", minBalance, err)
		}

		log.Info().Int("participant_count", participantCount).Float64("min_balance", minBalance).Msg("Fetched participants from BigQuery")

		// Step 2: Create census from CSV
		log.Info().Msg("Creating census from CSV...")
		censusRoot, actualCount, err := s.censusClient.CreateCensusFromCSV(csvPath, s.config.BatchSize)
		if err != nil {
			return fmt.Errorf("failed to create census for balance %.2f: %w", minBalance, err)
		}

		log.Info().
			Str("census_root", censusRoot.String()).
			Int("processed_count", actualCount).
			Float64("min_balance", minBalance).
			Msg("Census created successfully")

		// Step 3: Store snapshot based on storage type
		if s.config.GitHubEnabled {
			// Use Git storage
			if err := s.gitStorage.AddSnapshot(snapshotDate, censusRoot, actualCount, csvPath, minBalance, s.config.QueryName); err != nil {
				return fmt.Errorf("failed to store snapshot to Git for balance %.2f: %w", minBalance, err)
			}
			log.Info().Float64("min_balance", minBalance).Msg("Snapshot stored successfully to Git repository")
		} else {
			// Use local storage
			if err := s.localStorage.AddSnapshot(snapshotDate, censusRoot, actualCount, minBalance, s.config.QueryName); err != nil {
				return fmt.Errorf("failed to store snapshot locally for balance %.2f: %w", minBalance, err)
			}
			log.Info().Float64("min_balance", minBalance).Msg("Snapshot stored successfully to local storage")
		}
	}

	return nil
}

// waitForShutdown waits for shutdown signals
func (s *Service) waitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	select {
	case sig := <-sigChan:
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
		log.Info().Msg("Initiating graceful shutdown...")
	case <-s.ctx.Done():
		log.Info().Msg("Context cancelled, shutting down...")
	}

	// Stop signal notifications
	signal.Stop(sigChan)
	close(sigChan)

	// Call Stop to perform graceful shutdown
	s.Stop()
}
