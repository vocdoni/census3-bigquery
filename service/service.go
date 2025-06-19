package service

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vocdoni/arbo"
	"github.com/vocdoni/davinci-node/types"
	"go.vocdoni.io/dvote/db"
	"go.vocdoni.io/dvote/db/metadb"

	"census3-bigquery/api"
	"census3-bigquery/bigquery"
	"census3-bigquery/censusdb"
	"census3-bigquery/config"
	"census3-bigquery/log"
	"census3-bigquery/storage"
)

// BigQueryClient interface for BigQuery operations
type BigQueryClient interface {
	StreamBalances(ctx context.Context, cfg bigquery.Config, participantCh chan<- bigquery.Participant, errorCh chan<- error)
	FetchBalancesToCSV(ctx context.Context, cfg bigquery.Config, csvPath string) (int, error)
	Close() error
}

// Default batch size for census creation - configurable
const DefaultBatchSize = 10000

// QueryRunner represents a single query runner with its own schedule
type QueryRunner struct {
	config  *config.QueryConfig
	service *Service
	ctx     context.Context
	cancel  context.CancelFunc
}

// Service represents the main service that coordinates all operations
type Service struct {
	config         *config.Config
	kvStorage      *storage.KVSnapshotStorage
	bigqueryClient BigQueryClient
	censusDB       *censusdb.CensusDB
	apiServer      *api.Server
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	queryRunners   []*QueryRunner
}

// New creates a new service instance
func New(cfg *config.Config) (*Service, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize BigQuery client
	bqClient, err := bigquery.NewClient(ctx, cfg.Project)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	// Initialize shared database for both census and snapshots
	dataDir := cfg.DataDir
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		cancel()
		if closeErr := bqClient.Close(); closeErr != nil {
			log.Warn().Err(closeErr).Msg("Failed to close BigQuery client")
		}
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	database, err := metadb.New(db.TypePebble, dataDir)
	if err != nil {
		cancel()
		if closeErr := bqClient.Close(); closeErr != nil {
			log.Warn().Err(closeErr).Msg("Failed to close BigQuery client")
		}
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	// Initialize CensusDB with shared database
	censusDB := censusdb.NewCensusDB(database)

	// Initialize KV storage with shared database (using prefixed database)
	kvStorage := storage.NewKVSnapshotStorage(database)

	// Initialize API server with KV storage and censusDB
	apiServer := api.NewServer(kvStorage, censusDB, cfg.APIPort)

	service := &Service{
		config:         cfg,
		kvStorage:      kvStorage,
		bigqueryClient: bqClient,
		censusDB:       censusDB,
		apiServer:      apiServer,
		ctx:            ctx,
		cancel:         cancel,
	}

	// Create query runners for each query configuration
	for i, queryConfig := range cfg.Queries {
		runner, err := service.createQueryRunner(&queryConfig)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create query runner %d (%s): %w", i+1, queryConfig.Name, err)
		}
		service.queryRunners = append(service.queryRunners, runner)
	}

	return service, nil
}

// createQueryRunner creates a new query runner for a specific query configuration
func (s *Service) createQueryRunner(queryConfig *config.QueryConfig) (*QueryRunner, error) {
	ctx, cancel := context.WithCancel(s.ctx)

	// Validate that the query exists in the registry
	if _, err := bigquery.GetQuery(queryConfig.Query); err != nil {
		cancel()
		return nil, fmt.Errorf("invalid query '%s' for config '%s': %w", queryConfig.Query, queryConfig.Name, err)
	}

	return &QueryRunner{
		config:  queryConfig,
		service: s,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// Start starts the service
func (s *Service) Start() error {
	log.Info().Msg("Starting census3-bigquery service")

	// Log service configuration
	log.Info().
		Int("api_port", s.config.APIPort).
		Str("data_dir", s.config.DataDir).
		Str("project", s.config.Project).
		Int("batch_size", s.config.BatchSize).
		Int("query_count", len(s.config.Queries)).
		Str("queries_file", s.config.QueriesFile).
		Msg("Service configuration loaded")

	// Log each query configuration
	for i, queryConfig := range s.config.Queries {
		log.Info().
			Int("query_index", i+1).
			Str("query", queryConfig.Name).
			Str("period", queryConfig.Period.String()).
			Float64("min_balance", queryConfig.GetMinBalance()).
			Interface("parameters", queryConfig.Parameters).
			Msg("Query configuration loaded")
	}

	// Start API server in a goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.apiServer.Start(); err != nil {
			log.Error().Err(err).Msg("API server error")
		}
	}()

	// Start each query runner in its own goroutine
	for i, runner := range s.queryRunners {
		s.wg.Add(1)
		go func(index int, qr *QueryRunner) {
			defer s.wg.Done()
			log.Info().
				Int("query_index", index+1).
				Str("query", qr.config.Name).
				Msg("Starting query runner")
			qr.run()
		}(i, runner)
	}

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

// run executes the query runner's periodic sync process
func (qr *QueryRunner) run() {
	queryID := qr.config.Name

	// Run initial sync immediately
	log.Info().
		Str("query", queryID).
		Msg("Running initial sync")

	if err := qr.performSync(); err != nil {
		log.Error().
			Err(err).
			Str("query", queryID).
			Msg("Initial sync failed")
	}

	// Create ticker for periodic sync
	ticker := time.NewTicker(qr.config.Period)
	defer ticker.Stop()

	for {
		select {
		case <-qr.ctx.Done():
			log.Info().
				Str("query", queryID).
				Msg("Query runner stopped")
			return
		case <-ticker.C:
			log.Info().
				Str("query", queryID).
				Msg("Running periodic sync")

			if err := qr.performSync(); err != nil {
				log.Error().
					Err(err).
					Str("query", queryID).
					Msg("Periodic sync failed")
			}
		}
	}
}

// performSync performs a single synchronization cycle for this query
func (qr *QueryRunner) performSync() error {
	snapshotDate := time.Now().Truncate(time.Minute)
	queryID := qr.config.Name
	minBalance := qr.config.GetMinBalance()

	log.Info().
		Time("snapshot_date", snapshotDate).
		Str("query", queryID).
		Float64("min_balance", minBalance).
		Msg("Starting sync")

	// Step 1: Create a new census with UUID
	censusID := uuid.New()
	log.Info().
		Str("census_id", censusID.String()).
		Str("query", queryID).
		Msg("Creating new census")

	censusRef, err := qr.service.censusDB.New(censusID)
	if err != nil {
		return fmt.Errorf("failed to create new census for query %s: %w", queryID, err)
	}

	// Step 2: Stream data from BigQuery and create census
	log.Info().
		Str("query", queryID).
		Msg("Streaming data from BigQuery and creating census...")

	bqConfig := bigquery.Config{
		Project:      qr.service.config.Project,
		MinBalance:   minBalance, // For backward compatibility with bigquery.Config
		QueryName:    qr.config.Query,
		QueryParams:  qr.config.Parameters,
		Decimals:     qr.config.GetDecimals(),
		WeightConfig: convertWeightConfig(qr.config.GetWeightConfig()),
	}

	actualCount, err := qr.streamAndCreateCensus(censusRef, bqConfig)
	if err != nil {
		return fmt.Errorf("failed to create census for query %s: %w", queryID, err)
	}

	// Step 3: Get the census root
	censusRoot := censusRef.Root()
	if censusRoot == nil {
		return fmt.Errorf("failed to get census root for query %s", queryID)
	}

	log.Info().
		Str("census_root", fmt.Sprintf("0x%x", censusRoot)).
		Int("processed_count", actualCount).
		Str("query", queryID).
		Float64("min_balance", minBalance).
		Msg("Census created successfully")

	// Step 4: Store snapshot in KV storage
	rootHex := types.HexBytes(censusRoot)

	// Convert weight config to storage format
	var storageWeightConfig *storage.WeightConfig
	weightConfig := qr.config.GetWeightConfig()
	storageWeightConfig = &storage.WeightConfig{
		Strategy:        weightConfig.Strategy,
		ConstantWeight:  weightConfig.ConstantWeight,
		TargetMinWeight: weightConfig.TargetMinWeight,
		Multiplier:      weightConfig.Multiplier,
		MaxWeight:       weightConfig.MaxWeight,
	}

	if err := qr.service.kvStorage.AddSnapshot(
		snapshotDate,
		rootHex,
		actualCount,
		minBalance,
		queryID,
		qr.config.Query,
		qr.config.GetDecimals(),
		qr.config.Period.String(),
		qr.config.Parameters,
		storageWeightConfig,
	); err != nil {
		return fmt.Errorf("failed to store snapshot for query %s: %w", queryID, err)
	}

	log.Info().
		Str("query_id", queryID).
		Float64("min_balance", minBalance).
		Msg("Snapshot stored successfully to KV storage")

	return nil
}

// streamAndCreateCensus streams participants from BigQuery and creates census in batches
func (qr *QueryRunner) streamAndCreateCensus(censusRef *censusdb.CensusRef, bqConfig bigquery.Config) (int, error) {
	// Determine batch size
	batchSize := qr.service.config.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	// Create channels for streaming
	participantCh := make(chan bigquery.Participant, 100)
	errorCh := make(chan error, 1)

	// Start BigQuery streaming in a goroutine
	go qr.service.bigqueryClient.StreamBalances(qr.ctx, bqConfig, participantCh, errorCh)

	// Process participants in batches
	var totalProcessed int
	var batch [][]byte
	var values [][]byte
	startTime := time.Now()
	lastLogTime := startTime
	queryID := qr.config.Name

	for {
		select {
		case participant, ok := <-participantCh:
			if !ok {
				// Channel closed, process final batch if any
				if len(batch) > 0 {
					if _, err := censusRef.InsertBatch(batch, values); err != nil {
						return totalProcessed, fmt.Errorf("failed to insert final batch: %w", err)
					}
					totalProcessed += len(batch)
				}

				elapsed := time.Since(startTime)
				rate := float64(totalProcessed) / elapsed.Seconds()
				log.Info().
					Int("total_processed", totalProcessed).
					Str("elapsed", elapsed.String()).
					Float64("addr_per_sec", rate).
					Str("query", queryID).
					Msg("Census creation completed")

				return totalProcessed, nil
			}

			// Hash the address key for the census
			addressKey := participant.Address.Bytes()

			if addressKey == nil {
				log.Warn().
					Str("address", participant.Address.Hex()).
					Str("query", queryID).
					Msg("Failed to hash address key, skipping")
				continue
			}

			if len(addressKey) > types.CensusKeyMaxLen {
				log.Warn().
					Str("address", participant.Address.Hex()).
					Str("query", queryID).
					Msg("Address key length exceeded, skipping")
				continue
			}

			// Convert balance to bytes
			balanceBytes := arbo.BigIntToBytes(censusRef.Tree().HashFunction().Len(), participant.Balance)

			// Add to current batch
			log.Info().Msgf("Processing participant %s with balance %s for query %s", participant.Address.Hex(), participant.Balance.String(), queryID)
			batch = append(batch, addressKey)
			values = append(values, balanceBytes)

			// Process batch when it reaches the configured size
			if len(batch) >= batchSize {
				if _, err := censusRef.InsertBatch(batch, values); err != nil {
					return totalProcessed, fmt.Errorf("failed to insert batch: %w", err)
				}
				totalProcessed += len(batch)

				// Log progress every 10 seconds
				currentTime := time.Now()
				if currentTime.Sub(lastLogTime) >= 10*time.Second {
					elapsed := currentTime.Sub(startTime)
					rate := float64(totalProcessed) / elapsed.Seconds()
					log.Info().
						Int("processed", totalProcessed).
						Str("elapsed", elapsed.String()).
						Float64("addr_per_sec", rate).
						Str("query", queryID).
						Msg("Census creation progress")
					lastLogTime = currentTime
				}

				// Reset batch
				batch = batch[:0]
				values = values[:0]
			}

		case err := <-errorCh:
			if err != nil {
				return totalProcessed, fmt.Errorf("BigQuery streaming error: %w", err)
			}

		case <-qr.ctx.Done():
			return totalProcessed, fmt.Errorf("context cancelled during census creation")
		}
	}
}

// convertWeightConfig converts config.WeightConfig to bigquery.WeightConfig
func convertWeightConfig(cfg config.WeightConfig) bigquery.WeightConfig {
	return bigquery.WeightConfig{
		Strategy:        cfg.Strategy,
		ConstantWeight:  cfg.ConstantWeight,
		TargetMinWeight: cfg.TargetMinWeight,
		Multiplier:      cfg.Multiplier,
		MaxWeight:       cfg.MaxWeight,
	}
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
