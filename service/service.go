package service

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vocdoni/arbo"
	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/db/metadb"
	"github.com/vocdoni/davinci-node/types"

	"census3-bigquery/alchemy"
	"census3-bigquery/api"
	"census3-bigquery/bigquery"
	"census3-bigquery/censusdb"
	"census3-bigquery/config"
	"census3-bigquery/log"
	"census3-bigquery/metadata"
	"census3-bigquery/storage"
)

// DataSourceClient interface for data source operations (BigQuery or Alchemy)
type DataSourceClient interface {
	Close() error
}

// BigQueryClient interface for BigQuery operations
type BigQueryClient interface {
	DataSourceClient
	StreamBalances(ctx context.Context, cfg bigquery.Config, participantCh chan<- bigquery.Participant, errorCh chan<- error)
}

// AlchemyClient interface for Alchemy operations
type AlchemyClient interface {
	DataSourceClient
	StreamBalances(ctx context.Context, cfg alchemy.Config, participantCh chan<- alchemy.Participant, errorCh chan<- error)
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
	alchemyClient  AlchemyClient
	censusDB       *censusdb.CensusDB
	apiServer      *api.Server
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	queryRunners   []*QueryRunner
}

// New creates a new service instance
func New(cfg *config.Config) (*Service, error) {
	log.Info().Msg("Starting service initialization...")
	startTime := time.Now()
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize BigQuery client if needed
	var bqClient BigQueryClient
	hasBigQueryQueries := false
	for _, query := range cfg.Queries {
		if query.GetSource() == "bigquery" {
			hasBigQueryQueries = true
			break
		}
	}

	if hasBigQueryQueries {
		log.Info().Msg("Creating BigQuery client...")
		bqStartTime := time.Now()
		client, err := bigquery.NewClient(ctx, cfg.Project)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
		}
		bqClient = client
		log.Info().
			Str("duration", time.Since(bqStartTime).String()).
			Msg("BigQuery client created successfully")
	}

	// Initialize Alchemy client if needed
	var alchemyClient AlchemyClient
	hasAlchemyQueries := false
	for _, query := range cfg.Queries {
		if query.GetSource() == "alchemy" {
			hasAlchemyQueries = true
			break
		}
	}

	if hasAlchemyQueries {
		log.Info().Msg("Creating Alchemy client...")
		alchemyStartTime := time.Now()
		client, err := alchemy.NewClient(ctx, cfg.AlchemyAPIKey)
		if err != nil {
			cancel()
			if bqClient != nil {
				if closeErr := bqClient.Close(); closeErr != nil {
					log.Warn().Err(closeErr).Msg("Failed to close BigQuery client")
				}
			}
			return nil, fmt.Errorf("failed to create Alchemy client: %w", err)
		}
		alchemyClient = client
		log.Info().
			Str("duration", time.Since(alchemyStartTime).String()).
			Msg("Alchemy client created successfully")
	}

	// Initialize shared database for both census and snapshots
	log.Info().
		Str("data_dir", cfg.DataDir).
		Msg("Creating data directory...")
	dataDir := cfg.DataDir
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		cancel()
		if closeErr := bqClient.Close(); closeErr != nil {
			log.Warn().Err(closeErr).Msg("Failed to close BigQuery client")
		}
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	log.Info().Msg("Data directory created successfully")

	log.Info().
		Str("data_dir", dataDir).
		Msg("Initializing Pebble database...")
	dbStartTime := time.Now()
	database, err := metadb.New(db.TypePebble, dataDir)
	if err != nil {
		cancel()
		if bqClient != nil {
			if closeErr := bqClient.Close(); closeErr != nil {
				log.Warn().Err(closeErr).Msg("Failed to close BigQuery client")
			}
		}
		if alchemyClient != nil {
			if closeErr := alchemyClient.Close(); closeErr != nil {
				log.Warn().Err(closeErr).Msg("Failed to close Alchemy client")
			}
		}
		return nil, fmt.Errorf("failed to create database: %w", err)
	}
	log.Info().
		Str("duration", time.Since(dbStartTime).String()).
		Msg("Database initialized successfully")

	// Initialize CensusDB with shared database
	log.Info().Msg("Creating CensusDB...")
	censusDBStartTime := time.Now()
	censusDB := censusdb.NewCensusDB(database)
	log.Info().
		Str("duration", time.Since(censusDBStartTime).String()).
		Msg("CensusDB created successfully")

	// Initialize KV storage with shared database (using prefixed database)
	log.Info().Msg("Creating KV snapshot storage...")
	kvStartTime := time.Now()
	kvStorage := storage.NewKVSnapshotStorage(database)
	log.Info().
		Str("duration", time.Since(kvStartTime).String()).
		Msg("KV snapshot storage created successfully")

	// Purge all working censuses at startup (they are temporary and should not persist)
	log.Info().Msg("Starting background working census purge...")
	go func() {
		purgeStartTime := time.Now()
		// Use a very small max age to purge all working censuses
		maxAge := 1 * time.Nanosecond
		log.Debug().Msg("Purging working censuses...")
		if purged, err := censusDB.PurgeWorkingCensuses(maxAge); err != nil {
			log.Warn().Err(err).Msg("Failed to purge working censuses at startup")
		} else if purged > 0 {
			log.Info().
				Int("purged_count", purged).
				Str("duration", time.Since(purgeStartTime).String()).
				Msg("Purged all working censuses at startup")
		} else {
			log.Info().
				Str("duration", time.Since(purgeStartTime).String()).
				Msg("No working censuses to purge")
		}
	}()

	// Initialize API server with KV storage and censusDB
	log.Info().
		Int("api_port", cfg.APIPort).
		Msg("Creating API server...")
	apiStartTime := time.Now()
	apiServer := api.NewServer(kvStorage, censusDB, cfg.APIPort, cfg.MaxCensusSize)
	log.Info().
		Str("duration", time.Since(apiStartTime).String()).
		Msg("API server created successfully")

	service := &Service{
		config:         cfg,
		kvStorage:      kvStorage,
		bigqueryClient: bqClient,
		alchemyClient:  alchemyClient,
		censusDB:       censusDB,
		apiServer:      apiServer,
		ctx:            ctx,
		cancel:         cancel,
	}

	// Create query runners for each enabled query configuration
	log.Info().
		Int("query_count", len(cfg.Queries)).
		Msg("Creating query runners...")
	for i, queryConfig := range cfg.Queries {
		if queryConfig.IsDisabled() {
			log.Info().
				Str("query", queryConfig.Name).
				Msg("Query is disabled, skipping runner creation but keeping snapshots accessible")
			continue
		}

		runner, err := service.createQueryRunner(&queryConfig)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create query runner %d (%s): %w", i+1, queryConfig.Name, err)
		}
		service.queryRunners = append(service.queryRunners, runner)
		log.Info().
			Str("query", queryConfig.Name).
			Int("runner_index", i+1).
			Msg("Query runner created successfully")
	}

	log.Info().
		Str("total_duration", time.Since(startTime).String()).
		Int("query_runners", len(service.queryRunners)).
		Msg("Service initialization completed successfully")

	return service, nil
}

// createQueryRunner creates a new query runner for a specific query configuration
func (s *Service) createQueryRunner(queryConfig *config.QueryConfig) (*QueryRunner, error) {
	ctx, cancel := context.WithCancel(s.ctx)

	// Validate that the query exists in the appropriate registry
	source := queryConfig.GetSource()
	switch source {
	case "bigquery":
		if _, err := bigquery.GetQuery(queryConfig.Query); err != nil {
			cancel()
			return nil, fmt.Errorf("invalid BigQuery query '%s' for config '%s': %w", queryConfig.Query, queryConfig.Name, err)
		}
	case "alchemy":
		if _, err := alchemy.GetQuery(queryConfig.Query); err != nil {
			cancel()
			return nil, fmt.Errorf("invalid Alchemy query '%s' for config '%s': %w", queryConfig.Query, queryConfig.Name, err)
		}
	default:
		cancel()
		return nil, fmt.Errorf("unknown source '%s' for config '%s'", source, queryConfig.Name)
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
	log.Info().Msg("Starting API server...")
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		log.Info().Msg("API server goroutine started, binding to port...")
		if err := s.apiServer.Start(); err != nil {
			log.Error().Err(err).Msg("API server error")
		}
	}()

	// Start background query synchronization (moved from service initialization)
	log.Info().Msg("Starting background query synchronization...")
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		syncStartTime := time.Now()
		log.Info().Msg("Running query synchronization in background...")
		if err := s.synchronizeQueries(); err != nil {
			log.Warn().
				Err(err).
				Str("duration", time.Since(syncStartTime).String()).
				Msg("Failed to synchronize queries with storage")
		} else {
			log.Info().
				Str("duration", time.Since(syncStartTime).String()).
				Msg("Query synchronization completed successfully")
		}
	}()

	// Start each query runner in its own goroutine
	log.Info().
		Int("runner_count", len(s.queryRunners)).
		Msg("Starting query runners...")
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

	log.Info().Msg("All service components started successfully")

	// Wait for shutdown signal
	s.waitForShutdown()

	return nil
}

// Stop stops the service gracefully
func (s *Service) Stop() {
	log.Info().Msg("Stopping census3-bigquery service")
	s.cancel()
	s.wg.Wait()

	if s.bigqueryClient != nil {
		if err := s.bigqueryClient.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing BigQuery client")
		}
	}

	if s.alchemyClient != nil {
		if err := s.alchemyClient.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing Alchemy client")
		}
	}

	log.Info().Msg("Service stopped")
}

// run executes the query runner's periodic sync process
func (qr *QueryRunner) run() {
	queryID := qr.config.Name

	// Check if we should run initial sync
	if qr.shouldRunInitialSync() {
		log.Info().
			Str("query", queryID).
			Bool("syncOnStart", qr.config.GetSyncOnStart()).
			Msg("Running initial sync")

		if err := qr.performSync(); err != nil {
			log.Error().
				Err(err).
				Str("query", queryID).
				Msg("Initial sync failed")
		}
	} else {
		log.Info().
			Str("query", queryID).
			Msg("Skipping initial sync - period has not elapsed since last snapshot")
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

// shouldRunInitialSync determines if the initial sync should run based on syncOnStart and period timing
func (qr *QueryRunner) shouldRunInitialSync() bool {
	if qr.config.GetSyncOnStart() {
		log.Debug().
			Str("query", qr.config.Name).
			Msg("syncOnStart is true, running initial sync")
		return true // Always run if syncOnStart is true
	}

	// Check if enough time has passed since last snapshot
	latest, err := qr.service.kvStorage.GetLatestSnapshotByQuery(qr.config.Name)
	if err != nil {
		log.Warn().
			Err(err).
			Str("query", qr.config.Name).
			Msg("Failed to get latest snapshot, running initial sync")
		return true // Run sync if we can't determine last snapshot time
	}

	if latest == nil {
		log.Debug().
			Str("query", qr.config.Name).
			Msg("No previous snapshots found, running initial sync")
		return true // No previous snapshots, run sync
	}

	timeSinceLastSnapshot := time.Since(latest.SnapshotDate)
	shouldRun := timeSinceLastSnapshot >= qr.config.Period

	log.Debug().
		Str("query", qr.config.Name).
		Time("last_snapshot", latest.SnapshotDate).
		Str("time_since_last", timeSinceLastSnapshot.String()).
		Str("period", qr.config.Period.String()).
		Bool("should_run", shouldRun).
		Msg("Checking if initial sync should run based on period timing")

	return shouldRun
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

	// Step 2: Stream data from data source and create census
	source := qr.config.GetSource()
	log.Info().
		Str("query", queryID).
		Str("source", source).
		Msg("Streaming data from data source and creating census...")

	var actualCount int

	switch source {
	case "bigquery":
		bqConfig := bigquery.Config{
			Project:         qr.service.config.Project,
			MinBalance:      minBalance,
			QueryName:       qr.config.Query,
			QueryParams:     qr.config.Parameters,
			Decimals:        qr.config.GetDecimals(),
			WeightConfig:    convertWeightConfig(qr.config.GetWeightConfig()),
			EstimateFirst:   qr.config.GetEstimateFirst(),
			CostPreset:      qr.config.GetCostPreset(),
			BigQueryPricing: convertBigQueryPricing(qr.config.GetBigQueryPricing()),
		}
		actualCount, err = qr.streamAndCreateCensusBigQuery(censusRef, bqConfig)
	case "alchemy":
		// Get contract address from parameters
		contractAddress := ""
		if addr, ok := qr.config.Parameters["contract_address"]; ok {
			contractAddress = fmt.Sprintf("%v", addr)
		}

		// For multi_nft_holders, pass all parameters including contract_addresses
		alchemyConfig := alchemy.Config{
			APIKey:          qr.service.config.AlchemyAPIKey,
			Network:         qr.config.GetNetwork(),
			ContractAddress: contractAddress,
			MinBalance:      minBalance,
			QueryName:       qr.config.Query,
			QueryParams:     qr.config.Parameters, // This includes contract_addresses for multi queries
			WeightConfig:    convertAlchemyWeightConfig(qr.config.GetWeightConfig()),
			PageSize:        100, // Default page size
		}
		actualCount, err = qr.streamAndCreateCensusAlchemy(censusRef, alchemyConfig)
	default:
		return fmt.Errorf("unknown source '%s' for query %s", source, queryID)
	}

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
		Msg("Working census created successfully")

	// Step 4: Create new empty census identified by root
	log.Info().
		Str("census_root", fmt.Sprintf("0x%x", censusRoot)).
		Str("query", queryID).
		Msg("Creating root-based census")

	rootCensusRef, err := qr.service.censusDB.NewByRoot(censusRoot)
	if err != nil {
		// If census already exists, log error but continue
		if err == censusdb.ErrCensusAlreadyExists {
			log.Error().
				Str("census_root", fmt.Sprintf("0x%x", censusRoot)).
				Str("query", queryID).
				Msg("Root-based census already exists, continuing")
		} else {
			return fmt.Errorf("failed to create root-based census for query %s: %w", queryID, err)
		}
		// Load the existing root-based census
		rootCensusRef, err = qr.service.censusDB.LoadByRoot(censusRoot)
		if err != nil {
			return fmt.Errorf("failed to load existing root-based census for query %s: %w", queryID, err)
		}
	}

	// Step 5: Export data from working census and import to root-based census
	log.Info().
		Str("query", queryID).
		Msg("Transferring data from working census to root-based census")

	if err := qr.service.censusDB.PublishCensus(censusID, rootCensusRef); err != nil {
		return fmt.Errorf("failed to publish census for query %s: %w", queryID, err)
	}

	// Step 6: Verify the root matches
	finalRoot := rootCensusRef.Root()
	if !bytes.Equal(censusRoot, finalRoot) {
		return fmt.Errorf("root verification failed for query %s: expected %x, got %x", queryID, censusRoot, finalRoot)
	}

	log.Info().
		Str("census_root", fmt.Sprintf("0x%x", censusRoot)).
		Str("query", queryID).
		Msg("Root verification successful")

	// Step 7: Process Farcaster metadata if enabled
	if qr.config.HasFarcasterMetadata() {
		log.Info().
			Str("query", queryID).
			Str("census_root", fmt.Sprintf("0x%x", censusRoot)).
			Msg("Processing Farcaster metadata")

		farcasterConfig := qr.config.GetFarcasterConfig()
		farcasterProcessor := metadata.NewFarcasterProcessor(nil, qr.service.kvStorage)

		// Extract original addresses and weights from the stored address list
		addresses, weights, err := qr.extractOriginalAddressesAndWeights(types.HexBytes(censusRoot))
		if err != nil {
			log.Warn().
				Err(err).
				Str("query", queryID).
				Str("census_root", fmt.Sprintf("0x%x", censusRoot)).
				Msg("Failed to extract original addresses for Farcaster metadata, skipping")
		} else {
			if err := farcasterProcessor.ProcessCensus(qr.ctx, types.HexBytes(censusRoot), addresses, weights, farcasterConfig); err != nil {
				// Log error but don't fail the entire sync - metadata is optional
				log.Warn().
					Err(err).
					Str("query", queryID).
					Str("census_root", fmt.Sprintf("0x%x", censusRoot)).
					Msg("Failed to process Farcaster metadata, continuing without metadata")
			} else {
				log.Info().
					Str("query", queryID).
					Str("census_root", fmt.Sprintf("0x%x", censusRoot)).
					Msg("Farcaster metadata processed successfully")
			}
		}
	}

	// Step 8: Schedule cleanup of working census in goroutine
	go func(workingCensusID uuid.UUID, query string) {
		if err := qr.service.censusDB.CleanupWorkingCensus(workingCensusID); err != nil {
			log.Error().
				Str("census_id", workingCensusID.String()).
				Str("query", query).
				Err(err).
				Msg("Failed to cleanup working census")
		}
	}(censusID, queryID)

	// Step 9: Store snapshot in KV storage
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

	// Sanitize parameters to convert []interface{} to concrete types for Gob encoding
	sanitizedParams := make(map[string]interface{})
	for k, v := range qr.config.Parameters {
		switch val := v.(type) {
		case []interface{}:
			// Convert []interface{} to []string
			strSlice := make([]string, len(val))
			for i, item := range val {
				strSlice[i] = fmt.Sprintf("%v", item)
			}
			sanitizedParams[k] = strSlice
		default:
			sanitizedParams[k] = v
		}
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
		sanitizedParams,
		storageWeightConfig,
		qr.config.GetDisplayName(),
		qr.config.GetDisplayAvatar(),
	); err != nil {
		return fmt.Errorf("failed to store snapshot for query %s: %w", queryID, err)
	}

	log.Info().
		Str("query_id", queryID).
		Float64("min_balance", minBalance).
		Msg("Snapshot stored successfully to KV storage")

	// Step 9: Cleanup old snapshots if configured
	snapshotsToKeep := qr.config.GetSnapshotsToKeep()
	if snapshotsToKeep > 0 {
		// Schedule cleanup in a goroutine to avoid blocking
		go func() {
			log.Info().
				Str("query", queryID).
				Int("snapshots_to_keep", snapshotsToKeep).
				Msg("Starting cleanup of old snapshots")

			deletedCount, censusRootsToDelete, err := qr.service.kvStorage.DeleteOldSnapshotsByQuery(queryID, snapshotsToKeep)
			if err != nil {
				log.Error().
					Err(err).
					Str("query", queryID).
					Msg("Failed to delete old snapshots")
				return
			}

			if deletedCount > 0 {
				log.Info().
					Str("query", queryID).
					Int("deleted_snapshots", deletedCount).
					Int("snapshots_kept", snapshotsToKeep).
					Msg("Successfully deleted old snapshots")

				// Delete the associated census data and address lists
				for _, censusRoot := range censusRootsToDelete {
					// Delete census data
					if qr.service.censusDB.ExistsByRoot(censusRoot) {
						censusID := uuid.NewSHA1(uuid.NameSpaceOID, censusRoot)
						if err := qr.service.censusDB.Del(censusID); err != nil {
							log.Error().
								Err(err).
								Str("census_root", fmt.Sprintf("0x%x", censusRoot)).
								Str("query", queryID).
								Msg("Failed to delete census data for old snapshot")
						} else {
							log.Debug().
								Str("census_root", fmt.Sprintf("0x%x", censusRoot)).
								Str("query", queryID).
								Msg("Deleted census data for old snapshot")
						}
					}

					// Delete address list if it exists
					if hasAddressList, err := qr.service.kvStorage.HasAddressList(censusRoot); err == nil && hasAddressList {
						if err := qr.service.kvStorage.DeleteAddressList(censusRoot); err != nil {
							log.Error().
								Err(err).
								Str("census_root", fmt.Sprintf("0x%x", censusRoot)).
								Str("query", queryID).
								Msg("Failed to delete address list for old snapshot")
						} else {
							log.Debug().
								Str("census_root", fmt.Sprintf("0x%x", censusRoot)).
								Str("query", queryID).
								Msg("Deleted address list for old snapshot")
						}
					}
				}
			}
		}()
	}

	return nil
}

// streamAndCreateCensusBigQuery streams participants from BigQuery and creates census in batches
func (qr *QueryRunner) streamAndCreateCensusBigQuery(censusRef *censusdb.CensusRef, bqConfig bigquery.Config) (int, error) {
	// Determine batch size
	batchSize := qr.service.config.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	// Collect addresses in memory during streaming (we'll store them with the final census root)
	var collectedAddresses []string
	storeAddresses := qr.config.GetStoreAddresses()

	// Create channels for streaming
	participantCh := make(chan bigquery.Participant, batchSize)
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

				// Store collected addresses with the final census root
				if storeAddresses && len(collectedAddresses) > 0 {
					finalCensusRoot := types.HexBytes(censusRef.Root())
					addressCollector := NewAddressCollector(
						qr.service.kvStorage,
						finalCensusRoot,
						storage.DefaultAddressPageSize,
						true, // enabled
					)

					// Add all collected addresses
					for _, address := range collectedAddresses {
						if err := addressCollector.AddAddress(address); err != nil {
							log.Warn().
								Err(err).
								Str("address", address).
								Str("query", queryID).
								Msg("Failed to store collected address")
						}
					}

					// Finalize address collection
					if err := addressCollector.Finalize(); err != nil {
						log.Warn().
							Err(err).
							Str("query", queryID).
							Msg("Failed to finalize address collection")
					} else {
						log.Info().
							Str("query", queryID).
							Str("census_root", finalCensusRoot.String()).
							Int("addresses_stored", len(collectedAddresses)).
							Msg("Successfully stored addresses for metadata processing")
					}
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

			// Collect original address for metadata processing (in memory)
			if storeAddresses {
				collectedAddresses = append(collectedAddresses, participant.Address.Hex())
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
				batch = [][]byte{}
				values = [][]byte{}
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

// synchronizeQueries compares YAML query configurations with stored snapshots
func (s *Service) synchronizeQueries() error {
	log.Info().Msg("Synchronizing query configurations with storage")

	for _, queryConfig := range s.config.Queries {
		if queryConfig.IsDisabled() {
			log.Info().
				Str("query", queryConfig.Name).
				Msg("Query is disabled, skipping synchronization")
			continue
		}

		// Get latest snapshot for this query
		latest, err := s.kvStorage.GetLatestSnapshotByQuery(queryConfig.Name)
		if err != nil {
			log.Warn().
				Err(err).
				Str("query", queryConfig.Name).
				Msg("Failed to get latest snapshot for query")
			continue
		}

		if latest == nil {
			log.Info().
				Str("query", queryConfig.Name).
				Msg("No previous snapshots found for query")
			continue
		}

		// Compare configurations and log differences
		s.logConfigurationChanges(&queryConfig, latest)

		// Check if we need to cleanup old snapshots based on current configuration
		snapshotsToKeep := queryConfig.GetSnapshotsToKeep()
		if snapshotsToKeep > 0 {
			// Get all snapshots for this query to check if we have too many
			snapshots, err := s.kvStorage.SnapshotsByQuery(queryConfig.Name)
			if err != nil {
				log.Warn().
					Err(err).
					Str("query", queryConfig.Name).
					Msg("Failed to get snapshots for cleanup check")
				continue
			}

			if len(snapshots) > snapshotsToKeep {
				log.Info().
					Str("query", queryConfig.Name).
					Int("current_count", len(snapshots)).
					Int("snapshots_to_keep", snapshotsToKeep).
					Msg("Query has more snapshots than configured limit, cleaning up")

				// Perform cleanup
				deletedCount, censusRootsToDelete, err := s.kvStorage.DeleteOldSnapshotsByQuery(queryConfig.Name, snapshotsToKeep)
				if err != nil {
					log.Error().
						Err(err).
						Str("query", queryConfig.Name).
						Msg("Failed to delete old snapshots during startup cleanup")
					continue
				}

				if deletedCount > 0 {
					log.Info().
						Str("query", queryConfig.Name).
						Int("deleted_snapshots", deletedCount).
						Int("snapshots_kept", snapshotsToKeep).
						Msg("Successfully deleted old snapshots during startup cleanup")

					// Delete the associated census data
					for _, censusRoot := range censusRootsToDelete {
						if s.censusDB.ExistsByRoot(censusRoot) {
							censusID := uuid.NewSHA1(uuid.NameSpaceOID, censusRoot)
							if err := s.censusDB.Del(censusID); err != nil {
								log.Error().
									Err(err).
									Str("census_root", fmt.Sprintf("0x%x", censusRoot)).
									Str("query", queryConfig.Name).
									Msg("Failed to delete census data for old snapshot during startup cleanup")
							} else {
								log.Debug().
									Str("census_root", fmt.Sprintf("0x%x", censusRoot)).
									Str("query", queryConfig.Name).
									Msg("Deleted census data for old snapshot during startup cleanup")
							}
						}
					}
				}
			}
		}
	}

	return nil
}

// logConfigurationChanges compares current config with stored snapshot and logs differences
func (s *Service) logConfigurationChanges(current *config.QueryConfig, stored *storage.KVSnapshot) {
	changes := []string{}

	// Compare period
	currentPeriod := current.Period.String()
	if currentPeriod != stored.Period {
		changes = append(changes, fmt.Sprintf("period: %s -> %s", stored.Period, currentPeriod))
	}

	// Compare decimals
	currentDecimals := current.GetDecimals()
	if currentDecimals != stored.Decimals {
		changes = append(changes, fmt.Sprintf("decimals: %d -> %d", stored.Decimals, currentDecimals))
	}

	// Compare min_balance
	currentMinBalance := current.GetMinBalance()
	if currentMinBalance != stored.MinBalance {
		changes = append(changes, fmt.Sprintf("min_balance: %.6f -> %.6f", stored.MinBalance, currentMinBalance))
	}

	// Compare weight configuration
	currentWeight := current.GetWeightConfig()
	if stored.WeightConfig != nil {
		if currentWeight.Strategy != stored.WeightConfig.Strategy {
			changes = append(changes, fmt.Sprintf("weight.strategy: %s -> %s", stored.WeightConfig.Strategy, currentWeight.Strategy))
		}

		// Compare strategy-specific fields
		switch currentWeight.Strategy {
		case "constant":
			if currentWeight.ConstantWeight != nil && stored.WeightConfig.ConstantWeight != nil {
				if *currentWeight.ConstantWeight != *stored.WeightConfig.ConstantWeight {
					changes = append(changes, fmt.Sprintf("weight.constant_weight: %d -> %d", *stored.WeightConfig.ConstantWeight, *currentWeight.ConstantWeight))
				}
			}
		case "proportional_auto":
			if currentWeight.TargetMinWeight != nil && stored.WeightConfig.TargetMinWeight != nil {
				if *currentWeight.TargetMinWeight != *stored.WeightConfig.TargetMinWeight {
					changes = append(changes, fmt.Sprintf("weight.target_min_weight: %d -> %d", *stored.WeightConfig.TargetMinWeight, *currentWeight.TargetMinWeight))
				}
			}
		case "proportional_manual":
			if currentWeight.Multiplier != nil && stored.WeightConfig.Multiplier != nil {
				if *currentWeight.Multiplier != *stored.WeightConfig.Multiplier {
					changes = append(changes, fmt.Sprintf("weight.multiplier: %.2f -> %.2f", *stored.WeightConfig.Multiplier, *currentWeight.Multiplier))
				}
			}
		}

		// Compare max_weight
		if currentWeight.MaxWeight != nil && stored.WeightConfig.MaxWeight != nil {
			if *currentWeight.MaxWeight != *stored.WeightConfig.MaxWeight {
				changes = append(changes, fmt.Sprintf("weight.max_weight: %d -> %d", *stored.WeightConfig.MaxWeight, *currentWeight.MaxWeight))
			}
		} else if (currentWeight.MaxWeight == nil) != (stored.WeightConfig.MaxWeight == nil) {
			if currentWeight.MaxWeight != nil {
				changes = append(changes, fmt.Sprintf("weight.max_weight: none -> %d", *currentWeight.MaxWeight))
			} else {
				changes = append(changes, fmt.Sprintf("weight.max_weight: %d -> none", *stored.WeightConfig.MaxWeight))
			}
		}
	}

	// Log changes if any
	if len(changes) > 0 {
		log.Info().
			Str("query", current.Name).
			Strs("changes", changes).
			Time("last_snapshot", stored.SnapshotDate).
			Msg("Configuration changes detected since last snapshot")
	} else {
		log.Debug().
			Str("query", current.Name).
			Time("last_snapshot", stored.SnapshotDate).
			Msg("No configuration changes detected")
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

// convertBigQueryPricing converts config.BigQueryPricing to bigquery.BigQueryPricing
func convertBigQueryPricing(cfg *config.BigQueryPricing) *bigquery.BigQueryPricing {
	if cfg == nil {
		return nil
	}
	return &bigquery.BigQueryPricing{
		PricePerTBProcessed: cfg.PricePerTBProcessed,
	}
}

// streamAndCreateCensusAlchemy streams participants from Alchemy and creates census in batches
func (qr *QueryRunner) streamAndCreateCensusAlchemy(censusRef *censusdb.CensusRef, alchemyConfig alchemy.Config) (int, error) {
	// Determine batch size
	batchSize := qr.service.config.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	// Collect addresses in memory during streaming (we'll store them with the final census root)
	var collectedAddresses []string
	storeAddresses := qr.config.GetStoreAddresses()

	// Create channels for streaming
	participantCh := make(chan alchemy.Participant, batchSize)
	errorCh := make(chan error, 1)

	// Start Alchemy streaming in a goroutine
	go qr.service.alchemyClient.StreamBalances(qr.ctx, alchemyConfig, participantCh, errorCh)

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

				// Store collected addresses with the final census root
				if storeAddresses && len(collectedAddresses) > 0 {
					finalCensusRoot := types.HexBytes(censusRef.Root())
					addressCollector := NewAddressCollector(
						qr.service.kvStorage,
						finalCensusRoot,
						storage.DefaultAddressPageSize,
						true, // enabled
					)

					// Add all collected addresses
					for _, address := range collectedAddresses {
						if err := addressCollector.AddAddress(address); err != nil {
							log.Warn().
								Err(err).
								Str("address", address).
								Str("query", queryID).
								Msg("Failed to store collected address")
						}
					}

					// Finalize address collection
					if err := addressCollector.Finalize(); err != nil {
						log.Warn().
							Err(err).
							Str("query", queryID).
							Msg("Failed to finalize address collection")
					} else {
						log.Info().
							Str("query", queryID).
							Str("census_root", finalCensusRoot.String()).
							Int("addresses_stored", len(collectedAddresses)).
							Msg("Successfully stored addresses for metadata processing")
					}
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

			// Collect original address for metadata processing (in memory)
			if storeAddresses {
				collectedAddresses = append(collectedAddresses, participant.Address.Hex())
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
				batch = [][]byte{}
				values = [][]byte{}
			}

		case err := <-errorCh:
			if err != nil {
				return totalProcessed, fmt.Errorf("alchemy streaming error: %w", err)
			}

		case <-qr.ctx.Done():
			return totalProcessed, fmt.Errorf("context cancelled during census creation")
		}
	}
}

// convertAlchemyWeightConfig converts config.WeightConfig to alchemy.WeightConfig
func convertAlchemyWeightConfig(cfg config.WeightConfig) alchemy.WeightConfig {
	return alchemy.WeightConfig{
		Strategy:        cfg.Strategy,
		ConstantWeight:  cfg.ConstantWeight,
		TargetMinWeight: cfg.TargetMinWeight,
		Multiplier:      cfg.Multiplier,
		MaxWeight:       cfg.MaxWeight,
	}
}

// extractOriginalAddressesAndWeights extracts original addresses from stored address list
func (qr *QueryRunner) extractOriginalAddressesAndWeights(censusRoot types.HexBytes) ([]string, map[string]float64, error) {
	// Check if address list exists for this census
	hasAddressList, err := qr.service.kvStorage.HasAddressList(censusRoot)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to check address list existence: %w", err)
	}

	if !hasAddressList {
		return nil, nil, fmt.Errorf("no address list found for census root - address storage may be disabled")
	}

	// Get all stored addresses
	addresses, err := qr.service.kvStorage.GetAllAddresses(censusRoot)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get stored addresses: %w", err)
	}

	if len(addresses) == 0 {
		return nil, nil, fmt.Errorf("address list is empty")
	}

	// For now, we'll create a simple weight map with equal weights
	// In a more sophisticated implementation, we could store weights alongside addresses
	weights := make(map[string]float64)
	for _, address := range addresses {
		weights[address] = 1.0 // Default weight - this could be improved
	}

	log.Debug().
		Str("census_root", censusRoot.String()).
		Int("addresses_loaded", len(addresses)).
		Msg("Successfully loaded addresses from storage for metadata processing")

	return addresses, weights, nil
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
