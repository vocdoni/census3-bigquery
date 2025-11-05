// Package service provides the main census synchronization service that coordinates
// query execution, census creation, and snapshot management.
package service

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/vocdoni/census3-bigquery/alchemy"
	"github.com/vocdoni/census3-bigquery/api"
	"github.com/vocdoni/census3-bigquery/bigquery"
	"github.com/vocdoni/census3-bigquery/censusdb"
	"github.com/vocdoni/census3-bigquery/config"
	"github.com/vocdoni/census3-bigquery/storage"
	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/db/metadb"
	"github.com/vocdoni/davinci-node/log"
)

// DataSourceClient defines the interface for data source operations.
type DataSourceClient interface {
	Close() error
}

// BigQueryClient defines the interface for BigQuery operations.
type BigQueryClient interface {
	DataSourceClient
	StreamBalances(ctx context.Context, cfg bigquery.Config, participantCh chan<- bigquery.Participant, errorCh chan<- error)
}

// AlchemyClient defines the interface for Alchemy operations.
type AlchemyClient interface {
	DataSourceClient
	StreamBalances(ctx context.Context, cfg alchemy.Config, participantCh chan<- alchemy.Participant, errorCh chan<- error)
}

// Service coordinates all census synchronization operations including query execution,
// census creation, and snapshot management.
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

// New creates and initializes a new Service instance with all required components.
func New(cfg *config.Config) (*Service, error) {
	log.Infow("initializing service")
	startTime := time.Now()

	ctx, cancel := context.WithCancel(context.Background())

	// Initialize data source clients
	bqClient, alchemyClient, err := initializeDataSourceClients(ctx, cfg)
	if err != nil {
		cancel()
		return nil, err
	}

	// Initialize database and storage
	_, censusDB, kvStorage, err := initializeStorage(cfg)
	if err != nil {
		cancel()
		closeClients(bqClient, alchemyClient)
		return nil, err
	}

	// Purge temporary working censuses from previous runs
	go purgeWorkingCensuses(censusDB)

	// Initialize API server
	apiServer := api.NewServer(kvStorage, censusDB, cfg.APIPort, cfg.MaxCensusSize)
	log.Infow("API server initialized", "port", cfg.APIPort)

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

	// Create query runners
	if err := service.initializeQueryRunners(); err != nil {
		cancel()
		closeClients(bqClient, alchemyClient)
		return nil, err
	}

	log.Infow("service initialized", "duration", time.Since(startTime), "runners", len(service.queryRunners))

	return service, nil
}

// Start begins service operation, starting all query runners and the API server.
func (s *Service) Start() error {
	log.Infow("starting service")

	s.logConfiguration()

	// Start API server
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.apiServer.Start(); err != nil {
			log.Errorw(err, "API server error")
		}
	}()

	// Synchronize query configurations with stored snapshots
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.synchronizeQueries(); err != nil {
			log.Warnw("query synchronization failed", "error", err)
		}
	}()

	// Start query runners
	for i, runner := range s.queryRunners {
		s.wg.Add(1)
		go func(idx int, r *QueryRunner) {
			defer s.wg.Done()
			log.Infow("starting query runner", "index", idx+1, "query", r.config.Name)
			r.run()
		}(i, runner)
	}

	log.Infow("service started")

	// Wait for shutdown signal
	s.waitForShutdown()

	return nil
}

// Stop gracefully shuts down the service, stopping all query runners and closing connections.
func (s *Service) Stop() {
	log.Infow("stopping service")

	s.cancel()
	s.wg.Wait()

	closeClients(s.bigqueryClient, s.alchemyClient)

	log.Infow("service stopped")
}

// initializeDataSourceClients creates BigQuery and Alchemy clients based on query requirements.
func initializeDataSourceClients(ctx context.Context, cfg *config.Config) (BigQueryClient, AlchemyClient, error) {
	var bqClient BigQueryClient
	var alchemyClient AlchemyClient

	// Check if BigQuery is needed
	if hasQueriesForSource(cfg.Queries, "bigquery") {
		client, err := bigquery.NewClient(ctx, cfg.Project)
		if err != nil {
			return nil, nil, fmt.Errorf("bigquery client: %w", err)
		}
		bqClient = client
		log.Infow("BigQuery client initialized")
	}

	// Check if Alchemy is needed
	if hasQueriesForSource(cfg.Queries, "alchemy") {
		client, err := alchemy.NewClient(ctx, cfg.AlchemyAPIKey)
		if err != nil {
			if bqClient != nil {
				_ = bqClient.Close()
			}
			return nil, nil, fmt.Errorf("alchemy client: %w", err)
		}
		alchemyClient = client
		log.Infow("Alchemy client initialized")
	}

	return bqClient, alchemyClient, nil
}

// initializeStorage creates and initializes the database, census DB, and KV storage.
func initializeStorage(cfg *config.Config) (db.Database, *censusdb.CensusDB, *storage.KVSnapshotStorage, error) {
	// Create data directory
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, nil, nil, fmt.Errorf("create data directory: %w", err)
	}

	// Initialize database
	database, err := metadb.New(db.TypePebble, cfg.DataDir)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("initialize database: %w", err)
	}
	log.Infow("database initialized", "path", cfg.DataDir)

	// Initialize census DB
	censusDB := censusdb.NewCensusDB(database)
	log.Infow("census DB initialized")

	// Initialize KV storage
	kvStorage := storage.NewKVSnapshotStorage(database)
	log.Infow("snapshot storage initialized")

	return database, censusDB, kvStorage, nil
}

// initializeQueryRunners creates query runners for all enabled queries.
func (s *Service) initializeQueryRunners() error {
	for i, queryConfig := range s.config.Queries {
		if queryConfig.IsDisabled() {
			log.Infow("query disabled, skipping runner", "query", queryConfig.Name)
			continue
		}

		runner, err := s.createQueryRunner(&queryConfig)
		if err != nil {
			return fmt.Errorf("create runner %d (%s): %w", i+1, queryConfig.Name, err)
		}

		s.queryRunners = append(s.queryRunners, runner)
	}

	log.Infow("query runners initialized", "count", len(s.queryRunners))
	return nil
}

// createQueryRunner creates a new query runner for a specific query configuration.
func (s *Service) createQueryRunner(queryConfig *config.QueryConfig) (*QueryRunner, error) {
	ctx, cancel := context.WithCancel(s.ctx)

	// Validate query exists in appropriate registry
	source := queryConfig.GetSource()
	switch source {
	case "bigquery":
		if _, err := bigquery.GetQuery(queryConfig.Query); err != nil {
			cancel()
			return nil, fmt.Errorf("invalid bigquery query '%s': %w", queryConfig.Query, err)
		}
	case "alchemy":
		if _, err := alchemy.GetQuery(queryConfig.Query); err != nil {
			cancel()
			return nil, fmt.Errorf("invalid alchemy query '%s': %w", queryConfig.Query, err)
		}
	default:
		cancel()
		return nil, fmt.Errorf("unknown source '%s'", source)
	}

	return &QueryRunner{
		config:  queryConfig,
		service: s,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// logConfiguration logs the service configuration for debugging.
func (s *Service) logConfiguration() {
	log.Infow("service configuration", "apiPort", s.config.APIPort, "dataDir", s.config.DataDir, "project", s.config.Project, "batchSize", s.config.BatchSize, "queries", len(s.config.Queries))

	for i, q := range s.config.Queries {
		log.Infow("query configuration", "index", i+1, "name", q.Name, "period", q.Period.String(), "minBalance", q.GetMinBalance())
	}
}

// waitForShutdown blocks until a shutdown signal is received.
func (s *Service) waitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	select {
	case sig := <-sigChan:
		log.Infow("shutdown signal received", "signal", sig.String())
	case <-s.ctx.Done():
		log.Infow("context cancelled")
	}

	signal.Stop(sigChan)
	close(sigChan)

	s.Stop()
}

// hasQueriesForSource checks if any queries use the specified source.
func hasQueriesForSource(queries []config.QueryConfig, source string) bool {
	for _, q := range queries {
		if q.GetSource() == source {
			return true
		}
	}
	return false
}

// closeClients safely closes data source clients.
func closeClients(bq BigQueryClient, alchemy AlchemyClient) {
	if bq != nil {
		if err := bq.Close(); err != nil {
			log.Warnw("failed to close BigQuery client", "error", err)
		}
	}
	if alchemy != nil {
		if err := alchemy.Close(); err != nil {
			log.Warnw("failed to close Alchemy client", "error", err)
		}
	}
}

// purgeWorkingCensuses removes temporary working censuses from previous runs.
func purgeWorkingCensuses(censusDB *censusdb.CensusDB) {
	start := time.Now()
	purged, err := censusDB.PurgeWorkingCensuses(time.Nanosecond)
	if err != nil {
		log.Warnw("failed to purge working censuses", "error", err)
		return
	}

	if purged > 0 {
		log.Infow("purged working censuses", "count", purged, "duration", time.Since(start))
	}
}
