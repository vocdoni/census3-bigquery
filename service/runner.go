package service

import (
	"census3-bigquery/config"
	"census3-bigquery/log"
	"context"
	"time"
)

// QueryRunner executes a single query on a periodic schedule, creating census snapshots.
type QueryRunner struct {
	config  *config.QueryConfig
	service *Service
	ctx     context.Context
	cancel  context.CancelFunc
}

// run executes the query runner's periodic synchronization process.
func (qr *QueryRunner) run() {
	queryName := qr.config.Name

	// Run initial sync if needed
	if qr.shouldRunInitialSync() {
		log.Info().
			Str("query", queryName).
			Bool("sync_on_start", qr.config.GetSyncOnStart()).
			Msg("Running initial sync")

		if err := qr.performSync(); err != nil {
			log.Error().Err(err).Str("query", queryName).Msg("Initial sync failed")
		}
	} else {
		log.Info().Str("query", queryName).Msg("Skipping initial sync")
	}

	// Start periodic sync
	ticker := time.NewTicker(qr.config.Period)
	defer ticker.Stop()

	for {
		select {
		case <-qr.ctx.Done():
			log.Info().Str("query", queryName).Msg("Query runner stopped")
			return

		case <-ticker.C:
			log.Info().Str("query", queryName).Msg("Running periodic sync")
			if err := qr.performSync(); err != nil {
				log.Error().Err(err).Str("query", queryName).Msg("Periodic sync failed")
			}
		}
	}
}

// shouldRunInitialSync determines whether to run the initial sync based on
// configuration and the time since the last snapshot.
func (qr *QueryRunner) shouldRunInitialSync() bool {
	// Always run if explicitly configured
	if qr.config.GetSyncOnStart() {
		return true
	}

	// Check time since last snapshot
	latest, err := qr.service.kvStorage.GetLatestSnapshotByQuery(qr.config.Name)
	if err != nil {
		log.Warn().Err(err).Str("query", qr.config.Name).Msg("Cannot determine last snapshot")
		return true
	}

	if latest == nil {
		return true
	}

	timeSince := time.Since(latest.SnapshotDate)
	shouldRun := timeSince >= qr.config.Period

	log.Debug().
		Str("query", qr.config.Name).
		Time("last_snapshot", latest.SnapshotDate).
		Dur("time_since", timeSince).
		Dur("period", qr.config.Period).
		Bool("should_run", shouldRun).
		Msg("Initial sync check")

	return shouldRun
}
