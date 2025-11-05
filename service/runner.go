package service

import (
	"context"
	"time"

	"github.com/vocdoni/census3-bigquery/config"
	"github.com/vocdoni/davinci-node/log"
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
		log.Infow("running initial sync", "query", queryName, "syncOnStart", qr.config.GetSyncOnStart())

		if err := qr.performSync(); err != nil {
			log.Errorw(err, "initial sync failed")
		}
	} else {
		log.Infow("skipping initial sync", "query", queryName)
	}

	// Start periodic sync
	ticker := time.NewTicker(qr.config.Period)
	defer ticker.Stop()

	for {
		select {
		case <-qr.ctx.Done():
			log.Infow("query runner stopped", "query", queryName)
			return

		case <-ticker.C:
			log.Infow("running periodic sync", "query", queryName)
			if err := qr.performSync(); err != nil {
				log.Errorw(err, "periodic sync failed")
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
		log.Warnw("cannot determine last snapshot", "error", err, "query", qr.config.Name)
		return true
	}

	if latest == nil {
		return true
	}

	timeSince := time.Since(latest.SnapshotDate)
	shouldRun := timeSince >= qr.config.Period

	log.Debugw("initial sync check", "query", qr.config.Name, "lastSnapshot", latest.SnapshotDate, "timeSince", timeSince, "period", qr.config.Period, "shouldRun", shouldRun)

	return shouldRun
}
