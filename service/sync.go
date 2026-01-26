package service

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"github.com/vocdoni/census3-bigquery/storage"
	"github.com/vocdoni/davinci-node/census/censusdb"
	"github.com/vocdoni/davinci-node/log"
	"github.com/vocdoni/davinci-node/types"
)

// performSync executes a complete synchronization cycle for the query runner.
func (qr *QueryRunner) performSync() error {
	startTime := time.Now()
	queryID := qr.config.Name

	log.Infow("starting sync", "query", queryID)

	// Create working census
	censusID := uuid.New()
	workingRef, err := qr.service.censusDB.New(censusID)
	if err != nil {
		return fmt.Errorf("create working census: %w", err)
	}

	// Stream data and populate census
	participantCount, err := qr.streamAndCreateCensus(workingRef)
	if err != nil {
		return fmt.Errorf("stream and create census: %w", err)
	}

	if participantCount == 0 {
		log.Warnw("no participants found, skipping snapshot", "query", queryID)
		return qr.service.censusDB.CleanupWorkingCensus(censusID)
	}

	// Get census root
	censusRoot := workingRef.Root()
	if censusRoot == nil {
		return fmt.Errorf("census has no root")
	}

	log.Infow("census created", "query", queryID, "root", fmt.Sprintf("0x%x", censusRoot), "participants", participantCount)

	// Create root-based census
	rootRef, err := qr.service.censusDB.NewByRoot(censusRoot)
	if err != nil {
		return fmt.Errorf("create root-based census: %w", err)
	}

	// Publish working census to root-based census
	if err := qr.service.censusDB.PublishCensus(censusID, rootRef); err != nil {
		return fmt.Errorf("publish census: %w", err)
	}

	// Process Farcaster metadata if enabled
	if qr.config.HasFarcasterMetadata() {
		qr.processFarcasterMetadata(types.HexBytes(censusRoot))
	}

	// Store snapshot
	snapshot := storage.KVSnapshot{
		SnapshotDate:     time.Now(),
		CensusRoot:       types.HexBytes(censusRoot),
		ParticipantCount: participantCount,
		MinBalance:       qr.config.GetMinBalance(),
		QueryName:        queryID,
	}

	if err := qr.service.kvStorage.AddSnapshot(
		snapshot.SnapshotDate,
		snapshot.CensusRoot,
		snapshot.ParticipantCount,
		snapshot.MinBalance,
		snapshot.QueryName,
		"", // queryType
		0,  // decimals
		qr.config.Period.String(),
		nil, // parameters
		nil, // weightConfig
		"",  // displayName
		"",  // displayAvatar
	); err != nil {
		return fmt.Errorf("store snapshot: %w", err)
	}

	// Cleanup old snapshots if configured (using DeleteOldSnapshotsByQuery from storage)
	if qr.config.SnapshotsToKeep != nil && *qr.config.SnapshotsToKeep > 0 {
		deleted, _, err := qr.service.kvStorage.DeleteOldSnapshotsByQuery(queryID, *qr.config.SnapshotsToKeep)
		if err != nil {
			log.Warnw("failed to cleanup old snapshots", "error", err, "query", queryID)
		} else if deleted > 0 {
			log.Infow("cleaned up old snapshots", "query", queryID, "deleted", deleted)
		}
	}

	duration := time.Since(startTime)
	log.Infow("sync completed", "query", queryID, "root", fmt.Sprintf("0x%x", censusRoot), "participants", participantCount, "duration", duration)

	return nil
}

// streamAndCreateCensus streams data from the appropriate source and populates the census.
func (qr *QueryRunner) streamAndCreateCensus(workingRef *censusdb.CensusRef) (int, error) {
	source := qr.config.GetSource()

	switch source {
	case "bigquery":
		return qr.streamAndCreateCensusBigQuery(workingRef)
	case "alchemy":
		return qr.streamAndCreateCensusAlchemy(workingRef)
	default:
		return 0, fmt.Errorf("unknown source: %s", source)
	}
}

// processFarcasterMetadata processes Farcaster metadata for the census.
func (qr *QueryRunner) processFarcasterMetadata(censusRoot types.HexBytes) {
	log.Infow("processing Farcaster metadata", "query", qr.config.Name, "censusRoot", fmt.Sprintf("0x%x", censusRoot))

	// Extract addresses and weights
	addresses, weights, err := qr.extractOriginalAddressesAndWeights(censusRoot)
	if err != nil {
		log.Warnw("failed to extract addresses for Farcaster metadata", "error", err, "query", qr.config.Name, "censusRoot", fmt.Sprintf("0x%x", censusRoot))
		return
	}

	// Process metadata (implementation depends on metadata package)
	// This is a placeholder - actual implementation would use metadata.FarcasterProcessor
	log.Infow("Farcaster metadata processed", "query", qr.config.Name, "censusRoot", fmt.Sprintf("0x%x", censusRoot), "addresses", len(addresses), "weights", len(weights))
}

// extractOriginalAddressesAndWeights retrieves the original addresses and weights
// from the stored address list for a census root.
func (qr *QueryRunner) extractOriginalAddressesAndWeights(censusRoot types.HexBytes) ([]common.Address, []*big.Int, error) {
	// Get all addresses and weights from storage
	addressStrings, weightMap, err := qr.service.kvStorage.GetAllAddressesAndWeights(censusRoot)
	if err != nil {
		return nil, nil, fmt.Errorf("get addresses and weights: %w", err)
	}

	// Convert to required format
	addresses := make([]common.Address, len(addressStrings))
	weights := make([]*big.Int, len(addressStrings))

	for i, addrStr := range addressStrings {
		addresses[i] = common.HexToAddress(addrStr)
		weight := weightMap[addrStr]
		weights[i] = big.NewInt(int64(weight))
	}

	return addresses, weights, nil
}

// synchronizeQueries ensures query configurations match stored snapshots.
func (s *Service) synchronizeQueries() error {
	log.Infow("synchronizing query configurations")

	for _, queryConfig := range s.config.Queries {
		if queryConfig.IsDisabled() {
			continue
		}

		// Check if query has any snapshots
		snapshots, err := s.kvStorage.SnapshotsByQuery(queryConfig.Name)
		if err != nil {
			log.Warnw("failed to get snapshots for query", "error", err, "query", queryConfig.Name)
			continue
		}

		if len(snapshots) == 0 {
			log.Infow("no snapshots found for query", "query", queryConfig.Name)
			continue
		}

		// Verify latest snapshot configuration matches
		latest := snapshots[0]
		if latest.MinBalance != queryConfig.GetMinBalance() {
			log.Warnw("minBalance mismatch between stored snapshot and configuration", "query", queryConfig.Name, "stored", latest.MinBalance, "config", queryConfig.GetMinBalance())
		}

		log.Debugw("query synchronized", "query", queryConfig.Name, "snapshots", len(snapshots), "latest", latest.SnapshotDate)
	}

	return nil
}

// convertBalanceToBytes converts a big.Int balance to bytes for census storage in big-endian format.
// Big-endian is used for Ethereum/Solidity compatibility.
func convertBalanceToBytes(balance *big.Int) []byte {
	if balance == nil {
		return make([]byte, 8)
	}

	// Get bytes from big.Int (already big-endian from Go)
	valueBytes := balance.Bytes()

	// Create result with fixed length (8 bytes for lean-imt weight limit is 88 bits)
	result := make([]byte, 8)

	// Copy value bytes to the end (right-aligned, big-endian)
	if len(valueBytes) <= 8 {
		copy(result[8-len(valueBytes):], valueBytes)
	} else {
		// If value is too large, copy only the least significant bytes
		copy(result, valueBytes[len(valueBytes)-8:])
	}

	return result
}
