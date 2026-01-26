package service

import (
	"fmt"
	"time"

	"github.com/vocdoni/census3-bigquery/alchemy"
	"github.com/vocdoni/census3-bigquery/bigquery"
	"github.com/vocdoni/census3-bigquery/storage"
	"github.com/vocdoni/davinci-node/census/censusdb"
	"github.com/vocdoni/davinci-node/log"
	"github.com/vocdoni/davinci-node/types"
)

const (
	// DefaultBatchSize is the default number of participants to process in a single batch
	DefaultBatchSize = 1000
)

// streamAndCreateCensusBigQuery streams participants from BigQuery and creates census in batches.
func (qr *QueryRunner) streamAndCreateCensusBigQuery(censusRef *censusdb.CensusRef) (int, error) {
	// Determine batch size
	batchSize := qr.service.config.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	// Build BigQuery configuration
	bqConfig := bigquery.Config{
		Project:     qr.service.config.Project,
		QueryName:   qr.config.Query,
		MinBalance:  qr.config.GetMinBalance(),
		QueryParams: qr.config.Parameters,
	}

	// Collect address-weight pairs in memory for metadata processing
	var collectedEntries []storage.AddressEntry
	storeAddresses := qr.config.GetStoreAddresses()

	// Create channels for streaming
	participantCh := make(chan bigquery.Participant, batchSize)
	errorCh := make(chan error, 1)

	// Start BigQuery streaming in a goroutine
	go qr.service.bigqueryClient.StreamBalances(qr.ctx, bqConfig, participantCh, errorCh)

	// Process participants in batches
	var totalProcessed int
	var batch []types.HexBytes
	var values []types.HexBytes
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
						return totalProcessed, fmt.Errorf("insert final batch: %w", err)
					}
					totalProcessed += len(batch)
				}

				// Store collected address-weight entries
				if storeAddresses && len(collectedEntries) > 0 {
					if err := qr.storeAddressEntries(censusRef, collectedEntries); err != nil {
						log.Warnw("failed to store address entries", "error", err, "query", queryID)
					}
				}

				elapsed := time.Since(startTime)
				rate := float64(totalProcessed) / elapsed.Seconds()
				log.Infow("census creation completed", "total", totalProcessed, "elapsed", elapsed, "rate", rate, "query", queryID)

				return totalProcessed, nil
			}

			// Collect address and weight for metadata
			if storeAddresses {
				weightFloat, _ := participant.Balance.Float64()
				collectedEntries = append(collectedEntries, storage.AddressEntry{
					Address: participant.Address.Hex(),
					Weight:  weightFloat,
				})
			}

			// Prepare census entry
			addressKey := participant.Address.Bytes()
			if len(addressKey) > censusdb.CensusKeyMaxLen {
				log.Warnw("address key too long, skipping", "address", participant.Address.Hex(), "query", queryID)
				continue
			}

			// Convert balance to bytes (8 bytes for lean-imt weight limit)
			balanceBytes := convertBalanceToBytes(participant.Balance)

			// Add to batch
			batch = append(batch, addressKey)
			values = append(values, balanceBytes)

			// Process batch when full
			if len(batch) >= batchSize {
				if _, err := censusRef.InsertBatch(batch, values); err != nil {
					return totalProcessed, fmt.Errorf("insert batch: %w", err)
				}
				totalProcessed += len(batch)

				// Log progress periodically
				if time.Since(lastLogTime) >= 10*time.Second {
					elapsed := time.Since(startTime)
					rate := float64(totalProcessed) / elapsed.Seconds()
					log.Infow("progress", "processed", totalProcessed, "elapsed", elapsed, "rate", rate, "query", queryID)
					lastLogTime = time.Now()
				}

				// Reset batch
				batch = []types.HexBytes{}
				values = []types.HexBytes{}
			}

		case err := <-errorCh:
			if err != nil {
				return totalProcessed, fmt.Errorf("BigQuery streaming: %w", err)
			}

		case <-qr.ctx.Done():
			return totalProcessed, fmt.Errorf("context cancelled")
		}
	}
}

// streamAndCreateCensusAlchemy streams participants from Alchemy and creates census in batches.
func (qr *QueryRunner) streamAndCreateCensusAlchemy(censusRef *censusdb.CensusRef) (int, error) {
	// Determine batch size
	batchSize := qr.service.config.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	// Build Alchemy configuration
	contractAddress := ""
	if addr, ok := qr.config.Parameters["contract_address"]; ok {
		contractAddress = fmt.Sprintf("%v", addr)
	}

	// Get weight configuration with defaults
	weightCfg := qr.config.GetWeightConfig()

	alchemyConfig := alchemy.Config{
		APIKey:          qr.service.config.AlchemyAPIKey,
		Network:         qr.config.GetNetwork(),
		ContractAddress: contractAddress,
		MinBalance:      qr.config.GetMinBalance(),
		QueryName:       qr.config.Query,
		QueryParams:     qr.config.Parameters,
		PageSize:        100,
		WeightConfig: alchemy.WeightConfig{
			Strategy:        weightCfg.Strategy,
			ConstantWeight:  weightCfg.ConstantWeight,
			TargetMinWeight: weightCfg.TargetMinWeight,
			Multiplier:      weightCfg.Multiplier,
			MaxWeight:       weightCfg.MaxWeight,
		},
	}

	// Collect address-weight pairs in memory for metadata processing
	var collectedEntries []storage.AddressEntry
	storeAddresses := qr.config.GetStoreAddresses()

	// Create channels for streaming
	participantCh := make(chan alchemy.Participant, batchSize)
	errorCh := make(chan error, 1)

	// Start Alchemy streaming in a goroutine
	go qr.service.alchemyClient.StreamBalances(qr.ctx, alchemyConfig, participantCh, errorCh)

	// Process participants in batches
	var totalProcessed int
	var batch []types.HexBytes
	var values []types.HexBytes
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
						return totalProcessed, fmt.Errorf("insert final batch: %w", err)
					}
					totalProcessed += len(batch)
				}

				// Store collected address-weight entries
				if storeAddresses && len(collectedEntries) > 0 {
					if err := qr.storeAddressEntries(censusRef, collectedEntries); err != nil {
						log.Warnw("failed to store address entries", "error", err, "query", queryID)
					}
				}

				elapsed := time.Since(startTime)
				rate := float64(totalProcessed) / elapsed.Seconds()
				log.Infow("census creation completed", "total", totalProcessed, "elapsed", elapsed, "rate", rate, "query", queryID)

				return totalProcessed, nil
			}

			// Collect address and weight for metadata
			if storeAddresses {
				weightFloat, _ := participant.Balance.Float64()
				collectedEntries = append(collectedEntries, storage.AddressEntry{
					Address: participant.Address.Hex(),
					Weight:  weightFloat,
				})
			}

			// Prepare census entry
			addressKey := participant.Address.Bytes()
			if len(addressKey) > censusdb.CensusKeyMaxLen {
				log.Warnw("address key too long, skipping", "address", participant.Address.Hex(), "query", queryID)
				continue
			}

			// Convert balance to bytes (8 bytes for lean-imt weight limit)
			balanceBytes := convertBalanceToBytes(participant.Balance)

			// Add to batch
			batch = append(batch, addressKey)
			values = append(values, balanceBytes)

			// Process batch when full
			if len(batch) >= batchSize {
				if _, err := censusRef.InsertBatch(batch, values); err != nil {
					return totalProcessed, fmt.Errorf("insert batch: %w", err)
				}
				totalProcessed += len(batch)

				// Log progress periodically
				if time.Since(lastLogTime) >= 10*time.Second {
					elapsed := time.Since(startTime)
					rate := float64(totalProcessed) / elapsed.Seconds()
					log.Infow("progress", "processed", totalProcessed, "elapsed", elapsed, "rate", rate, "query", queryID)
					lastLogTime = time.Now()
				}

				// Reset batch
				batch = []types.HexBytes{}
				values = []types.HexBytes{}
			}

		case err := <-errorCh:
			if err != nil {
				return totalProcessed, fmt.Errorf("alchemy streaming: %w", err)
			}

		case <-qr.ctx.Done():
			return totalProcessed, fmt.Errorf("context cancelled")
		}
	}
}

// storeAddressEntries stores collected address entries using the AddressCollector.
func (qr *QueryRunner) storeAddressEntries(censusRef *censusdb.CensusRef, entries []storage.AddressEntry) error {
	finalRoot := types.HexBytes(censusRef.Root())
	collector := NewAddressCollector(
		qr.service.kvStorage,
		finalRoot,
		storage.DefaultAddressPageSize,
		true,
	)

	for _, entry := range entries {
		if err := collector.AddAddressWithWeight(entry.Address, entry.Weight); err != nil {
			log.Warnw("failed to add address entry", "error", err, "address", entry.Address, "weight", entry.Weight)
		}
	}

	if err := collector.Finalize(); err != nil {
		return fmt.Errorf("finalize address collection: %w", err)
	}

	log.Infow("stored address entries", "censusRoot", finalRoot.String(), "entries", len(entries))

	return nil
}
