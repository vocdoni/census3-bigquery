package service

import (
	"census3-bigquery/alchemy"
	"census3-bigquery/bigquery"
	"census3-bigquery/censusdb"
	"census3-bigquery/log"
	"census3-bigquery/storage"
	"fmt"
	"time"

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
						return totalProcessed, fmt.Errorf("insert final batch: %w", err)
					}
					totalProcessed += len(batch)
				}

				// Store collected address-weight entries
				if storeAddresses && len(collectedEntries) > 0 {
					if err := qr.storeAddressEntries(censusRef, collectedEntries); err != nil {
						log.Warn().Err(err).Str("query", queryID).Msg("Failed to store address entries")
					}
				}

				elapsed := time.Since(startTime)
				rate := float64(totalProcessed) / elapsed.Seconds()
				log.Info().
					Int("total", totalProcessed).
					Dur("elapsed", elapsed).
					Float64("rate", rate).
					Str("query", queryID).
					Msg("Census creation completed")

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
				log.Warn().
					Str("address", participant.Address.Hex()).
					Str("query", queryID).
					Msg("Address key too long, skipping")
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
					log.Info().
						Int("processed", totalProcessed).
						Dur("elapsed", elapsed).
						Float64("rate", rate).
						Str("query", queryID).
						Msg("Progress")
					lastLogTime = time.Now()
				}

				// Reset batch
				batch = [][]byte{}
				values = [][]byte{}
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

	alchemyConfig := alchemy.Config{
		APIKey:          qr.service.config.AlchemyAPIKey,
		Network:         qr.config.GetNetwork(),
		ContractAddress: contractAddress,
		MinBalance:      qr.config.GetMinBalance(),
		QueryName:       qr.config.Query,
		QueryParams:     qr.config.Parameters,
		PageSize:        100,
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
						return totalProcessed, fmt.Errorf("insert final batch: %w", err)
					}
					totalProcessed += len(batch)
				}

				// Store collected address-weight entries
				if storeAddresses && len(collectedEntries) > 0 {
					if err := qr.storeAddressEntries(censusRef, collectedEntries); err != nil {
						log.Warn().Err(err).Str("query", queryID).Msg("Failed to store address entries")
					}
				}

				elapsed := time.Since(startTime)
				rate := float64(totalProcessed) / elapsed.Seconds()
				log.Info().
					Int("total", totalProcessed).
					Dur("elapsed", elapsed).
					Float64("rate", rate).
					Str("query", queryID).
					Msg("Census creation completed")

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
				log.Warn().
					Str("address", participant.Address.Hex()).
					Str("query", queryID).
					Msg("Address key too long, skipping")
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
					log.Info().
						Int("processed", totalProcessed).
						Dur("elapsed", elapsed).
						Float64("rate", rate).
						Str("query", queryID).
						Msg("Progress")
					lastLogTime = time.Now()
				}

				// Reset batch
				batch = [][]byte{}
				values = [][]byte{}
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
			log.Warn().
				Err(err).
				Str("address", entry.Address).
				Float64("weight", entry.Weight).
				Msg("Failed to add address entry")
		}
	}

	if err := collector.Finalize(); err != nil {
		return fmt.Errorf("finalize address collection: %w", err)
	}

	log.Info().
		Str("census_root", finalRoot.String()).
		Int("entries", len(entries)).
		Msg("Stored address entries")

	return nil
}
