package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sort"
	"time"

	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/types"

	"census3-bigquery/log"
)

const (
	// Key prefixes for different data types
	snapshotPrefix      = "snap_"
	indexPrefix         = "idx_"
	metadataPrefix      = "meta_"
	balanceIndexPrefix  = "bal_"
	queryIndexPrefix    = "qry_"
	metaFarcasterPrefix = "meta_farcaster_"
	addressListPrefix   = "addr_list_"

	// DefaultAddressPageSize is the default number of addresses per page
	DefaultAddressPageSize = 1024
)

// WeightConfig represents weight calculation configuration for storage
type WeightConfig struct {
	Strategy        string   `json:"strategy"` // "constant", "proportional_auto", "proportional_manual"
	ConstantWeight  *int     `json:"constantWeight,omitempty"`
	TargetMinWeight *int     `json:"targetMinWeight,omitempty"`
	Multiplier      *float64 `json:"multiplier,omitempty"`
	MaxWeight       *int     `json:"maxWeight,omitempty"`
}

// KVSnapshot represents a census snapshot stored in KV database
type KVSnapshot struct {
	SnapshotDate     time.Time              `json:"snapshotDate"`
	CensusRoot       types.HexBytes         `json:"censusRoot"`
	ParticipantCount int                    `json:"participantCount"`
	CreatedAt        time.Time              `json:"createdAt"`
	MinBalance       float64                `json:"minBalance"`
	QueryName        string                 `json:"queryName"`     // User-defined name for this query instance
	QueryType        string                 `json:"queryType"`     // BigQuery query name from registry
	Decimals         int                    `json:"decimals"`      // Token decimals used for this query
	Period           string                 `json:"period"`        // Query execution period (e.g., "1h", "30m")
	Parameters       map[string]interface{} `json:"parameters"`    // All query parameters
	WeightConfig     *WeightConfig          `json:"weightConfig"`  // Weight calculation configuration
	DisplayName      string                 `json:"displayName"`   // Human-readable display name
	DisplayAvatar    string                 `json:"displayAvatar"` // Avatar URL for visual representation
}

// AddressEntry represents an address with its weight
type AddressEntry struct {
	Address string  `json:"address"` // Ethereum address
	Weight  float64 `json:"weight"`  // Voting weight for this address
}

// AddressPage represents a page of addresses with weights for a census
type AddressPage struct {
	Entries []AddressEntry `json:"entries"` // List of address-weight pairs
}

// KVSnapshotStorage manages persistent storage of snapshots using KV database
type KVSnapshotStorage struct {
	db db.Database
}

// NewKVSnapshotStorage creates a new KV-based snapshot storage instance
func NewKVSnapshotStorage(database db.Database) *KVSnapshotStorage {
	return &KVSnapshotStorage{
		db: database,
	}
}

// generateSnapshotKey creates a sortable key for snapshots
// Format: timestamp_minBalance_queryName
func (s *KVSnapshotStorage) generateSnapshotKey(snapshotDate time.Time, minBalance float64, queryName string) []byte {
	// Use Unix timestamp for sorting (most recent first when iterating in reverse)
	timestamp := snapshotDate.Unix()

	// Create a composite key that allows efficient querying
	key := fmt.Sprintf("%s%016x_%016x_%s",
		snapshotPrefix,
		^uint64(timestamp),         // Bitwise NOT for reverse chronological order
		uint64(minBalance*1000000), // Convert to micro-units for precision
		queryName)

	return []byte(key)
}

// generateIndexKey creates index keys for efficient querying
func (s *KVSnapshotStorage) generateIndexKey(prefix string, value interface{}) []byte {
	switch v := value.(type) {
	case float64:
		return []byte(fmt.Sprintf("%s%016x", prefix, uint64(v*1000000)))
	case string:
		return []byte(fmt.Sprintf("%s%s", prefix, v))
	case time.Time:
		return []byte(fmt.Sprintf("%s%016x", prefix, ^uint64(v.Unix())))
	default:
		return []byte(fmt.Sprintf("%s%v", prefix, v))
	}
}

// AddSnapshot adds a new snapshot to storage with efficient indexing
func (s *KVSnapshotStorage) AddSnapshot(snapshotDate time.Time, censusRoot types.HexBytes, participantCount int, minBalance float64, queryName string, queryType string, decimals int, period string, parameters map[string]interface{}, weightConfig *WeightConfig, displayName string, displayAvatar string) error {
	snapshot := KVSnapshot{
		SnapshotDate:     snapshotDate,
		CensusRoot:       censusRoot,
		ParticipantCount: participantCount,
		CreatedAt:        time.Now(),
		MinBalance:       minBalance,
		QueryName:        queryName,
		QueryType:        queryType,
		Decimals:         decimals,
		Period:           period,
		Parameters:       parameters,
		WeightConfig:     weightConfig,
		DisplayName:      displayName,
		DisplayAvatar:    displayAvatar,
	}

	// Serialize snapshot
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snapshot); err != nil {
		return fmt.Errorf("failed to encode snapshot: %w", err)
	}

	// Start transaction
	wtx := s.db.WriteTx()
	defer wtx.Discard()

	// Store main snapshot data
	snapshotKey := s.generateSnapshotKey(snapshotDate, minBalance, queryName)
	if err := wtx.Set(snapshotKey, buf.Bytes()); err != nil {
		return fmt.Errorf("failed to store snapshot: %w", err)
	}

	// Update metadata (total count, latest snapshot info)
	if err := s.updateMetadata(wtx, snapshot); err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	return wtx.Commit()
}

// updateMetadata updates storage metadata (for AddSnapshot - checks if newer)
func (s *KVSnapshotStorage) updateMetadata(wtx db.WriteTx, snapshot KVSnapshot) error {
	// Check if this is the latest snapshot by comparing with existing latest
	latestKey := []byte(metadataPrefix + "latest")
	existingData, err := s.db.Get(latestKey)

	shouldUpdate := true
	if err == nil {
		// There's an existing latest snapshot, compare dates
		var existingSnapshot KVSnapshot
		if err := gob.NewDecoder(bytes.NewReader(existingData)).Decode(&existingSnapshot); err == nil {
			// Only update if the new snapshot is more recent
			shouldUpdate = snapshot.SnapshotDate.After(existingSnapshot.SnapshotDate) || snapshot.SnapshotDate.Equal(existingSnapshot.SnapshotDate)
		}
	}

	if shouldUpdate {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(snapshot); err != nil {
			return err
		}
		return wtx.Set(latestKey, buf.Bytes())
	}

	return nil
}

// forceUpdateMetadata updates storage metadata without checking (for deletion operations)
func (s *KVSnapshotStorage) forceUpdateMetadata(wtx db.WriteTx, snapshot KVSnapshot) error {
	latestKey := []byte(metadataPrefix + "latest")

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snapshot); err != nil {
		return err
	}
	return wtx.Set(latestKey, buf.Bytes())
}

// Snapshots returns all snapshots ordered by most recent first
func (s *KVSnapshotStorage) Snapshots() ([]KVSnapshot, error) {
	startTime := time.Now()
	var snapshots []KVSnapshot

	err := s.db.Iterate([]byte(snapshotPrefix), func(key, value []byte) bool {
		var snapshot KVSnapshot
		if err := gob.NewDecoder(bytes.NewReader(value)).Decode(&snapshot); err != nil {
			log.Warn().Err(err).Msg("Failed to decode snapshot, skipping")
			return true // Continue iteration, skip invalid entries
		}
		snapshots = append(snapshots, snapshot)
		return true
	})

	if err != nil {
		log.Error().
			Err(err).
			Str("duration", time.Since(startTime).String()).
			Msg("Database iteration failed")
		return nil, fmt.Errorf("failed to iterate snapshots: %w", err)
	}

	// Sort by snapshot date (most recent first)
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].SnapshotDate.After(snapshots[j].SnapshotDate)
	})

	return snapshots, nil
}

// SnapshotsByBalance returns snapshots filtered by minimum balance
func (s *KVSnapshotStorage) SnapshotsByBalance(minBalance float64) ([]KVSnapshot, error) {
	var snapshots []KVSnapshot

	// Get all snapshots and filter by balance
	allSnapshots, err := s.Snapshots()
	if err != nil {
		return nil, err
	}

	for _, snapshot := range allSnapshots {
		if snapshot.MinBalance == minBalance {
			snapshots = append(snapshots, snapshot)
		}
	}

	return snapshots, nil
}

// SnapshotsByQuery returns snapshots filtered by query name
func (s *KVSnapshotStorage) SnapshotsByQuery(queryName string) ([]KVSnapshot, error) {
	var snapshots []KVSnapshot

	// Get all snapshots and filter by query name
	allSnapshots, err := s.Snapshots()
	if err != nil {
		return nil, err
	}

	for _, snapshot := range allSnapshots {
		if snapshot.QueryName == queryName {
			snapshots = append(snapshots, snapshot)
		}
	}

	return snapshots, nil
}

// LatestSnapshot returns the most recent snapshot, or nil if none exist
func (s *KVSnapshotStorage) LatestSnapshot() (*KVSnapshot, error) {
	latestKey := []byte(metadataPrefix + "latest")
	data, err := s.db.Get(latestKey)
	if err != nil {
		if err == db.ErrKeyNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get latest snapshot: %w", err)
	}

	var snapshot KVSnapshot
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("failed to decode latest snapshot: %w", err)
	}

	return &snapshot, nil
}

// GetLatestSnapshotByQuery returns the most recent snapshot for a specific query name
func (s *KVSnapshotStorage) GetLatestSnapshotByQuery(queryName string) (*KVSnapshot, error) {
	var latestSnapshot *KVSnapshot

	err := s.db.Iterate([]byte(snapshotPrefix), func(key, value []byte) bool {
		var snapshot KVSnapshot
		if err := gob.NewDecoder(bytes.NewReader(value)).Decode(&snapshot); err != nil {
			return true // Continue iteration, skip invalid entries
		}

		// Filter by query name
		if snapshot.QueryName == queryName {
			// Keep track of the latest snapshot for this query
			if latestSnapshot == nil || snapshot.SnapshotDate.After(latestSnapshot.SnapshotDate) {
				latestSnapshot = &snapshot
			}
		}
		return true
	})

	if err != nil {
		return nil, fmt.Errorf("failed to iterate snapshots: %w", err)
	}

	return latestSnapshot, nil
}

// SnapshotCount returns the total number of snapshots
func (s *KVSnapshotStorage) SnapshotCount() (int, error) {
	count := 0
	err := s.db.Iterate([]byte(snapshotPrefix), func(key, value []byte) bool {
		count++
		return true
	})
	return count, err
}

// DeleteOldSnapshots removes snapshots older than the specified duration
func (s *KVSnapshotStorage) DeleteOldSnapshots(maxAge time.Duration) (int, error) {
	cutoffTime := time.Now().Add(-maxAge)
	var keysToDelete [][]byte
	var latestSnapshot *KVSnapshot

	err := s.db.Iterate([]byte(snapshotPrefix), func(key, value []byte) bool {
		var snapshot KVSnapshot
		if err := gob.NewDecoder(bytes.NewReader(value)).Decode(&snapshot); err != nil {
			return true // Continue iteration
		}

		// Delete snapshots that are older than the cutoff time
		if snapshot.SnapshotDate.Before(cutoffTime) {
			// The key from iteration doesn't include the prefix, so we need to add it back
			fullKey := append([]byte(snapshotPrefix), key...)
			keysToDelete = append(keysToDelete, fullKey)
		} else {
			// Keep track of the latest remaining snapshot
			if latestSnapshot == nil || snapshot.SnapshotDate.After(latestSnapshot.SnapshotDate) {
				latestSnapshot = &snapshot
			}
		}
		return true
	})

	if err != nil {
		return 0, fmt.Errorf("failed to iterate for deletion: %w", err)
	}

	if len(keysToDelete) == 0 {
		return 0, nil
	}

	// Delete in transaction
	wtx := s.db.WriteTx()
	defer func() {
		if wtx != nil {
			wtx.Discard()
		}
	}()

	for _, key := range keysToDelete {
		if err := wtx.Delete(key); err != nil {
			return 0, fmt.Errorf("failed to delete snapshot: %w", err)
		}
	}

	// Update metadata with the latest remaining snapshot
	if latestSnapshot != nil {
		if err := s.forceUpdateMetadata(wtx, *latestSnapshot); err != nil {
			return 0, fmt.Errorf("failed to update metadata after deletion: %w", err)
		}
	} else {
		// No snapshots remaining, delete the latest metadata
		latestKey := []byte(metadataPrefix + "latest")
		if err := wtx.Delete(latestKey); err != nil && err != db.ErrKeyNotFound {
			return 0, fmt.Errorf("failed to delete latest metadata: %w", err)
		}
	}

	if err := wtx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit deletion: %w", err)
	}
	wtx = nil // Prevent discard from being called

	return len(keysToDelete), nil
}

// DeleteOldSnapshotsByQuery removes snapshots for a specific query, keeping only the most recent N snapshots
func (s *KVSnapshotStorage) DeleteOldSnapshotsByQuery(queryName string, keepCount int) (int, []types.HexBytes, error) {
	if keepCount < 0 {
		return 0, nil, fmt.Errorf("keepCount must be non-negative")
	}

	// Get all snapshots for this query
	snapshots, err := s.SnapshotsByQuery(queryName)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get snapshots for query %s: %w", queryName, err)
	}

	// If we have fewer snapshots than keepCount, nothing to delete
	if len(snapshots) <= keepCount {
		return 0, nil, nil
	}

	// Snapshots are already sorted by date (newest first) from SnapshotsByQuery
	// We want to delete everything after index keepCount
	snapshotsToDelete := snapshots[keepCount:]
	var keysToDelete [][]byte
	var censusRootsToDelete []types.HexBytes
	var latestSnapshot *KVSnapshot

	// If keepCount > 0, the latest snapshot to keep is at index keepCount-1
	if keepCount > 0 {
		latestSnapshot = &snapshots[keepCount-1]
	}

	// Collect keys and census roots to delete
	for _, snapshot := range snapshotsToDelete {
		key := s.generateSnapshotKey(snapshot.SnapshotDate, snapshot.MinBalance, snapshot.QueryName)
		keysToDelete = append(keysToDelete, key)
		censusRootsToDelete = append(censusRootsToDelete, snapshot.CensusRoot)
	}

	if len(keysToDelete) == 0 {
		return 0, nil, nil
	}

	// Delete in transaction
	wtx := s.db.WriteTx()
	defer func() {
		if wtx != nil {
			wtx.Discard()
		}
	}()

	for _, key := range keysToDelete {
		if err := wtx.Delete(key); err != nil {
			return 0, nil, fmt.Errorf("failed to delete snapshot: %w", err)
		}
	}

	// Update metadata with the latest remaining snapshot for this query
	if latestSnapshot != nil {
		// Check if this query's latest snapshot is the overall latest
		overallLatest, _ := s.LatestSnapshot()
		if overallLatest != nil && overallLatest.QueryName == queryName {
			// We're deleting snapshots from the query that has the overall latest snapshot
			// Need to update the overall latest metadata
			if err := s.forceUpdateMetadata(wtx, *latestSnapshot); err != nil {
				return 0, nil, fmt.Errorf("failed to update metadata after deletion: %w", err)
			}
		}
	} else if keepCount == 0 {
		// All snapshots for this query are being deleted
		// Check if we need to update the overall latest metadata
		overallLatest, _ := s.LatestSnapshot()
		if overallLatest != nil && overallLatest.QueryName == queryName {
			// Find the latest snapshot from other queries
			var newLatest *KVSnapshot
			err := s.db.Iterate([]byte(snapshotPrefix), func(key, value []byte) bool {
				var snapshot KVSnapshot
				if err := gob.NewDecoder(bytes.NewReader(value)).Decode(&snapshot); err != nil {
					return true // Continue iteration
				}
				if snapshot.QueryName != queryName {
					if newLatest == nil || snapshot.SnapshotDate.After(newLatest.SnapshotDate) {
						newLatest = &snapshot
					}
				}
				return true
			})
			if err == nil && newLatest != nil {
				if err := s.forceUpdateMetadata(wtx, *newLatest); err != nil {
					return 0, nil, fmt.Errorf("failed to update metadata after deletion: %w", err)
				}
			} else {
				// No other snapshots exist, delete the latest metadata
				latestKey := []byte(metadataPrefix + "latest")
				if err := wtx.Delete(latestKey); err != nil && err != db.ErrKeyNotFound {
					return 0, nil, fmt.Errorf("failed to delete latest metadata: %w", err)
				}
			}
		}
	}

	if err := wtx.Commit(); err != nil {
		return 0, nil, fmt.Errorf("failed to commit deletion: %w", err)
	}
	wtx = nil // Prevent discard from being called

	log.Info().
		Str("query", queryName).
		Int("deleted_count", len(keysToDelete)).
		Int("keep_count", keepCount).
		Msg("Deleted old snapshots for query")

	return len(keysToDelete), censusRootsToDelete, nil
}

// StoreMetadata stores generic metadata for a census root with the given metadata type
// The key format is: {metadataType}_{censusRoot}
// The value is stored as raw bytes (typically JSON)
func (s *KVSnapshotStorage) StoreMetadata(metadataType string, censusRoot types.HexBytes, data []byte) error {
	key := fmt.Sprintf("%s_%s", metadataType, censusRoot.String())

	wtx := s.db.WriteTx()
	defer wtx.Discard()

	if err := wtx.Set([]byte(key), data); err != nil {
		return fmt.Errorf("failed to store metadata: %w", err)
	}

	return wtx.Commit()
}

// GetMetadata retrieves generic metadata for a census root with the given metadata type
// Returns nil if metadata doesn't exist
func (s *KVSnapshotStorage) GetMetadata(metadataType string, censusRoot types.HexBytes) ([]byte, error) {
	key := fmt.Sprintf("%s_%s", metadataType, censusRoot.String())

	data, err := s.db.Get([]byte(key))
	if err != nil {
		if err == db.ErrKeyNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	return data, nil
}

// HasMetadata checks if metadata exists for a census root with the given metadata type
func (s *KVSnapshotStorage) HasMetadata(metadataType string, censusRoot types.HexBytes) (bool, error) {
	data, err := s.GetMetadata(metadataType, censusRoot)
	if err != nil {
		return false, err
	}
	return data != nil, nil
}

// DeleteMetadata removes metadata for a census root with the given metadata type
func (s *KVSnapshotStorage) DeleteMetadata(metadataType string, censusRoot types.HexBytes) error {
	key := fmt.Sprintf("%s_%s", metadataType, censusRoot.String())

	wtx := s.db.WriteTx()
	defer wtx.Discard()

	if err := wtx.Delete([]byte(key)); err != nil && err != db.ErrKeyNotFound {
		return fmt.Errorf("failed to delete metadata: %w", err)
	}

	return wtx.Commit()
}

// StoreAddressPage stores a page of address entries for a census root
func (s *KVSnapshotStorage) StoreAddressPage(censusRoot types.HexBytes, pageNumber int, entries []AddressEntry) error {
	if len(entries) == 0 {
		return nil // Nothing to store
	}

	key := fmt.Sprintf("%s%s_%d", addressListPrefix, censusRoot.String(), pageNumber)

	page := AddressPage{
		Entries: entries,
	}

	// Serialize page to GOB for space efficiency
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(page); err != nil {
		return fmt.Errorf("failed to encode address page: %w", err)
	}

	wtx := s.db.WriteTx()
	defer wtx.Discard()

	if err := wtx.Set([]byte(key), buf.Bytes()); err != nil {
		return fmt.Errorf("failed to store address page: %w", err)
	}

	return wtx.Commit()
}

// GetAddressPage retrieves a specific page of addresses for a census root
func (s *KVSnapshotStorage) GetAddressPage(censusRoot types.HexBytes, pageNumber int) (*AddressPage, error) {
	key := fmt.Sprintf("%s%s_%d", addressListPrefix, censusRoot.String(), pageNumber)

	data, err := s.db.Get([]byte(key))
	if err != nil {
		if err == db.ErrKeyNotFound {
			return nil, nil // Page doesn't exist
		}
		return nil, fmt.Errorf("failed to get address page: %w", err)
	}

	var page AddressPage
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&page); err != nil {
		return nil, fmt.Errorf("failed to decode address page: %w", err)
	}

	return &page, nil
}

// GetAllAddresses retrieves all addresses for a census root by iterating through all pages
func (s *KVSnapshotStorage) GetAllAddresses(censusRoot types.HexBytes) ([]string, error) {
	addresses, _, err := s.GetAllAddressesAndWeights(censusRoot)
	return addresses, err
}

// GetAllAddressesAndWeights retrieves all addresses and weights for a census root by iterating through all pages
func (s *KVSnapshotStorage) GetAllAddressesAndWeights(censusRoot types.HexBytes) ([]string, map[string]float64, error) {
	var allAddresses []string
	weights := make(map[string]float64)
	pageNumber := 0

	for {
		page, err := s.GetAddressPage(censusRoot, pageNumber)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get address page %d: %w", pageNumber, err)
		}

		if page == nil {
			// No more pages
			break
		}

		for _, entry := range page.Entries {
			allAddresses = append(allAddresses, entry.Address)
			weights[entry.Address] = entry.Weight
		}
		pageNumber++
	}

	log.Debug().
		Str("census_root", censusRoot.String()).
		Int("total_addresses", len(allAddresses)).
		Int("pages_loaded", pageNumber).
		Msg("Loaded all addresses and weights from storage")

	return allAddresses, weights, nil
}

// GetAddressListPageCount returns the number of pages for a census root's address list
func (s *KVSnapshotStorage) GetAddressListPageCount(censusRoot types.HexBytes) (int, error) {
	pageCount := 0

	for {
		page, err := s.GetAddressPage(censusRoot, pageCount)
		if err != nil {
			return 0, fmt.Errorf("failed to check address page %d: %w", pageCount, err)
		}

		if page == nil {
			break
		}

		pageCount++
	}

	return pageCount, nil
}

// HasAddressList checks if an address list exists for a census root
func (s *KVSnapshotStorage) HasAddressList(censusRoot types.HexBytes) (bool, error) {
	page, err := s.GetAddressPage(censusRoot, 0)
	if err != nil {
		return false, err
	}
	return page != nil, nil
}

// DeleteAddressList removes all address pages for a census root
func (s *KVSnapshotStorage) DeleteAddressList(censusRoot types.HexBytes) error {
	// Get the number of pages first
	pageCount, err := s.GetAddressListPageCount(censusRoot)
	if err != nil {
		return fmt.Errorf("failed to get page count: %w", err)
	}

	if pageCount == 0 {
		return nil // Nothing to delete
	}

	wtx := s.db.WriteTx()
	defer wtx.Discard()

	// Delete all pages
	for i := 0; i < pageCount; i++ {
		key := fmt.Sprintf("%s%s_%d", addressListPrefix, censusRoot.String(), i)
		if err := wtx.Delete([]byte(key)); err != nil && err != db.ErrKeyNotFound {
			return fmt.Errorf("failed to delete address page %d: %w", i, err)
		}
	}

	if err := wtx.Commit(); err != nil {
		return fmt.Errorf("failed to commit address list deletion: %w", err)
	}

	log.Debug().
		Str("census_root", censusRoot.String()).
		Int("pages_deleted", pageCount).
		Msg("Deleted address list")

	return nil
}

// Close closes the storage (if needed)
func (s *KVSnapshotStorage) Close() error {
	// The underlying database is managed externally
	return nil
}
