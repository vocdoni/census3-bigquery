package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/vocdoni/arbo"
	"github.com/vocdoni/davinci-node/types"

	"census3-bigquery/censusdb"
	"census3-bigquery/config"
	"census3-bigquery/log"
	"census3-bigquery/neynar"
	"census3-bigquery/storage"
)

const (
	// FarcasterMetadataType is the metadata type identifier for Farcaster data
	FarcasterMetadataType = "meta_farcaster"
)

// FarcasterUser represents a Farcaster user with their voting weight
type FarcasterUser struct {
	Username string  `json:"username"` // Farcaster username
	Weight   float64 `json:"weight"`   // Voting weight from census
	FID      int64   `json:"fid"`      // Farcaster ID
	Address  string  `json:"address"`  // Ethereum address
}

// FarcasterMetadata represents the complete Farcaster metadata for a census
type FarcasterMetadata struct {
	CensusRoot types.HexBytes  `json:"censusRoot"` // Census root this metadata belongs to
	Users      []FarcasterUser `json:"users"`      // List of Farcaster users with weights
	CreatedAt  time.Time       `json:"createdAt"`  // When this metadata was created
	TotalUsers int             `json:"totalUsers"` // Total number of users found
}

// FarcasterProcessor handles Farcaster metadata processing
type FarcasterProcessor struct {
	neynarClient *neynar.Client
	storage      *storage.KVSnapshotStorage
}

// NewFarcasterProcessor creates a new Farcaster metadata processor
func NewFarcasterProcessor(neynarClient *neynar.Client, storage *storage.KVSnapshotStorage) *FarcasterProcessor {
	return &FarcasterProcessor{
		neynarClient: neynarClient,
		storage:      storage,
	}
}

// ProcessCensus processes a census to extract Farcaster metadata
// This method extracts all addresses from the census, queries Neynar API, and stores the results
func (fp *FarcasterProcessor) ProcessCensus(ctx context.Context, censusRef *censusdb.CensusRef, censusRoot types.HexBytes, farcasterConfig *config.FarcasterConfig) error {
	log.Info().
		Str("census_root", censusRoot.String()).
		Msg("Starting Farcaster metadata processing")

	startTime := time.Now()

	// Extract all addresses and weights from the census
	addresses, weights, err := fp.extractAddressesAndWeights(censusRef)
	if err != nil {
		return fmt.Errorf("failed to extract addresses from census: %w", err)
	}

	if len(addresses) == 0 {
		log.Info().
			Str("census_root", censusRoot.String()).
			Msg("No addresses found in census, skipping Farcaster metadata processing")
		return nil
	}

	log.Info().
		Str("census_root", censusRoot.String()).
		Int("total_addresses", len(addresses)).
		Msg("Extracted addresses from census")

	// Create Neynar client with the provided API key
	neynarClient := neynar.NewClient(farcasterConfig.NeynarAPIKey)

	// Query Neynar API for Farcaster users
	neynarUsers, err := neynarClient.GetUsersByAddresses(ctx, addresses)
	if err != nil {
		return fmt.Errorf("failed to query Neynar API: %w", err)
	}

	// Process the results and create FarcasterUser objects
	farcasterUsers := fp.processFarcasterUsers(neynarUsers, weights)

	// Create metadata object
	metadata := FarcasterMetadata{
		CensusRoot: censusRoot,
		Users:      farcasterUsers,
		CreatedAt:  time.Now(),
		TotalUsers: len(farcasterUsers),
	}

	// Serialize metadata to JSON
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal Farcaster metadata: %w", err)
	}

	// Store metadata in storage
	if err := fp.storage.StoreMetadata(FarcasterMetadataType, censusRoot, metadataJSON); err != nil {
		return fmt.Errorf("failed to store Farcaster metadata: %w", err)
	}

	elapsed := time.Since(startTime)
	log.Info().
		Str("census_root", censusRoot.String()).
		Int("total_addresses", len(addresses)).
		Int("farcaster_users_found", len(farcasterUsers)).
		Str("duration", elapsed.String()).
		Msg("Farcaster metadata processing completed successfully")

	return nil
}

// extractAddressesAndWeights extracts all addresses and their weights from a census
func (fp *FarcasterProcessor) extractAddressesAndWeights(censusRef *censusdb.CensusRef) ([]string, map[string]float64, error) {
	// Get the tree from the census reference
	tree := censusRef.Tree()
	if tree == nil {
		return nil, nil, fmt.Errorf("census tree is nil")
	}

	var addresses []string
	weights := make(map[string]float64)

	// Iterate through all keys in the tree
	err := tree.Iterate(nil, func(key []byte, value []byte) {
		// Convert key to address string (assuming it's already an address or hash)
		address := fmt.Sprintf("0x%x", key)
		addresses = append(addresses, address)

		// Convert value (weight) from bytes to float64
		if len(value) > 0 {
			// The value is stored as big.Int bytes, convert it back
			weightBigInt := arbo.BytesToBigInt(value)
			if weightBigInt != nil {
				// Convert to float64 for easier handling
				weightFloat, _ := new(big.Float).SetInt(weightBigInt).Float64()
				weights[address] = weightFloat
			} else {
				weights[address] = 1.0 // Default weight if conversion fails
			}
		} else {
			weights[address] = 1.0 // Default weight if no value
		}
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to iterate census tree: %w", err)
	}

	log.Debug().
		Int("addresses_extracted", len(addresses)).
		Msg("Successfully extracted addresses and weights from census")

	return addresses, weights, nil
}

// processFarcasterUsers processes Neynar API results and creates FarcasterUser objects
func (fp *FarcasterProcessor) processFarcasterUsers(neynarUsers map[string][]neynar.User, weights map[string]float64) []FarcasterUser {
	var farcasterUsers []FarcasterUser

	for address, users := range neynarUsers {
		// Normalize address for lookup (ensure consistent format)
		normalizedAddress := strings.ToLower(address)
		if !strings.HasPrefix(normalizedAddress, "0x") {
			normalizedAddress = "0x" + normalizedAddress
		}

		// Get weight for this address
		weight, exists := weights[normalizedAddress]
		if !exists {
			// Try original address format
			weight, exists = weights[address]
			if !exists {
				weight = 1.0 // Default weight
			}
		}

		// Process each user for this address (there can be multiple users per address)
		for _, user := range users {
			farcasterUser := FarcasterUser{
				Username: user.Username,
				Weight:   weight,
				FID:      user.FID,
				Address:  address,
			}
			farcasterUsers = append(farcasterUsers, farcasterUser)
		}
	}

	log.Debug().
		Int("total_users", len(farcasterUsers)).
		Int("unique_addresses", len(neynarUsers)).
		Msg("Processed Farcaster users from Neynar API results")

	return farcasterUsers
}

// GetMetadata retrieves Farcaster metadata for a census root
func (fp *FarcasterProcessor) GetMetadata(censusRoot types.HexBytes) (*FarcasterMetadata, error) {
	data, err := fp.storage.GetMetadata(FarcasterMetadataType, censusRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to get Farcaster metadata: %w", err)
	}

	if data == nil {
		return nil, nil // No metadata found
	}

	var metadata FarcasterMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Farcaster metadata: %w", err)
	}

	return &metadata, nil
}

// HasMetadata checks if Farcaster metadata exists for a census root
func (fp *FarcasterProcessor) HasMetadata(censusRoot types.HexBytes) (bool, error) {
	return fp.storage.HasMetadata(FarcasterMetadataType, censusRoot)
}

// DeleteMetadata removes Farcaster metadata for a census root
func (fp *FarcasterProcessor) DeleteMetadata(censusRoot types.HexBytes) error {
	return fp.storage.DeleteMetadata(FarcasterMetadataType, censusRoot)
}
