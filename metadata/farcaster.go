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

// ProcessCensus processes a census to extract Farcaster metadata using stored addresses
// This method uses stored addresses, queries Neynar API, and stores the results
func (fp *FarcasterProcessor) ProcessCensus(ctx context.Context, censusRoot types.HexBytes, addresses []string, weights map[string]float64, farcasterConfig *config.FarcasterConfig) error {
	log.Info().
		Str("census_root", censusRoot.String()).
		Int("total_addresses", len(addresses)).
		Msg("Starting Farcaster metadata processing")

	startTime := time.Now()

	if len(addresses) == 0 {
		log.Info().
			Str("census_root", censusRoot.String()).
			Msg("No addresses provided, skipping Farcaster metadata processing")
		return nil
	}

	// Filter out invalid addresses (zero addresses, non-hex, etc.)
	validAddresses := fp.filterValidAddresses(addresses)

	if len(validAddresses) == 0 {
		log.Info().
			Str("census_root", censusRoot.String()).
			Int("original_count", len(addresses)).
			Msg("No valid addresses found after filtering, skipping Farcaster metadata processing")
		return nil
	}

	log.Info().
		Str("census_root", censusRoot.String()).
		Int("original_addresses", len(addresses)).
		Int("valid_addresses", len(validAddresses)).
		Msg("Filtered addresses for Neynar API")

	// Create Neynar client with the provided API key
	neynarClient := neynar.NewClient(farcasterConfig.NeynarAPIKey)

	// Query Neynar API for Farcaster users
	neynarUsers, err := neynarClient.GetUsersByAddresses(ctx, validAddresses)
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
		Int("valid_addresses", len(validAddresses)).
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
// Aggregates weights for users who control multiple addresses
func (fp *FarcasterProcessor) processFarcasterUsers(neynarUsers map[string][]neynar.User, weights map[string]float64) []FarcasterUser {
	// Group users by FID to aggregate weights for multiple addresses
	userAggregation := make(map[int64]*FarcasterUserAggregation)

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

		// Process each user for this address
		for _, user := range users {
			if existing, exists := userAggregation[user.FID]; exists {
				// User already exists, aggregate the weight
				existing.TotalWeight += weight
				existing.Addresses = append(existing.Addresses, address)
			} else {
				// New user, create aggregation entry
				userAggregation[user.FID] = &FarcasterUserAggregation{
					Username:    user.Username,
					FID:         user.FID,
					TotalWeight: weight,
					Addresses:   []string{address},
				}
			}
		}
	}

	// Convert aggregated data to final FarcasterUser objects
	var farcasterUsers []FarcasterUser
	for _, aggregation := range userAggregation {
		// Use the first address as the primary address for display
		primaryAddress := aggregation.Addresses[0]

		farcasterUser := FarcasterUser{
			Username: aggregation.Username,
			Weight:   aggregation.TotalWeight,
			FID:      aggregation.FID,
			Address:  primaryAddress,
		}
		farcasterUsers = append(farcasterUsers, farcasterUser)
	}

	log.Debug().
		Int("total_users", len(farcasterUsers)).
		Int("unique_addresses", len(neynarUsers)).
		Int("aggregated_users", len(userAggregation)).
		Msg("Processed and aggregated Farcaster users from Neynar API results")

	return farcasterUsers
}

// FarcasterUserAggregation is used internally to aggregate weights for users with multiple addresses
type FarcasterUserAggregation struct {
	Username    string
	FID         int64
	TotalWeight float64
	Addresses   []string
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

// filterValidAddresses filters out invalid addresses (zero addresses, invalid hex, etc.)
func (fp *FarcasterProcessor) filterValidAddresses(addresses []string) []string {
	var validAddresses []string

	for _, address := range addresses {
		// Skip empty addresses
		if address == "" {
			continue
		}

		// Skip zero addresses
		if address == "0x0000000000000000000000000000000000000000000000000000000000000000" ||
			address == "0x0000000000000000000000000000000000000000" {
			log.Debug().
				Str("address", address).
				Msg("Skipping zero address")
			continue
		}

		// Ensure address has proper 0x prefix
		if !strings.HasPrefix(address, "0x") {
			address = "0x" + address
		}

		// Skip addresses that are too short (less than 40 hex chars + 0x prefix)
		if len(address) < 42 {
			log.Debug().
				Str("address", address).
				Msg("Skipping address that is too short")
			continue
		}

		// Skip addresses that are too long (more than 42 chars)
		if len(address) > 42 {
			log.Debug().
				Str("address", address).
				Msg("Skipping address that is too long")
			continue
		}

		// Validate hex format (basic check)
		hexPart := address[2:] // Remove 0x prefix
		if len(hexPart) != 40 {
			log.Debug().
				Str("address", address).
				Msg("Skipping address with invalid length")
			continue
		}

		// Check if all characters are valid hex
		validHex := true
		for _, char := range hexPart {
			if (char < '0' || char > '9') && (char < 'a' || char > 'f') && (char < 'A' || char > 'F') {
				validHex = false
				break
			}
		}

		if !validHex {
			log.Debug().
				Str("address", address).
				Msg("Skipping address with invalid hex characters")
			continue
		}

		validAddresses = append(validAddresses, address)
	}

	log.Debug().
		Int("original_count", len(addresses)).
		Int("valid_count", len(validAddresses)).
		Msg("Address filtering completed")

	return validAddresses
}

// DeleteMetadata removes Farcaster metadata for a census root
func (fp *FarcasterProcessor) DeleteMetadata(censusRoot types.HexBytes) error {
	return fp.storage.DeleteMetadata(FarcasterMetadataType, censusRoot)
}
