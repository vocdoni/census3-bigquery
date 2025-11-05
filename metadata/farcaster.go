package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/vocdoni/census3-bigquery/config"
	"github.com/vocdoni/census3-bigquery/neynar"
	"github.com/vocdoni/census3-bigquery/storage"
	"github.com/vocdoni/davinci-node/log"
	"github.com/vocdoni/davinci-node/types"
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
	log.Infow("starting Farcaster metadata processing", "censusRoot", censusRoot.String(), "totalAddresses", len(addresses))

	startTime := time.Now()

	if len(addresses) == 0 {
		log.Infow("no addresses provided, skipping Farcaster metadata processing", "censusRoot", censusRoot.String())
		return nil
	}

	// Filter out invalid addresses (zero addresses, non-hex, etc.)
	validAddresses := fp.filterValidAddresses(addresses)

	if len(validAddresses) == 0 {
		log.Infow("no valid addresses found after filtering, skipping Farcaster metadata processing", "censusRoot", censusRoot.String(), "originalCount", len(addresses))
		return nil
	}

	log.Infow("filtered addresses for Neynar API", "censusRoot", censusRoot.String(), "originalAddresses", len(addresses), "validAddresses", len(validAddresses))

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
	log.Infow("Farcaster metadata processing completed successfully", "censusRoot", censusRoot.String(), "totalAddresses", len(addresses), "validAddresses", len(validAddresses), "farcasterUsersFound", len(farcasterUsers), "duration", elapsed.String())

	return nil
}

// processFarcasterUsers processes Neynar API results and creates FarcasterUser objects
// Aggregates weights for users who control multiple addresses
func (fp *FarcasterProcessor) processFarcasterUsers(neynarUsers map[string][]neynar.User, weights map[string]float64) []FarcasterUser {
	// Group users by FID to aggregate weights for multiple addresses
	userAggregation := make(map[int64]*FarcasterUserAggregation)

	log.Debugw("starting Farcaster user processing and weight aggregation", "neynarAddresses", len(neynarUsers), "weightEntries", len(weights))

	for address, users := range neynarUsers {
		// Try multiple address formats to find the weight
		var weight float64
		var weightFound bool

		// Try exact address format first
		if w, exists := weights[address]; exists {
			weight = w
			weightFound = true
			log.Debugw("found weight with exact address format", "address", address, "weight", weight)
		} else {
			// Try normalized lowercase format
			normalizedAddress := strings.ToLower(address)
			if !strings.HasPrefix(normalizedAddress, "0x") {
				normalizedAddress = "0x" + normalizedAddress
			}
			if w, exists := weights[normalizedAddress]; exists {
				weight = w
				weightFound = true
				log.Debugw("found weight with normalized address format", "address", address, "normalized", normalizedAddress, "weight", weight)
			} else {
				// Try all stored addresses to find a case-insensitive match
				for storedAddr, w := range weights {
					if strings.EqualFold(address, storedAddr) {
						weight = w
						weightFound = true
						log.Debugw("found weight with case-insensitive match", "address", address, "storedAddr", storedAddr, "weight", weight)
						break
					}
				}
			}
		}

		if !weightFound {
			weight = 1.0 // Default weight
			log.Warnw("no weight found for address, using default", "address", address, "defaultWeight", weight)
		}

		// Process each user for this address
		for _, user := range users {
			log.Debugw("processing user for address", "address", address, "username", user.Username, "fid", user.FID, "weight", weight)

			if existing, exists := userAggregation[user.FID]; exists {
				// User already exists, aggregate the weight
				oldWeight := existing.TotalWeight
				existing.TotalWeight += weight
				existing.Addresses = append(existing.Addresses, address)
				log.Debugw("aggregated weight for existing user", "username", user.Username, "fid", user.FID, "oldWeight", oldWeight, "addedWeight", weight, "newTotal", existing.TotalWeight)
			} else {
				// New user, create aggregation entry
				userAggregation[user.FID] = &FarcasterUserAggregation{
					Username:    user.Username,
					FID:         user.FID,
					TotalWeight: weight,
					Addresses:   []string{address},
				}
				log.Debugw("created new user aggregation", "username", user.Username, "fid", user.FID, "initialWeight", weight)
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

		log.Debugw("created final Farcaster user entry", "username", farcasterUser.Username, "fid", farcasterUser.FID, "finalWeight", farcasterUser.Weight, "addressCount", len(aggregation.Addresses), "primaryAddress", primaryAddress)
	}

	log.Infow("processed and aggregated Farcaster users from Neynar API results", "totalUsers", len(farcasterUsers), "uniqueAddresses", len(neynarUsers), "aggregatedUsers", len(userAggregation))

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
			log.Debugw("skipping zero address", "address", address)
			continue
		}

		// Ensure address has proper 0x prefix
		if !strings.HasPrefix(address, "0x") {
			address = "0x" + address
		}

		// Skip addresses that are too short (less than 40 hex chars + 0x prefix)
		if len(address) < 42 {
			log.Debugw("skipping address that is too short", "address", address)
			continue
		}

		// Skip addresses that are too long (more than 42 chars)
		if len(address) > 42 {
			log.Debugw("skipping address that is too long", "address", address)
			continue
		}

		// Validate hex format (basic check)
		hexPart := address[2:] // Remove 0x prefix
		if len(hexPart) != 40 {
			log.Debugw("skipping address with invalid length", "address", address)
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
			log.Debugw("skipping address with invalid hex characters", "address", address)
			continue
		}

		validAddresses = append(validAddresses, address)
	}

	log.Debugw("address filtering completed", "originalCount", len(addresses), "validCount", len(validAddresses))

	return validAddresses
}

// DeleteMetadata removes Farcaster metadata for a census root
func (fp *FarcasterProcessor) DeleteMetadata(censusRoot types.HexBytes) error {
	return fp.storage.DeleteMetadata(FarcasterMetadataType, censusRoot)
}
