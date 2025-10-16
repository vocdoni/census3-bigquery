package metadata

import (
	"census3-bigquery/config"
	"census3-bigquery/log"
	"census3-bigquery/neynar"
	"census3-bigquery/storage"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

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

// processFarcasterUsers processes Neynar API results and creates FarcasterUser objects
// Aggregates weights for users who control multiple addresses
func (fp *FarcasterProcessor) processFarcasterUsers(neynarUsers map[string][]neynar.User, weights map[string]float64) []FarcasterUser {
	// Group users by FID to aggregate weights for multiple addresses
	userAggregation := make(map[int64]*FarcasterUserAggregation)

	log.Debug().
		Int("neynar_addresses", len(neynarUsers)).
		Int("weight_entries", len(weights)).
		Msg("Starting Farcaster user processing and weight aggregation")

	for address, users := range neynarUsers {
		// Try multiple address formats to find the weight
		var weight float64
		var weightFound bool

		// Try exact address format first
		if w, exists := weights[address]; exists {
			weight = w
			weightFound = true
			log.Debug().
				Str("address", address).
				Float64("weight", weight).
				Msg("Found weight with exact address format")
		} else {
			// Try normalized lowercase format
			normalizedAddress := strings.ToLower(address)
			if !strings.HasPrefix(normalizedAddress, "0x") {
				normalizedAddress = "0x" + normalizedAddress
			}
			if w, exists := weights[normalizedAddress]; exists {
				weight = w
				weightFound = true
				log.Debug().
					Str("address", address).
					Str("normalized", normalizedAddress).
					Float64("weight", weight).
					Msg("Found weight with normalized address format")
			} else {
				// Try all stored addresses to find a case-insensitive match
				for storedAddr, w := range weights {
					if strings.EqualFold(address, storedAddr) {
						weight = w
						weightFound = true
						log.Debug().
							Str("address", address).
							Str("stored_addr", storedAddr).
							Float64("weight", weight).
							Msg("Found weight with case-insensitive match")
						break
					}
				}
			}
		}

		if !weightFound {
			weight = 1.0 // Default weight
			log.Warn().
				Str("address", address).
				Float64("default_weight", weight).
				Msg("No weight found for address, using default")
		}

		// Process each user for this address
		for _, user := range users {
			log.Debug().
				Str("address", address).
				Str("username", user.Username).
				Int64("fid", user.FID).
				Float64("weight", weight).
				Msg("Processing user for address")

			if existing, exists := userAggregation[user.FID]; exists {
				// User already exists, aggregate the weight
				oldWeight := existing.TotalWeight
				existing.TotalWeight += weight
				existing.Addresses = append(existing.Addresses, address)
				log.Debug().
					Str("username", user.Username).
					Int64("fid", user.FID).
					Float64("old_weight", oldWeight).
					Float64("added_weight", weight).
					Float64("new_total", existing.TotalWeight).
					Msg("Aggregated weight for existing user")
			} else {
				// New user, create aggregation entry
				userAggregation[user.FID] = &FarcasterUserAggregation{
					Username:    user.Username,
					FID:         user.FID,
					TotalWeight: weight,
					Addresses:   []string{address},
				}
				log.Debug().
					Str("username", user.Username).
					Int64("fid", user.FID).
					Float64("initial_weight", weight).
					Msg("Created new user aggregation")
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

		log.Debug().
			Str("username", farcasterUser.Username).
			Int64("fid", farcasterUser.FID).
			Float64("final_weight", farcasterUser.Weight).
			Int("address_count", len(aggregation.Addresses)).
			Str("primary_address", primaryAddress).
			Msg("Created final Farcaster user entry")
	}

	log.Info().
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
