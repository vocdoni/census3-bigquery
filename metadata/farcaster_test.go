package metadata

import (
	"github.com/vocdoni/census3-bigquery/censusdb"
	"github.com/vocdoni/census3-bigquery/config"
	"github.com/vocdoni/census3-bigquery/neynar"
	"github.com/vocdoni/census3-bigquery/storage"
	"encoding/json"
	"testing"
	"time"

	"github.com/frankban/quicktest"
	"github.com/google/uuid"
	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/db/metadb"
	"github.com/vocdoni/davinci-node/types"
)

func TestFarcasterProcessor(t *testing.T) {
	c := quicktest.New(t)

	// Create test database
	database, err := metadb.New(db.TypePebble, t.TempDir())
	c.Assert(err, quicktest.IsNil)

	// Create storage and census DB
	kvStorage := storage.NewKVSnapshotStorage(database)
	censusDB := censusdb.NewCensusDB(database)

	// Create Farcaster processor
	processor := NewFarcasterProcessor(nil, kvStorage)

	// Create test census with some addresses
	censusID := uuid.New()
	censusRef, err := censusDB.New(censusID)
	c.Assert(err, quicktest.IsNil)

	// Add test addresses to census
	testAddresses := []string{
		"0xe1b8799659bE5d41a0e57E179d6cB42E00B9211C",
		"0x742d35Cc6634C0532925a3b8D4C9db96C4b4d8b6",
	}

	for i, addr := range testAddresses {
		// Hash the address key to fit within census tree limits
		key := censusDB.TrunkKey([]byte(addr))
		c.Assert(key, quicktest.Not(quicktest.IsNil))
		value := []byte{byte(i + 1)} // Simple weight
		err = censusRef.Insert(key, value)
		c.Assert(err, quicktest.IsNil)
	}

	censusRoot := types.HexBytes(censusRef.Root())

	// Test metadata storage and retrieval
	testMetadata := FarcasterMetadata{
		CensusRoot: censusRoot,
		Users: []FarcasterUser{
			{
				Username: "testuser1",
				Weight:   1.0,
				FID:      12345,
				Address:  testAddresses[0],
			},
			{
				Username: "testuser2",
				Weight:   2.0,
				FID:      67890,
				Address:  testAddresses[1],
			},
		},
		CreatedAt:  time.Now(),
		TotalUsers: 2,
	}

	// Serialize and store metadata
	metadataJSON, err := json.Marshal(testMetadata)
	c.Assert(err, quicktest.IsNil)

	err = kvStorage.StoreMetadata(FarcasterMetadataType, censusRoot, metadataJSON)
	c.Assert(err, quicktest.IsNil)

	// Test HasMetadata
	hasMetadata, err := processor.HasMetadata(censusRoot)
	c.Assert(err, quicktest.IsNil)
	c.Assert(hasMetadata, quicktest.IsTrue)

	// Test GetMetadata
	retrievedMetadata, err := processor.GetMetadata(censusRoot)
	c.Assert(err, quicktest.IsNil)
	c.Assert(retrievedMetadata, quicktest.Not(quicktest.IsNil))
	c.Assert(retrievedMetadata.TotalUsers, quicktest.Equals, 2)
	c.Assert(len(retrievedMetadata.Users), quicktest.Equals, 2)
	c.Assert(retrievedMetadata.Users[0].Username, quicktest.Equals, "testuser1")
	c.Assert(retrievedMetadata.Users[1].Username, quicktest.Equals, "testuser2")

	// Test DeleteMetadata
	err = processor.DeleteMetadata(censusRoot)
	c.Assert(err, quicktest.IsNil)

	// Verify metadata is deleted
	hasMetadata, err = processor.HasMetadata(censusRoot)
	c.Assert(err, quicktest.IsNil)
	c.Assert(hasMetadata, quicktest.IsFalse)

	retrievedMetadata, err = processor.GetMetadata(censusRoot)
	c.Assert(err, quicktest.IsNil)
	c.Assert(retrievedMetadata, quicktest.IsNil)
}

func TestFarcasterProcessorWithMockNeynar(t *testing.T) {
	c := quicktest.New(t)

	// Create test database
	database, err := metadb.New(db.TypePebble, t.TempDir())
	c.Assert(err, quicktest.IsNil)

	// Create storage and census DB
	kvStorage := storage.NewKVSnapshotStorage(database)
	censusDB := censusdb.NewCensusDB(database)

	// Create Farcaster processor
	processor := NewFarcasterProcessor(nil, kvStorage)

	// Create test census with some addresses
	censusID2 := uuid.New()
	censusRef, err := censusDB.New(censusID2)
	c.Assert(err, quicktest.IsNil)

	// Add test addresses to census
	testAddresses := []string{
		"0xe1b8799659bE5d41a0e57E179d6cB42E00B9211C",
		"0x742d35Cc6634C0532925a3b8D4C9db96C4b4d8b6",
	}

	for i, addr := range testAddresses {
		// Hash the address key to fit within census tree limits
		key := censusDB.TrunkKey([]byte(addr))
		c.Assert(key, quicktest.Not(quicktest.IsNil))
		value := []byte{byte(i + 1)} // Simple weight
		err = censusRef.Insert(key, value)
		c.Assert(err, quicktest.IsNil)
	}

	// Create proper weights map with original addresses (not hashed)
	weights := map[string]float64{
		testAddresses[0]: 100.5, // Real weight for first address
		testAddresses[1]: 75.2,  // Real weight for second address
	}

	// Test processFarcasterUsers method with proper weights
	mockNeynarUsers := map[string][]neynar.User{
		testAddresses[0]: {
			{
				Username: "testuser1",
				FID:      12345,
			},
		},
		testAddresses[1]: {
			{
				Username: "testuser2",
				FID:      67890,
			},
		},
	}

	farcasterUsers := processor.processFarcasterUsers(mockNeynarUsers, weights)
	c.Assert(len(farcasterUsers), quicktest.Equals, 2)

	// Check that both users are present (order may vary due to map iteration)
	usernames := make(map[string]bool)
	for _, user := range farcasterUsers {
		usernames[user.Username] = true
	}
	c.Assert(usernames["testuser1"], quicktest.IsTrue)
	c.Assert(usernames["testuser2"], quicktest.IsTrue)
}

func TestFarcasterWeightAggregation(t *testing.T) {
	c := quicktest.New(t)

	// Create test database
	database, err := metadb.New(db.TypePebble, t.TempDir())
	c.Assert(err, quicktest.IsNil)

	// Create storage
	kvStorage := storage.NewKVSnapshotStorage(database)

	// Create Farcaster processor
	processor := NewFarcasterProcessor(nil, kvStorage)

	// Test addresses - alice controls two addresses, bob controls one
	testAddresses := []string{
		"0xe1b8799659bE5d41a0e57E179d6cB42E00B9211C", // Alice's first address
		"0x742d35Cc6634C0532925a3b8D4C9db96C4b4d8b6", // Alice's second address
		"0x8ba1f109551bD432803012645Hac136c",         // Bob's address
	}

	// Create weights map with different weights for each address
	weights := map[string]float64{
		testAddresses[0]: 150.5, // Alice's first address weight
		testAddresses[1]: 75.2,  // Alice's second address weight
		testAddresses[2]: 200.0, // Bob's address weight
	}

	// Mock Neynar response - Alice appears for two addresses, Bob for one
	mockNeynarUsers := map[string][]neynar.User{
		testAddresses[0]: {
			{
				Username: "alice",
				FID:      12345, // Same FID for Alice
			},
		},
		testAddresses[1]: {
			{
				Username: "alice",
				FID:      12345, // Same FID for Alice (should be aggregated)
			},
		},
		testAddresses[2]: {
			{
				Username: "bob",
				FID:      67890, // Different FID for Bob
			},
		},
	}

	// Process users and test aggregation
	farcasterUsers := processor.processFarcasterUsers(mockNeynarUsers, weights)
	c.Assert(len(farcasterUsers), quicktest.Equals, 2) // Should have 2 unique users

	// Find Alice and Bob in the results
	var alice, bob *FarcasterUser
	for i := range farcasterUsers {
		switch farcasterUsers[i].Username {
		case "alice":
			alice = &farcasterUsers[i]
		case "bob":
			bob = &farcasterUsers[i]
		}
	}

	// Verify Alice's weight is aggregated (150.5 + 75.2 = 225.7)
	c.Assert(alice, quicktest.Not(quicktest.IsNil))
	c.Assert(alice.Weight, quicktest.Equals, 225.7)
	c.Assert(alice.FID, quicktest.Equals, int64(12345))

	// Verify Bob's weight is not aggregated (just 200.0)
	c.Assert(bob, quicktest.Not(quicktest.IsNil))
	c.Assert(bob.Weight, quicktest.Equals, 200.0)
	c.Assert(bob.FID, quicktest.Equals, int64(67890))
}

func TestFarcasterConfigHelpers(t *testing.T) {
	c := quicktest.New(t)

	// Test query config without Farcaster metadata
	queryConfig := &config.QueryConfig{
		Name: "test_query",
	}

	c.Assert(queryConfig.HasFarcasterMetadata(), quicktest.IsFalse)
	c.Assert(queryConfig.GetFarcasterConfig(), quicktest.IsNil)

	// Test query config with Farcaster metadata
	queryConfigWithFarcaster := &config.QueryConfig{
		Name: "test_query_farcaster",
		Metadata: &config.MetadataConfig{
			Farcaster: &config.FarcasterConfig{
				NeynarAPIKey: "test-api-key",
			},
		},
	}

	c.Assert(queryConfigWithFarcaster.HasFarcasterMetadata(), quicktest.IsTrue)
	farcasterConfig := queryConfigWithFarcaster.GetFarcasterConfig()
	c.Assert(farcasterConfig, quicktest.Not(quicktest.IsNil))
	c.Assert(farcasterConfig.NeynarAPIKey, quicktest.Equals, "test-api-key")

	// Test query config with empty API key
	queryConfigEmptyKey := &config.QueryConfig{
		Name: "test_query_empty",
		Metadata: &config.MetadataConfig{
			Farcaster: &config.FarcasterConfig{
				NeynarAPIKey: "",
			},
		},
	}

	c.Assert(queryConfigEmptyKey.HasFarcasterMetadata(), quicktest.IsFalse)
	c.Assert(queryConfigEmptyKey.GetFarcasterConfig(), quicktest.IsNil)
}
