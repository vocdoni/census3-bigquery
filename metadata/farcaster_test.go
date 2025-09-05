package metadata

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/frankban/quicktest"
	"github.com/google/uuid"
	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/db/metadb"
	"github.com/vocdoni/davinci-node/types"

	"census3-bigquery/censusdb"
	"census3-bigquery/config"
	"census3-bigquery/neynar"
	"census3-bigquery/storage"
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
		key := censusDB.HashAndTrunkKey([]byte(addr))
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
		key := censusDB.HashAndTrunkKey([]byte(addr))
		c.Assert(key, quicktest.Not(quicktest.IsNil))
		value := []byte{byte(i + 1)} // Simple weight
		err = censusRef.Insert(key, value)
		c.Assert(err, quicktest.IsNil)
	}

	// Test extractAddressesAndWeights method
	addresses, weights, err := processor.extractAddressesAndWeights(censusRef)
	c.Assert(err, quicktest.IsNil)
	c.Assert(len(addresses), quicktest.Not(quicktest.Equals), 0) // Should have some addresses
	c.Assert(len(weights), quicktest.Equals, len(addresses))     // Weights should match addresses

	// Test processFarcasterUsers method
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
