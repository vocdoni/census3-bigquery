package storage

import (
	"testing"
	"time"

	"github.com/frankban/quicktest"
	"github.com/vocdoni/davinci-node/db/metadb"
	"github.com/vocdoni/davinci-node/types"
)

func TestKVSnapshotStorage(t *testing.T) {
	c := quicktest.New(t)

	// Create test database
	database := metadb.NewTest(t)
	storage := NewKVSnapshotStorage(database)

	// Test data
	now := time.Now().Truncate(time.Minute)
	testSnapshots := []struct {
		date    time.Time
		root    types.HexBytes
		count   int
		balance float64
		query   string
	}{
		{now, types.HexBytes{0x01, 0x02, 0x03}, 100, 0.25, "ethereum_balances"},
		{now.Add(-time.Hour), types.HexBytes{0x04, 0x05, 0x06}, 200, 0.5, "ethereum_balances"},
		{now.Add(-2 * time.Hour), types.HexBytes{0x07, 0x08, 0x09}, 150, 0.25, "ethereum_balances_recent"},
		{now.Add(-3 * time.Hour), types.HexBytes{0x0a, 0x0b, 0x0c}, 300, 1.0, "ethereum_balances"},
	}

	// Add snapshots
	for _, snapshot := range testSnapshots {
		// Create test weight config
		weightConfig := &WeightConfig{
			Strategy:   "proportional_manual",
			Multiplier: func() *float64 { v := 100.0; return &v }(),
		}

		// Create test parameters
		parameters := map[string]interface{}{
			"min_balance": snapshot.balance,
		}

		err := storage.AddSnapshot(snapshot.date, snapshot.root, snapshot.count, snapshot.balance, snapshot.query, snapshot.query, 18, "1h", parameters, weightConfig, "Test Display Name", "")
		c.Assert(err, quicktest.IsNil)
	}

	// Test Snapshots (should be ordered by most recent first)
	snapshots, err := storage.Snapshots()
	c.Assert(err, quicktest.IsNil)
	c.Assert(len(snapshots), quicktest.Equals, len(testSnapshots))

	// Verify ordering (most recent first)
	for i := 1; i < len(snapshots); i++ {
		c.Assert(snapshots[i-1].SnapshotDate.After(snapshots[i].SnapshotDate), quicktest.IsTrue)
	}

	// Test LatestSnapshot
	latest, err := storage.LatestSnapshot()
	c.Assert(err, quicktest.IsNil)
	c.Assert(latest, quicktest.Not(quicktest.IsNil))
	c.Assert(latest.SnapshotDate.Equal(now), quicktest.IsTrue)
	c.Assert(latest.ParticipantCount, quicktest.Equals, 100)

	// Test SnapshotsByBalance
	balanceSnapshots, err := storage.SnapshotsByBalance(0.25)
	c.Assert(err, quicktest.IsNil)
	c.Assert(len(balanceSnapshots), quicktest.Equals, 2) // Two snapshots with 0.25 balance

	// Test SnapshotsByQuery
	querySnapshots, err := storage.SnapshotsByQuery("ethereum_balances")
	c.Assert(err, quicktest.IsNil)
	c.Assert(len(querySnapshots), quicktest.Equals, 3) // Three snapshots with ethereum_balances query

	// Test SnapshotCount
	count, err := storage.SnapshotCount()
	c.Assert(err, quicktest.IsNil)
	c.Assert(count, quicktest.Equals, len(testSnapshots))

	// Test DeleteOldSnapshots
	deleted, err := storage.DeleteOldSnapshots(90 * time.Minute) // Delete snapshots older than 90 minutes
	c.Assert(err, quicktest.IsNil)
	c.Assert(deleted, quicktest.Equals, 2) // Should delete 2 snapshots (2h and 3h old)

	// Create a new storage instance to ensure we see the committed changes
	storage2 := NewKVSnapshotStorage(database)

	// Verify deletion
	remainingSnapshots, err := storage2.Snapshots()
	c.Assert(err, quicktest.IsNil)
	c.Assert(len(remainingSnapshots), quicktest.Equals, 2) // Should have 2 remaining

	// Test that latest snapshot is still the most recent
	newLatest, err := storage.LatestSnapshot()
	c.Assert(err, quicktest.IsNil)
	c.Assert(newLatest.SnapshotDate.Equal(now), quicktest.IsTrue)
}

func TestKVSnapshotStorageEmpty(t *testing.T) {
	c := quicktest.New(t)

	// Create test database
	database := metadb.NewTest(t)
	storage := NewKVSnapshotStorage(database)

	// Test empty storage
	snapshots, err := storage.Snapshots()
	c.Assert(err, quicktest.IsNil)
	c.Assert(len(snapshots), quicktest.Equals, 0)

	latest, err := storage.LatestSnapshot()
	c.Assert(err, quicktest.IsNil)
	c.Assert(latest, quicktest.IsNil)

	count, err := storage.SnapshotCount()
	c.Assert(err, quicktest.IsNil)
	c.Assert(count, quicktest.Equals, 0)

	deleted, err := storage.DeleteOldSnapshots(time.Hour)
	c.Assert(err, quicktest.IsNil)
	c.Assert(deleted, quicktest.Equals, 0)
}

func TestKVSnapshotStorageKeyGeneration(t *testing.T) {
	c := quicktest.New(t)

	database := metadb.NewTest(t)
	storage := NewKVSnapshotStorage(database)

	// Test key generation for sorting
	now := time.Now()
	older := now.Add(-time.Hour)

	// Generate keys
	key1 := storage.generateSnapshotKey(now, 0.25, "test")
	key2 := storage.generateSnapshotKey(older, 0.25, "test")

	// Keys should be different
	c.Assert(string(key1), quicktest.Not(quicktest.Equals), string(key2))

	// Test index key generation
	balanceKey1 := storage.generateIndexKey(balanceIndexPrefix, 0.25)
	balanceKey2 := storage.generateIndexKey(balanceIndexPrefix, 0.5)
	c.Assert(string(balanceKey1), quicktest.Not(quicktest.Equals), string(balanceKey2))

	queryKey1 := storage.generateIndexKey(queryIndexPrefix, "query1")
	queryKey2 := storage.generateIndexKey(queryIndexPrefix, "query2")
	c.Assert(string(queryKey1), quicktest.Not(quicktest.Equals), string(queryKey2))
}

func TestKVSnapshotStoragePerformance(t *testing.T) {
	c := quicktest.New(t)

	database := metadb.NewTest(t)
	storage := NewKVSnapshotStorage(database)

	// Add many snapshots to test performance
	numSnapshots := 1000
	start := time.Now()

	for i := 0; i < numSnapshots; i++ {
		snapshotTime := time.Now().Add(-time.Duration(i) * time.Minute)
		root := types.HexBytes{byte(i), byte(i >> 8), byte(i >> 16)}
		balance := float64(i%10) * 0.1
		query := "test_query"

		// Create test weight config
		weightConfig := &WeightConfig{
			Strategy:   "proportional_manual",
			Multiplier: func() *float64 { v := 100.0; return &v }(),
		}

		// Create test parameters
		parameters := map[string]interface{}{
			"min_balance": balance,
		}

		err := storage.AddSnapshot(snapshotTime, root, i*10, balance, query, query, 18, "1h", parameters, weightConfig, "Test Display Name", "")
		c.Assert(err, quicktest.IsNil)
	}

	addDuration := time.Since(start)
	t.Logf("Added %d snapshots in %v (%.2f snapshots/sec)",
		numSnapshots, addDuration, float64(numSnapshots)/addDuration.Seconds())

	// Test retrieval performance
	start = time.Now()
	snapshots, err := storage.Snapshots()
	c.Assert(err, quicktest.IsNil)
	c.Assert(len(snapshots), quicktest.Equals, numSnapshots)
	retrievalDuration := time.Since(start)
	t.Logf("Retrieved %d snapshots in %v", numSnapshots, retrievalDuration)

	// Test filtered queries
	start = time.Now()
	balanceSnapshots, err := storage.SnapshotsByBalance(0.5)
	c.Assert(err, quicktest.IsNil)
	c.Assert(len(balanceSnapshots) > 0, quicktest.IsTrue)
	balanceQueryDuration := time.Since(start)
	t.Logf("Retrieved %d snapshots by balance in %v", len(balanceSnapshots), balanceQueryDuration)

	start = time.Now()
	querySnapshots, err := storage.SnapshotsByQuery("test_query")
	c.Assert(err, quicktest.IsNil)
	c.Assert(len(querySnapshots), quicktest.Equals, numSnapshots)
	queryQueryDuration := time.Since(start)
	t.Logf("Retrieved %d snapshots by query in %v", len(querySnapshots), queryQueryDuration)
}
