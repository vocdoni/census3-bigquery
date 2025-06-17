package storage

import (
	"os"
	"testing"
	"time"

	"github.com/vocdoni/davinci-node/types"
)

func TestSnapshotStorage(t *testing.T) {
	// Create temporary file for testing
	tmpFile, err := os.CreateTemp("", "snapshots_test_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() {
		if err := os.Remove(tmpFile.Name()); err != nil {
			t.Logf("Warning: failed to remove temp file: %v", err)
		}
	}()
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

	// Create storage instance
	storage := NewSnapshotStorage(tmpFile.Name())

	// Test loading empty storage
	err = storage.Load()
	if err != nil {
		t.Fatalf("Failed to load empty storage: %v", err)
	}

	// Test getting snapshots from empty storage
	snapshots := storage.GetSnapshots()
	if len(snapshots) != 0 {
		t.Errorf("Expected 0 snapshots, got %d", len(snapshots))
	}

	// Test getting latest snapshot from empty storage
	latest := storage.GetLatestSnapshot()
	if latest != nil {
		t.Errorf("Expected nil latest snapshot, got %v", latest)
	}

	// Test adding a snapshot
	snapshotDate := time.Now().Truncate(time.Hour)
	censusRoot := types.HexBytes{0x01, 0x02, 0x03}
	participantCount := 100

	err = storage.AddSnapshot(snapshotDate, censusRoot, participantCount, 0.25, "ethereum_balances")
	if err != nil {
		t.Fatalf("Failed to add snapshot: %v", err)
	}

	// Test getting snapshots after adding one
	snapshots = storage.GetSnapshots()
	if len(snapshots) != 1 {
		t.Errorf("Expected 1 snapshot, got %d", len(snapshots))
	}

	snapshot := snapshots[0]
	if !snapshot.SnapshotDate.Equal(snapshotDate) {
		t.Errorf("Expected snapshot date %v, got %v", snapshotDate, snapshot.SnapshotDate)
	}
	if string(snapshot.CensusRoot) != string(censusRoot) {
		t.Errorf("Expected census root %x, got %x", censusRoot, snapshot.CensusRoot)
	}
	if snapshot.ParticipantCount != participantCount {
		t.Errorf("Expected participant count %d, got %d", participantCount, snapshot.ParticipantCount)
	}

	// Test getting latest snapshot
	latest = storage.GetLatestSnapshot()
	if latest == nil {
		t.Fatalf("Expected latest snapshot, got nil")
	}
	if !latest.SnapshotDate.Equal(snapshotDate) {
		t.Errorf("Expected latest snapshot date %v, got %v", snapshotDate, latest.SnapshotDate)
	}

	// Test adding another snapshot (newer)
	newerDate := snapshotDate.Add(time.Hour)
	newerRoot := types.HexBytes{0x04, 0x05, 0x06}
	newerCount := 200

	err = storage.AddSnapshot(newerDate, newerRoot, newerCount, 1.0, "ethereum_balances_recent")
	if err != nil {
		t.Fatalf("Failed to add newer snapshot: %v", err)
	}

	// Test that snapshots are ordered by most recent first
	snapshots = storage.GetSnapshots()
	if len(snapshots) != 2 {
		t.Errorf("Expected 2 snapshots, got %d", len(snapshots))
	}

	// First snapshot should be the newer one
	if !snapshots[0].SnapshotDate.Equal(newerDate) {
		t.Errorf("Expected first snapshot date %v, got %v", newerDate, snapshots[0].SnapshotDate)
	}
	if !snapshots[1].SnapshotDate.Equal(snapshotDate) {
		t.Errorf("Expected second snapshot date %v, got %v", snapshotDate, snapshots[1].SnapshotDate)
	}

	// Test latest snapshot is the newer one
	latest = storage.GetLatestSnapshot()
	if !latest.SnapshotDate.Equal(newerDate) {
		t.Errorf("Expected latest snapshot date %v, got %v", newerDate, latest.SnapshotDate)
	}

	// Test persistence by creating new storage instance
	newStorage := NewSnapshotStorage(tmpFile.Name())
	err = newStorage.Load()
	if err != nil {
		t.Fatalf("Failed to load persisted storage: %v", err)
	}

	persistedSnapshots := newStorage.GetSnapshots()
	if len(persistedSnapshots) != 2 {
		t.Errorf("Expected 2 persisted snapshots, got %d", len(persistedSnapshots))
	}

	// Verify the data is correctly persisted
	if !persistedSnapshots[0].SnapshotDate.Equal(newerDate) {
		t.Errorf("Expected persisted first snapshot date %v, got %v", newerDate, persistedSnapshots[0].SnapshotDate)
	}
}

func TestSnapshotStorageNonExistentFile(t *testing.T) {
	// Test loading from non-existent file
	storage := NewSnapshotStorage("/tmp/non_existent_file.json")
	err := storage.Load()
	if err != nil {
		t.Fatalf("Failed to load from non-existent file: %v", err)
	}

	snapshots := storage.GetSnapshots()
	if len(snapshots) != 0 {
		t.Errorf("Expected 0 snapshots from non-existent file, got %d", len(snapshots))
	}
}
