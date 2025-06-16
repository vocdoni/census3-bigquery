package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vocdoni/davinci-node/types"
)

func TestGitStorageCreation(t *testing.T) {
	// Test creating a new Git storage instance
	repoURL := "https://github.com/test/repo"
	pat := "test-token"
	localPath := "./test-repo"

	gitStorage, err := NewGitStorage(repoURL, pat, localPath)
	if err != nil {
		t.Fatalf("Failed to create Git storage: %v", err)
	}

	if gitStorage.repoURL != repoURL {
		t.Errorf("Expected repo URL %s, got %s", repoURL, gitStorage.repoURL)
	}

	if gitStorage.localPath != localPath {
		t.Errorf("Expected local path %s, got %s", localPath, gitStorage.localPath)
	}

	if gitStorage.auth.Username != "token" {
		t.Errorf("Expected auth username 'token', got %s", gitStorage.auth.Username)
	}

	if gitStorage.auth.Password != pat {
		t.Errorf("Expected auth password %s, got %s", pat, gitStorage.auth.Password)
	}
}

func TestGitStorageFilenameGeneration(t *testing.T) {
	gitStorage, err := NewGitStorage("https://github.com/test/repo", "test-token", "./test-repo")
	if err != nil {
		t.Fatalf("Failed to create Git storage: %v", err)
	}

	// Test filename generation
	testDate := time.Date(2025, 6, 16, 17, 30, 45, 0, time.UTC)
	minBalance := 0.25

	filename := gitStorage.generateFilename(testDate, minBalance)
	expected := "2025-06-16-173045-ethereum-0.25.gz"

	if filename != expected {
		t.Errorf("Expected filename %s, got %s", expected, filename)
	}

	// Test with different balance
	minBalance = 1.5
	filename = gitStorage.generateFilename(testDate, minBalance)
	expected = "2025-06-16-173045-ethereum-1.50.gz"

	if filename != expected {
		t.Errorf("Expected filename %s, got %s", expected, filename)
	}
}

func TestGitStorageCompressFile(t *testing.T) {
	gitStorage, err := NewGitStorage("https://github.com/test/repo", "test-token", "./test-repo")
	if err != nil {
		t.Fatalf("Failed to create Git storage: %v", err)
	}

	// Create a temporary source file
	tmpDir, err := os.MkdirTemp("", "git_storage_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("Warning: failed to remove temp dir: %v", err)
		}
	}()

	srcPath := filepath.Join(tmpDir, "test.csv")
	dstPath := filepath.Join(tmpDir, "test.csv.gz")

	// Write test data to source file
	testData := "address,balance\n0x123,1.5\n0x456,2.0\n"
	if err := os.WriteFile(srcPath, []byte(testData), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Compress the file
	if err := gitStorage.compressFile(srcPath, dstPath); err != nil {
		t.Fatalf("Failed to compress file: %v", err)
	}

	// Check that compressed file exists
	if _, err := os.Stat(dstPath); os.IsNotExist(err) {
		t.Errorf("Compressed file does not exist: %s", dstPath)
	}

	// Check that compressed file is smaller than original (for this test data it should be)
	srcInfo, err := os.Stat(srcPath)
	if err != nil {
		t.Fatalf("Failed to stat source file: %v", err)
	}

	dstInfo, err := os.Stat(dstPath)
	if err != nil {
		t.Fatalf("Failed to stat compressed file: %v", err)
	}

	// For small files, gzip might actually be larger due to headers, so just check it exists
	if dstInfo.Size() == 0 {
		t.Errorf("Compressed file is empty")
	}

	t.Logf("Original size: %d bytes, Compressed size: %d bytes", srcInfo.Size(), dstInfo.Size())
}

func TestGitStorageGetSnapshots(t *testing.T) {
	gitStorage, err := NewGitStorage("https://github.com/test/repo", "test-token", "./test-repo")
	if err != nil {
		t.Fatalf("Failed to create Git storage: %v", err)
	}

	// Test getting snapshots from empty storage
	snapshots := gitStorage.GetSnapshots()
	if len(snapshots) != 0 {
		t.Errorf("Expected 0 snapshots, got %d", len(snapshots))
	}

	// Test getting latest snapshot from empty storage
	latest := gitStorage.GetLatestSnapshot()
	if latest != nil {
		t.Errorf("Expected nil latest snapshot, got %v", latest)
	}

	// Add some test snapshots manually
	testSnapshots := []Snapshot{
		{
			SnapshotDate:     time.Now().Add(-2 * time.Hour),
			CensusRoot:       types.HexBytes{0x01, 0x02, 0x03},
			ParticipantCount: 100,
			CreatedAt:        time.Now().Add(-2 * time.Hour),
			Filename:         "2025-06-16-150000-ethereum-0.25.gz",
		},
		{
			SnapshotDate:     time.Now().Add(-1 * time.Hour),
			CensusRoot:       types.HexBytes{0x04, 0x05, 0x06},
			ParticipantCount: 200,
			CreatedAt:        time.Now().Add(-1 * time.Hour),
			Filename:         "2025-06-16-160000-ethereum-0.25.gz",
		},
	}

	// Set snapshots in reverse chronological order (most recent first)
	gitStorage.snapshots = []Snapshot{testSnapshots[1], testSnapshots[0]}

	// Test getting snapshots
	snapshots = gitStorage.GetSnapshots()
	if len(snapshots) != 2 {
		t.Errorf("Expected 2 snapshots, got %d", len(snapshots))
	}

	// Test getting latest snapshot
	latest = gitStorage.GetLatestSnapshot()
	if latest == nil {
		t.Fatalf("Expected latest snapshot, got nil")
	}

	// Should be the more recent one (first in our sorted array)
	if latest.ParticipantCount != 200 {
		t.Errorf("Expected latest snapshot participant count 200, got %d", latest.ParticipantCount)
	}
}
