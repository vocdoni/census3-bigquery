package storage

import (
	"testing"
	"time"
)

func TestGitStorageFilename(t *testing.T) {
	gitStorage := &GitStorage{}

	testTime := time.Date(2025, 6, 17, 12, 30, 0, 0, time.UTC)

	// Test filename generation with different queries
	filename1 := gitStorage.generateFilename(testTime, 0.25, "ethereum_balances")
	expected1 := "2025-06-17-123000-ethereum_balances-0.25.gz"
	if filename1 != expected1 {
		t.Errorf("Expected filename %s, got %s", expected1, filename1)
	}

	filename2 := gitStorage.generateFilename(testTime, 1.0, "ethereum_balances_recent")
	expected2 := "2025-06-17-123000-ethereum_balances_recent-1.00.gz"
	if filename2 != expected2 {
		t.Errorf("Expected filename %s, got %s", expected2, filename2)
	}
}
