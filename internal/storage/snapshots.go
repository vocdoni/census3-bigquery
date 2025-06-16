package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/vocdoni/davinci-node/types"
)

// Snapshot represents a census snapshot
type Snapshot struct {
	SnapshotDate     time.Time      `json:"snapshotDate"`
	CensusRoot       types.HexBytes `json:"censusRoot"`
	ParticipantCount int            `json:"participantCount"`
	CreatedAt        time.Time      `json:"createdAt"`
	MinBalance       float64        `json:"minBalance"`         // ETH amount used to compute the census
	Filename         string         `json:"filename,omitempty"` // Optional: compressed CSV filename for Git storage
}

// SnapshotStorage manages persistent storage of snapshots
type SnapshotStorage struct {
	filePath  string
	snapshots []Snapshot
	mu        sync.RWMutex
}

// StorageData represents the JSON structure stored in the file
type StorageData struct {
	Snapshots []Snapshot `json:"snapshots"`
}

// NewSnapshotStorage creates a new snapshot storage instance
func NewSnapshotStorage(filePath string) *SnapshotStorage {
	return &SnapshotStorage{
		filePath:  filePath,
		snapshots: make([]Snapshot, 0),
	}
}

// Load loads snapshots from the storage file
func (s *SnapshotStorage) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if file exists
	if _, err := os.Stat(s.filePath); os.IsNotExist(err) {
		// File doesn't exist, start with empty snapshots
		s.snapshots = make([]Snapshot, 0)
		return nil
	}

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return fmt.Errorf("failed to read storage file: %w", err)
	}

	if len(data) == 0 {
		// Empty file, start with empty snapshots
		s.snapshots = make([]Snapshot, 0)
		return nil
	}

	var storageData StorageData
	if err := json.Unmarshal(data, &storageData); err != nil {
		return fmt.Errorf("failed to unmarshal storage data: %w", err)
	}

	s.snapshots = storageData.Snapshots
	return nil
}

// Save saves snapshots to the storage file
func (s *SnapshotStorage) Save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	storageData := StorageData{
		Snapshots: s.snapshots,
	}

	data, err := json.MarshalIndent(storageData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal storage data: %w", err)
	}

	if err := os.WriteFile(s.filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write storage file: %w", err)
	}

	return nil
}

// AddSnapshot adds a new snapshot to storage
func (s *SnapshotStorage) AddSnapshot(snapshotDate time.Time, censusRoot types.HexBytes, participantCount int, minBalance float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := Snapshot{
		SnapshotDate:     snapshotDate,
		CensusRoot:       censusRoot,
		ParticipantCount: participantCount,
		CreatedAt:        time.Now(),
		MinBalance:       minBalance,
	}

	s.snapshots = append(s.snapshots, snapshot)

	// Sort snapshots by snapshot date (most recent first)
	sort.Slice(s.snapshots, func(i, j int) bool {
		return s.snapshots[i].SnapshotDate.After(s.snapshots[j].SnapshotDate)
	})

	return s.save()
}

// GetSnapshots returns all snapshots ordered by most recent first
func (s *SnapshotStorage) GetSnapshots() []Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return a copy to avoid race conditions
	snapshots := make([]Snapshot, len(s.snapshots))
	copy(snapshots, s.snapshots)
	return snapshots
}

// GetLatestSnapshot returns the most recent snapshot, or nil if none exist
func (s *SnapshotStorage) GetLatestSnapshot() *Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.snapshots) == 0 {
		return nil
	}

	// Return a copy to avoid race conditions
	latest := s.snapshots[0]
	return &latest
}

// save is an internal method that saves without acquiring the lock
func (s *SnapshotStorage) save() error {
	storageData := StorageData{
		Snapshots: s.snapshots,
	}

	data, err := json.MarshalIndent(storageData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal storage data: %w", err)
	}

	if err := os.WriteFile(s.filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write storage file: %w", err)
	}

	return nil
}
