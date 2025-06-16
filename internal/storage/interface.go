package storage

import (
	"time"

	"github.com/vocdoni/davinci-node/types"
)

// Storage interface defines the common operations for snapshot storage
type Storage interface {
	Load() error
	Save() error
	GetSnapshots() []Snapshot
	GetLatestSnapshot() *Snapshot
}

// SnapshotAdder interface for adding snapshots (different signatures for local vs git)
type SnapshotAdder interface {
	Storage
}

// LocalSnapshotAdder interface for local storage
type LocalSnapshotAdder interface {
	SnapshotAdder
	AddSnapshot(snapshotDate time.Time, censusRoot types.HexBytes, participantCount int) error
}

// GitSnapshotAdder interface for Git storage
type GitSnapshotAdder interface {
	SnapshotAdder
	AddSnapshot(snapshotDate time.Time, censusRoot types.HexBytes, participantCount int, csvPath string, minBalance float64) error
}
