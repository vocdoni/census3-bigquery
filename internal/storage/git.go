package storage

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/rs/zerolog/log"
	"github.com/vocdoni/davinci-node/types"
)

// GitStorage manages snapshots in a Git repository
type GitStorage struct {
	repo               *git.Repository
	worktree           *git.Worktree
	auth               *http.BasicAuth
	repoURL            string
	localPath          string
	snapshots          []Snapshot
	maxSnapshotsToKeep int
	skipCSVUpload      bool
	mu                 sync.RWMutex
}

// NewGitStorage creates a new Git storage instance
func NewGitStorage(repoURL, pat, localPath string, maxSnapshotsToKeep int, skipCSVUpload bool) (*GitStorage, error) {
	auth := &http.BasicAuth{
		Username: "token", // GitHub PAT uses "token" as username
		Password: pat,
	}

	gs := &GitStorage{
		auth:               auth,
		repoURL:            repoURL,
		localPath:          localPath,
		snapshots:          make([]Snapshot, 0),
		maxSnapshotsToKeep: maxSnapshotsToKeep,
		skipCSVUpload:      skipCSVUpload,
	}

	return gs, nil
}

// Load clones or pulls the repository and loads snapshots
func (gs *GitStorage) Load() error {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	var err error

	// Check if repository already exists locally
	if _, err := os.Stat(gs.localPath); os.IsNotExist(err) {
		// Clone repository
		log.Info().Str("repo_url", gs.repoURL).Str("local_path", gs.localPath).Msg("Cloning repository")
		gs.repo, err = git.PlainClone(gs.localPath, false, &git.CloneOptions{
			URL:  gs.repoURL,
			Auth: gs.auth,
		})
		if err != nil {
			// If repository is empty, initialize it locally
			if err.Error() == "remote repository is empty" {
				log.Info().Msg("Remote repository is empty, initializing local repository")
				gs.repo, err = git.PlainInit(gs.localPath, false)
				if err != nil {
					return fmt.Errorf("failed to initialize repository: %w", err)
				}
				// Add remote origin
				_, err = gs.repo.CreateRemote(&config.RemoteConfig{
					Name: "origin",
					URLs: []string{gs.repoURL},
				})
				if err != nil {
					return fmt.Errorf("failed to add remote origin: %w", err)
				}
			} else {
				return fmt.Errorf("failed to clone repository: %w", err)
			}
		}
	} else {
		// Open existing repository
		gs.repo, err = git.PlainOpen(gs.localPath)
		if err != nil {
			return fmt.Errorf("failed to open repository: %w", err)
		}

		// Pull latest changes
		gs.worktree, err = gs.repo.Worktree()
		if err != nil {
			return fmt.Errorf("failed to get worktree: %w", err)
		}

		err = gs.worktree.Pull(&git.PullOptions{
			Auth: gs.auth,
		})
		if err != nil && err != git.NoErrAlreadyUpToDate {
			return fmt.Errorf("failed to pull repository: %w", err)
		}
	}

	// Get worktree if not already set
	if gs.worktree == nil {
		gs.worktree, err = gs.repo.Worktree()
		if err != nil {
			return fmt.Errorf("failed to get worktree: %w", err)
		}
	}

	// Load snapshots.json
	return gs.loadSnapshots()
}

// loadSnapshots loads snapshots from the snapshots.json file
func (gs *GitStorage) loadSnapshots() error {
	snapshotsPath := filepath.Join(gs.localPath, "snapshots.json")

	// Check if snapshots.json exists
	if _, err := os.Stat(snapshotsPath); os.IsNotExist(err) {
		// File doesn't exist, start with empty snapshots
		gs.snapshots = make([]Snapshot, 0)
		return nil
	}

	data, err := os.ReadFile(snapshotsPath)
	if err != nil {
		return fmt.Errorf("failed to read snapshots.json: %w", err)
	}

	if len(data) == 0 {
		// Empty file, start with empty snapshots
		gs.snapshots = make([]Snapshot, 0)
		return nil
	}

	var storageData StorageData
	if err := json.Unmarshal(data, &storageData); err != nil {
		return fmt.Errorf("failed to unmarshal snapshots.json: %w", err)
	}

	gs.snapshots = storageData.Snapshots
	return nil
}

// Save saves snapshots to snapshots.json (used for compatibility)
func (gs *GitStorage) Save() error {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	return gs.saveSnapshots()
}

// saveSnapshots saves snapshots to snapshots.json file
func (gs *GitStorage) saveSnapshots() error {
	snapshotsPath := filepath.Join(gs.localPath, "snapshots.json")

	storageData := StorageData{
		Snapshots: gs.snapshots,
	}

	data, err := json.MarshalIndent(storageData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshots data: %w", err)
	}

	if err := os.WriteFile(snapshotsPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write snapshots.json: %w", err)
	}

	return nil
}

// AddSnapshot adds a new snapshot and force pushes to maintain clean repository
func (gs *GitStorage) AddSnapshot(snapshotDate time.Time, censusRoot types.HexBytes, participantCount int, csvPath string, minBalance float64, queryName string) error {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	var filename string

	// Generate filename for compressed CSV if not skipping upload
	if !gs.skipCSVUpload {
		filename = gs.generateFilename(snapshotDate, minBalance, queryName)
	}

	// Create snapshot entry
	snapshot := Snapshot{
		SnapshotDate:     snapshotDate,
		CensusRoot:       censusRoot,
		ParticipantCount: participantCount,
		CreatedAt:        time.Now(),
		MinBalance:       minBalance,
		QueryName:        queryName,
		Filename:         filename, // Will be empty if skipping CSV upload
	}

	// Add new snapshot and sort by date (most recent first)
	gs.snapshots = append(gs.snapshots, snapshot)
	sort.Slice(gs.snapshots, func(i, j int) bool {
		return gs.snapshots[i].SnapshotDate.After(gs.snapshots[j].SnapshotDate)
	})

	// Write fresh snapshots.json (keep all entries, don't remove any)
	if err := gs.saveSnapshots(); err != nil {
		return fmt.Errorf("failed to save snapshots.json: %w", err)
	}

	// Add the new CSV file (if not skipping CSV upload)
	if !gs.skipCSVUpload && filename != "" {
		snapshotsDir := filepath.Join(gs.localPath, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			return fmt.Errorf("failed to create snapshots directory: %w", err)
		}

		// Compress and write the new CSV file
		compressedPath := filepath.Join(snapshotsDir, filename)
		if err := gs.compressFile(csvPath, compressedPath); err != nil {
			return fmt.Errorf("failed to compress CSV file: %w", err)
		}
	}

	// Identify and remove old CSV files (keep only the most recent ones)
	if len(gs.snapshots) > gs.maxSnapshotsToKeep {
		// Create a map of filenames to keep (most recent ones)
		filesToKeep := make(map[string]bool)
		for i := 0; i < gs.maxSnapshotsToKeep && i < len(gs.snapshots); i++ {
			if gs.snapshots[i].Filename != "" {
				filesToKeep[gs.snapshots[i].Filename] = true
			}
		}

		// Find all CSV files in the snapshots directory
		snapshotsDir := filepath.Join(gs.localPath, "snapshots")
		if entries, err := os.ReadDir(snapshotsDir); err == nil {
			var filesToRemove []string
			for _, entry := range entries {
				if !entry.IsDir() && filepath.Ext(entry.Name()) == ".gz" {
					// If this file is not in the "keep" list, mark it for removal
					if !filesToKeep[entry.Name()] {
						filesToRemove = append(filesToRemove, entry.Name())
					}
				}
			}

			if len(filesToRemove) > 0 {
				log.Info().
					Int("files_to_remove", len(filesToRemove)).
					Int("files_to_keep", len(filesToKeep)).
					Int("total_snapshots", len(gs.snapshots)).
					Int("max_limit", gs.maxSnapshotsToKeep).
					Msg("Removing old CSV files (keeping all snapshot entries)")

				// Remove old CSV files from disk and git
				for _, fileToRemove := range filesToRemove {
					filePath := filepath.Join(snapshotsDir, fileToRemove)

					// Remove from disk
					if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
						log.Warn().Err(err).Str("file", fileToRemove).Msg("Failed to remove old snapshot file from disk")
					}

					// Remove from git (only if it exists)
					relativeFilePath := filepath.Join("snapshots", fileToRemove)
					if _, err := gs.worktree.Remove(relativeFilePath); err != nil {
						// Only log as debug since file might not be in git yet
						log.Debug().Err(err).Str("file", fileToRemove).Msg("File not found in git (might not be tracked)")
					}
				}
			}
		}
	}

	// Add all files to git
	if _, err := gs.worktree.Add("."); err != nil {
		return fmt.Errorf("failed to add files to git: %w", err)
	}

	// Commit changes
	commitMsg := fmt.Sprintf("Update snapshots - %s with %d participants (keeping %d snapshots)",
		snapshotDate.Format("2006-01-02 15:04:05"), participantCount, len(gs.snapshots))

	_, err := gs.worktree.Commit(commitMsg, &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Census3 BigQuery Service",
			Email: "census3@vocdoni.io",
			When:  time.Now(),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to commit changes: %w", err)
	}

	// Force push to overwrite remote repository
	err = gs.repo.Push(&git.PushOptions{
		Auth:  gs.auth,
		Force: true, // Force push to maintain clean history
	})
	if err != nil {
		return fmt.Errorf("failed to force push changes: %w", err)
	}

	log.Info().
		Str("filename", filename).
		Int("total_snapshots", len(gs.snapshots)).
		Msg("Successfully force pushed snapshot to Git repository")
	return nil
}

// GetSnapshots returns all snapshots ordered by most recent first
func (gs *GitStorage) GetSnapshots() []Snapshot {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	// Return a copy to avoid race conditions
	snapshots := make([]Snapshot, len(gs.snapshots))
	copy(snapshots, gs.snapshots)
	return snapshots
}

// GetLatestSnapshot returns the most recent snapshot, or nil if none exist
func (gs *GitStorage) GetLatestSnapshot() *Snapshot {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	if len(gs.snapshots) == 0 {
		return nil
	}

	// Return a copy to avoid race conditions
	latest := gs.snapshots[0]
	return &latest
}

// generateFilename generates a filename for the compressed CSV
func (gs *GitStorage) generateFilename(snapshotDate time.Time, minBalance float64, queryName string) string {
	timestamp := snapshotDate.Format("2006-01-02-150405")
	return fmt.Sprintf("%s-%s-%.2f.gz", timestamp, queryName, minBalance)
}

// compressFile compresses a file using gzip
func (gs *GitStorage) compressFile(srcPath, dstPath string) error {
	// Open source file
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer func() {
		if err := srcFile.Close(); err != nil {
			log.Warn().Err(err).Str("path", srcPath).Msg("Failed to close source file")
		}
	}()

	// Create destination file
	dstFile, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer func() {
		if err := dstFile.Close(); err != nil {
			log.Warn().Err(err).Str("path", dstPath).Msg("Failed to close destination file")
		}
	}()

	// Create gzip writer
	gzipWriter := gzip.NewWriter(dstFile)
	defer func() {
		if err := gzipWriter.Close(); err != nil {
			log.Warn().Err(err).Msg("Failed to close gzip writer")
		}
	}()

	// Copy and compress
	_, err = io.Copy(gzipWriter, srcFile)
	if err != nil {
		return fmt.Errorf("failed to compress file: %w", err)
	}

	return nil
}
