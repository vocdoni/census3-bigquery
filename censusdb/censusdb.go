package censusdb

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vocdoni/arbo"
	"go.vocdoni.io/dvote/db"
	"go.vocdoni.io/dvote/db/prefixeddb"

	"census3-bigquery/log"
)

const (
	censusDBprefix           = "cs_"
	censusDBWorkingOnQueries = "cw_"                   // Prefix for working/temporary censuses during query execution
	censusDBRootPrefix       = "cr_"                   // Prefix for final censuses identified by their root
	CensusTreeMaxLevels      = 160                     // Maximum levels for the Merkle tree
	CensusKeyMaxLen          = CensusTreeMaxLevels / 8 // Maximum length of a key in bytes

	// Batch deletion constants
	maxDeletionBatchSize    = 100 * 1024 * 1024 // 100 MiB
	maxDeletionBatchCount   = 10000             // Max operations per batch
	avgKeyValueSizeEstimate = 64                // Conservative size estimate for overhead
)

var (
	// ErrCensusNotFound is returned when a census is not found in the database.
	ErrCensusNotFound = fmt.Errorf("census not found in the local database")
	// ErrCensusAlreadyExists is returned by New() if the census already exists.
	ErrCensusAlreadyExists = fmt.Errorf("census already exists in the local database")
	// ErrWrongAuthenticationToken is returned when the authentication token is invalid.
	ErrWrongAuthenticationToken = fmt.Errorf("wrong authentication token")
	// ErrCensusIsLocked is returned if the census does not allow write operations.
	ErrCensusIsLocked = fmt.Errorf("census is locked")
	// ErrKeyNotFound is returned when a key is not found in the Merkle tree.
	ErrKeyNotFound = fmt.Errorf("key not found")

	defaultHashFunction = arbo.HashFunctionMiMC_BLS12_377
)

// CensusDB is a safe and persistent database of census trees.
type CensusDB struct {
	mu           sync.RWMutex
	db           db.Database
	loadedCensus map[uuid.UUID]*CensusRef
}

// NewCensusDB creates a new CensusDB object.
func NewCensusDB(db db.Database) *CensusDB {
	return &CensusDB{
		db:           db,
		loadedCensus: make(map[uuid.UUID]*CensusRef),
	}
}

// New creates a new working census with a UUID identifier and adds it to the database.
// It returns ErrCensusAlreadyExists if a census with the given UUID is already present.
func (c *CensusDB) New(censusID uuid.UUID) (*CensusRef, error) {
	return c.newCensus(censusID, censusDBWorkingOnQueries, censusID[:])
}

// NewByRoot creates a new census identified by its root.
// It returns ErrCensusAlreadyExists if a census with the given root is already present.
func (c *CensusDB) NewByRoot(root []byte) (*CensusRef, error) {
	// Generate a deterministic UUID from the root for internal use
	censusID := uuid.NewSHA1(uuid.NameSpaceOID, root)
	return c.newCensus(censusID, censusDBRootPrefix, root)
}

// newCensus is the internal method that creates a new census with the given parameters.
func (c *CensusDB) newCensus(censusID uuid.UUID, prefix string, keyIdentifier []byte) (*CensusRef, error) {
	key := append([]byte(prefix), keyIdentifier...)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check in‑memory.
	if _, exists := c.loadedCensus[censusID]; exists {
		return nil, ErrCensusAlreadyExists
	}

	// Check persistent DB.
	if _, err := c.db.Get(key); err == nil {
		return nil, ErrCensusAlreadyExists
	} else if !errors.Is(err, db.ErrKeyNotFound) {
		return nil, err
	}

	// Prepare a new census reference.
	ref := &CensusRef{
		ID:        censusID,
		MaxLevels: CensusTreeMaxLevels,
		HashType:  string(defaultHashFunction.Type()),
		LastUsed:  time.Now(),
	}

	// Initialize the database for this census.
	ref.db = prefixeddb.NewPrefixedDatabase(c.db, censusPrefix(censusID))

	// Create the Merkle tree.
	tree, err := arbo.NewTree(arbo.Config{
		Database:     ref.db,
		MaxLevels:    CensusTreeMaxLevels,
		HashFunction: defaultHashFunction,
	})
	if err != nil {
		return nil, err
	}
	ref.SetTree(tree)

	// Compute the current root.
	root, err := tree.Root()
	if err != nil {
		return nil, err
	}
	ref.currentRoot = root

	// Store the reference in the database.
	if err := c.writeReferenceWithPrefix(ref, prefix, keyIdentifier); err != nil {
		return nil, err
	}

	// Add to the in‑memory map.
	c.loadedCensus[censusID] = ref

	return ref, nil
}

// writeReferenceWithPrefix writes a census reference to the database with a specific prefix.
func (c *CensusDB) writeReferenceWithPrefix(ref *CensusRef, prefix string, identifier []byte) error {
	key := append([]byte(prefix), identifier...)
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(ref); err != nil {
		return err
	}
	wtx := c.db.WriteTx()
	defer wtx.Discard()
	if err := wtx.Set(key, buf.Bytes()); err != nil {
		return err
	}
	return wtx.Commit()
}

// HashAndTrunkKey computes the hash of a key and truncates it to the required length.
// Returns nil if the hash function fails. Panics if the hash output is too short.
func (c *CensusDB) HashAndTrunkKey(key []byte) []byte {
	hash, err := defaultHashFunction.Hash(key)
	if err != nil {
		return nil
	}
	if len(hash) < CensusKeyMaxLen {
		panic("hash function output is too short, maxlevels is too high")
	}
	return hash[:CensusKeyMaxLen]
}

// HashLen returns the length of the hash function output in bytes.
func (c *CensusDB) HashLen() int {
	return defaultHashFunction.Len()
}

// Exists returns true if the censusID exists in the local database.
func (c *CensusDB) Exists(censusID uuid.UUID) bool {
	c.mu.RLock()
	_, exists := c.loadedCensus[censusID]
	c.mu.RUnlock()
	if exists {
		return true
	}
	key := append([]byte(censusDBWorkingOnQueries), censusID[:]...)
	_, err := c.db.Get(key)
	return err == nil
}

// ExistsByRoot returns true if a census with the given root exists in the local database.
func (c *CensusDB) ExistsByRoot(root []byte) bool {
	censusID := uuid.NewSHA1(uuid.NameSpaceOID, root)
	c.mu.RLock()
	_, exists := c.loadedCensus[censusID]
	c.mu.RUnlock()
	if exists {
		return true
	}
	key := append([]byte(censusDBRootPrefix), root...)
	_, err := c.db.Get(key)
	return err == nil
}

// Load returns a census from memory or from the persistent KV database.
func (c *CensusDB) Load(censusID uuid.UUID) (*CensusRef, error) {
	ref, err := c.loadCensusRef(censusID)
	if err != nil {
		return nil, err
	}
	return ref, nil
}

// LoadByRoot loads a census by its root from memory or from the persistent KV database.
func (c *CensusDB) LoadByRoot(root []byte) (*CensusRef, error) {
	censusID := uuid.NewSHA1(uuid.NameSpaceOID, root)
	return c.loadCensusRefByRoot(censusID, root)
}

// loadCensusRef loads a census reference from memory or persistent DB using a double‑check.
func (c *CensusDB) loadCensusRef(censusID uuid.UUID) (*CensusRef, error) {
	c.mu.RLock()
	if ref, exists := c.loadedCensus[censusID]; exists {
		c.mu.RUnlock()
		return ref, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	key := append([]byte(censusDBWorkingOnQueries), censusID[:]...)
	b, err := c.db.Get(key)
	if err != nil {
		if errors.Is(err, db.ErrKeyNotFound) {
			return nil, fmt.Errorf("%w: %x", ErrCensusNotFound, censusID)
		}
		return nil, err
	}

	var ref CensusRef
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&ref); err != nil {
		return nil, err
	}

	ref.db = prefixeddb.NewPrefixedDatabase(c.db, censusPrefix(censusID))
	tree, err := arbo.NewTree(arbo.Config{
		Database:     ref.db,
		MaxLevels:    ref.MaxLevels,
		HashFunction: defaultHashFunction,
	})
	if err != nil {
		return nil, err
	}

	ref.tree = tree
	root, err := tree.Root()
	if err != nil {
		return nil, err
	}
	ref.currentRoot = root

	// Update the LastUsed timestamp and write back to the database.
	ref.LastUsed = time.Now()
	if err := c.writeReferenceWithPrefix(&ref, censusDBWorkingOnQueries, censusID[:]); err != nil {
		return nil, err
	}

	c.loadedCensus[censusID] = &ref
	return &ref, nil
}

// loadCensusRefByRoot loads a census reference by root from memory or persistent DB.
func (c *CensusDB) loadCensusRefByRoot(censusID uuid.UUID, root []byte) (*CensusRef, error) {
	c.mu.RLock()
	if ref, exists := c.loadedCensus[censusID]; exists {
		c.mu.RUnlock()
		return ref, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	key := append([]byte(censusDBRootPrefix), root...)
	b, err := c.db.Get(key)
	if err != nil {
		if errors.Is(err, db.ErrKeyNotFound) {
			return nil, fmt.Errorf("%w: %x", ErrCensusNotFound, root)
		}
		return nil, err
	}

	var ref CensusRef
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&ref); err != nil {
		return nil, err
	}

	ref.db = prefixeddb.NewPrefixedDatabase(c.db, censusPrefix(censusID))
	tree, err := arbo.NewTree(arbo.Config{
		Database:     ref.db,
		MaxLevels:    ref.MaxLevels,
		HashFunction: defaultHashFunction,
	})
	if err != nil {
		return nil, err
	}

	ref.tree = tree
	treeRoot, err := tree.Root()
	if err != nil {
		return nil, err
	}
	ref.currentRoot = treeRoot

	// Update the LastUsed timestamp and write back to the database.
	ref.LastUsed = time.Now()
	if err := c.writeReferenceWithPrefix(&ref, censusDBRootPrefix, root); err != nil {
		return nil, err
	}

	c.loadedCensus[censusID] = &ref
	return &ref, nil
}

// Del removes a census from the database and memory.
func (c *CensusDB) Del(censusID uuid.UUID) error {
	key := append([]byte(censusDBWorkingOnQueries), censusID[:]...)
	wtx := c.db.WriteTx()
	if err := wtx.Delete(key); err != nil {
		wtx.Discard()
		return err
	}
	if err := wtx.Commit(); err != nil {
		return err
	}

	c.mu.Lock()
	delete(c.loadedCensus, censusID)
	c.mu.Unlock()

	go func(id uuid.UUID) {
		if _, err := deleteCensusTreeFromDatabase(c.db, censusPrefix(id)); err != nil {
			log.Warn().
				Str("id", fmt.Sprintf("%x", id)).
				Err(err).
				Msg("Error deleting census tree")
		}
	}(censusID)

	return nil
}

// CleanupWorkingCensus removes a working census from the database and memory.
// This is used to clean up temporary censuses after they have been converted to root-based ones.
func (c *CensusDB) CleanupWorkingCensus(censusID uuid.UUID) error {
	startTime := time.Now()

	key := append([]byte(censusDBWorkingOnQueries), censusID[:]...)
	wtx := c.db.WriteTx()
	if err := wtx.Delete(key); err != nil {
		wtx.Discard()
		return err
	}
	if err := wtx.Commit(); err != nil {
		return err
	}

	c.mu.Lock()
	delete(c.loadedCensus, censusID)
	c.mu.Unlock()

	// Delete the census tree data
	count, err := deleteCensusTreeFromDatabase(c.db, censusPrefix(censusID))
	duration := time.Since(startTime)

	log.Info().
		Str("census_id", fmt.Sprintf("%x", censusID)).
		Int("keys_deleted", count).
		Str("duration", duration.String()).
		Msg("Working census cleanup completed")

	return err
}

// deleteCensusTreeFromDatabase removes all keys belonging to a census tree from the database.
func deleteCensusTreeFromDatabase(kv db.Database, prefix []byte) (int, error) {
	database := prefixeddb.NewPrefixedDatabase(kv, prefix)

	var (
		wtx              = database.WriteTx()
		count            = 0
		batchCount       = 0
		currentBatchSize = 0
	)

	defer func() {
		if wtx != nil {
			wtx.Discard()
		}
	}()

	err := database.Iterate(nil, func(k, v []byte) bool {
		if err := wtx.Delete(k); err != nil {
			log.Warn().
				Str("key", fmt.Sprintf("%x", k)).
				Err(err).
				Msg("Could not remove key from database")
			return true
		}

		count++
		batchCount++
		currentBatchSize += len(k) + len(v) + avgKeyValueSizeEstimate

		// Check if we should commit this batch
		if currentBatchSize >= maxDeletionBatchSize || batchCount >= maxDeletionBatchCount {
			if err := wtx.Commit(); err != nil {
				log.Error().Err(err).Msg("Failed to commit deletion batch")
				return false
			}

			// Start new batch
			wtx = database.WriteTx()
			batchCount = 0
			currentBatchSize = 0
		}

		return true
	})

	if err != nil {
		return count, err
	}

	// Commit final batch if there are pending deletions
	if batchCount > 0 {
		if err := wtx.Commit(); err != nil {
			return count, err
		}
		wtx = nil
	}

	return count, nil
}

func (c *CensusDB) PublishCensus(originCensusID uuid.UUID, destinationRef *CensusRef) error {
	ref, err := c.Load(originCensusID)
	if err != nil {
		return err
	}

	ref.treeMu.Lock()
	defer ref.treeMu.Unlock()

	return ref.tree.CloneAndVacuum(destinationRef.db, nil)
}

// VerifyProof checks the validity of a Merkle proof.
func (c *CensusDB) VerifyProof(proof *CensusProof) bool {
	if proof == nil {
		return false
	}
	// if weight is available, check it
	if proof.Weight != nil {
		if proof.Weight.MathBigInt().Cmp(arbo.BytesToBigInt(proof.Value)) != 0 {
			return false
		}
	}
	return VerifyProof(proof.Key, proof.Value, proof.Root, proof.Siblings)
}

// ProofByRoot generates a Merkle proof for the given leafKey in a census identified by its root.
func (c *CensusDB) ProofByRoot(root, leafKey []byte) (*CensusProof, error) {
	ref, err := c.LoadByRoot(root)
	if err != nil {
		return nil, err
	}

	key, value, siblings, inclusion, err := ref.GenProof(leafKey)
	if err != nil {
		return nil, err
	}
	if !inclusion {
		return nil, ErrKeyNotFound
	}

	return &CensusProof{
		Root:     root,
		Key:      key,
		Value:    value,
		Siblings: siblings,
		Weight:   (*BigInt)(arbo.BytesToBigInt(value)),
	}, nil
}

// SizeByRoot returns the number of leaves in the Merkle tree with the given root.
func (c *CensusDB) SizeByRoot(root []byte) (int, error) {
	ref, err := c.LoadByRoot(root)
	if err != nil {
		return 0, err
	}
	return ref.Size(), nil
}

// PurgeWorkingCensuses removes all working censuses older than the specified duration.
func (c *CensusDB) PurgeWorkingCensuses(maxAge time.Duration) (int, error) {
	cutoffTime := time.Now().Add(-maxAge)
	var keysToDelete [][]byte
	var censusIDsToDelete []uuid.UUID

	err := c.db.Iterate([]byte(censusDBWorkingOnQueries), func(key, value []byte) bool {
		var ref CensusRef
		if err := gob.NewDecoder(bytes.NewReader(value)).Decode(&ref); err != nil {
			return true // Continue iteration
		}

		// Delete censuses that are older than the cutoff time
		if ref.LastUsed.Before(cutoffTime) {
			keysToDelete = append(keysToDelete, append([]byte(censusDBWorkingOnQueries), key...))
			censusIDsToDelete = append(censusIDsToDelete, ref.ID)
		}
		return true
	})

	if err != nil {
		return 0, fmt.Errorf("failed to iterate for purge: %w", err)
	}

	if len(keysToDelete) == 0 {
		return 0, nil
	}

	// Delete in transaction
	wtx := c.db.WriteTx()
	defer func() {
		if wtx != nil {
			wtx.Discard()
		}
	}()

	for _, key := range keysToDelete {
		if err := wtx.Delete(key); err != nil {
			return 0, fmt.Errorf("failed to delete working census: %w", err)
		}
	}

	if err := wtx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit purge: %w", err)
	}
	wtx = nil // Prevent discard from being called

	// Remove from memory and clean up tree data
	c.mu.Lock()
	for _, censusID := range censusIDsToDelete {
		delete(c.loadedCensus, censusID)
	}
	c.mu.Unlock()

	// Clean up tree data synchronously to avoid race conditions in tests
	for _, censusID := range censusIDsToDelete {
		if _, err := deleteCensusTreeFromDatabase(c.db, censusPrefix(censusID)); err != nil {
			log.Warn().
				Str("id", fmt.Sprintf("%x", censusID)).
				Err(err).
				Msg("Error deleting purged census tree")
		}
	}

	log.Info().
		Int("purged_count", len(keysToDelete)).
		Str("max_age", maxAge.String()).
		Msg("Purged old working censuses")

	return len(keysToDelete), nil
}

// censusPrefix returns the prefix used for the census tree in the database.
func censusPrefix(censusID uuid.UUID) []byte {
	return append([]byte(censusDBprefix), censusID[:]...)
}
