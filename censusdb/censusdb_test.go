package censusdb

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/google/uuid"
	"go.vocdoni.io/dvote/db"
	"go.vocdoni.io/dvote/db/metadb"
)

// newDatabase returns a new in-memory test database.
func newDatabase(t *testing.T) db.Database {
	return metadb.NewTest(t)
}

func TestNewCensusDB(t *testing.T) {
	t.Parallel()
	db := newDatabase(t)
	censusDB := NewCensusDB(db)
	qt.Assert(t, censusDB, qt.IsNotNil)
	qt.Assert(t, censusDB.db, qt.IsNotNil)
}

func TestCensusDBNew(t *testing.T) {
	t.Parallel()
	censusDB := NewCensusDB(newDatabase(t))
	censusID := uuid.New()

	censusRef, err := censusDB.New(censusID)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, censusRef, qt.IsNotNil)
	qt.Assert(t, censusRef.Tree(), qt.IsNotNil)
}

func TestCensusDBNewByRoot(t *testing.T) {
	t.Parallel()
	censusDB := NewCensusDB(newDatabase(t))
	root := []byte("test_root_12345678901234567890")

	censusRef, err := censusDB.NewByRoot(root)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, censusRef, qt.IsNotNil)
	qt.Assert(t, censusRef.Tree(), qt.IsNotNil)
}

func TestCensusDBExists(t *testing.T) {
	t.Parallel()
	censusDB := NewCensusDB(newDatabase(t))
	censusID := uuid.New()

	// Before creation.
	existsBefore := censusDB.Exists(censusID)
	qt.Assert(t, existsBefore, qt.IsFalse)

	// Create a new census.
	_, err := censusDB.New(censusID)
	qt.Assert(t, err, qt.IsNil)

	existsAfter := censusDB.Exists(censusID)
	qt.Assert(t, existsAfter, qt.IsTrue)
}

func TestCensusDBExistsByRoot(t *testing.T) {
	t.Parallel()
	censusDB := NewCensusDB(newDatabase(t))
	root := []byte("test_root_12345678901234567890")

	// Before creation.
	existsBefore := censusDB.ExistsByRoot(root)
	qt.Assert(t, existsBefore, qt.IsFalse)

	// Create a new census.
	_, err := censusDB.NewByRoot(root)
	qt.Assert(t, err, qt.IsNil)

	existsAfter := censusDB.ExistsByRoot(root)
	qt.Assert(t, existsAfter, qt.IsTrue)
}

func TestCensusDBDel(t *testing.T) {
	t.Parallel()
	censusDB := NewCensusDB(newDatabase(t))
	censusID := uuid.New()

	// Create a census for deletion.
	_, err := censusDB.New(censusID)
	qt.Assert(t, err, qt.IsNil)

	// Delete the census.
	err = censusDB.Del(censusID)
	qt.Assert(t, err, qt.IsNil)

	// Wait a bit since the deletion of the underlying tree is asynchronous.
	time.Sleep(100 * time.Millisecond)

	// Check that the census is no longer accessible.
	existsAfter := censusDB.Exists(censusID)
	qt.Assert(t, existsAfter, qt.IsFalse)
}

func TestSequentialLoadReturnsSamePointer(t *testing.T) {
	t.Parallel()
	censusDB := NewCensusDB(newDatabase(t))
	censusID := uuid.New()

	ref1, err := censusDB.New(censusID)
	qt.Assert(t, err, qt.IsNil)

	ref2, err := censusDB.Load(censusID)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, ref1, qt.Equals, ref2)
}

func TestLoadNonExistingCensus(t *testing.T) {
	t.Parallel()
	censusDB := NewCensusDB(newDatabase(t))
	censusID := uuid.New() // Not created.

	ref, err := censusDB.Load(censusID)
	qt.Assert(t, ref, qt.IsNil)
	qt.Assert(t, err, qt.Not(qt.IsNil))
	qt.Assert(t, err.Error(), qt.Contains, "census not found")
}

func TestLoadNonExistingCensusByRoot(t *testing.T) {
	t.Parallel()
	censusDB := NewCensusDB(newDatabase(t))
	root := []byte("nonexistent_root_123456789012")

	ref, err := censusDB.LoadByRoot(root)
	qt.Assert(t, ref, qt.IsNil)
	qt.Assert(t, err, qt.Not(qt.IsNil))
	qt.Assert(t, err.Error(), qt.Contains, "census not found")
}

func TestPersistenceAcrossCensusDBInstances(t *testing.T) {
	t.Parallel()
	db := newDatabase(t)
	censusID := uuid.New()

	censusDB1 := NewCensusDB(db)
	ref1, err := censusDB1.New(censusID)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, ref1, qt.IsNotNil)

	// Create a new CensusDB instance sharing the same underlying database.
	censusDB2 := NewCensusDB(db)
	ref2, err := censusDB2.Load(censusID)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, ref2, qt.IsNotNil)
	qt.Assert(t, ref2.Tree(), qt.IsNotNil)
}

func TestLoadAfterDelete(t *testing.T) {
	t.Parallel()
	censusDB := NewCensusDB(newDatabase(t))
	censusID := uuid.New()

	_, err := censusDB.New(censusID)
	qt.Assert(t, err, qt.IsNil)

	err = censusDB.Del(censusID)
	qt.Assert(t, err, qt.IsNil)

	// Allow async deletion to complete.
	time.Sleep(100 * time.Millisecond)

	ref, err := censusDB.Load(censusID)
	qt.Assert(t, ref, qt.IsNil)
	qt.Assert(t, err, qt.Not(qt.IsNil))
	qt.Assert(t, err.Error(), qt.Contains, "census not found")
}

func TestCensusDBConcurrentLoad(t *testing.T) {
	censusDB := NewCensusDB(newDatabase(t))
	censusID := uuid.New()

	// Create the census.
	ref, err := censusDB.New(censusID)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, ref, qt.IsNotNil)

	const numGoroutines = 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Channels to collect results.
	errs := make(chan error, numGoroutines)
	refs := make(chan *CensusRef, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			r, err := censusDB.Load(censusID)
			if err != nil {
				errs <- err
			} else {
				refs <- r
			}
		}()
	}
	wg.Wait()
	close(errs)
	close(refs)

	for err := range errs {
		qt.Assert(t, err, qt.IsNil)
	}

	var firstRef *CensusRef
	for r := range refs {
		if firstRef == nil {
			firstRef = r
		} else {
			qt.Assert(t, r, qt.Equals, firstRef)
		}
	}
}

func TestCensusDBConcurrentNew(t *testing.T) {
	censusDB := NewCensusDB(newDatabase(t))
	censusID := uuid.New()
	const numGoroutines = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	var successCount int32
	var failureCount int32

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			ref, err := censusDB.New(censusID)
			if err == nil && ref != nil {
				atomic.AddInt32(&successCount, 1)
			} else if err != nil {
				// Only ErrCensusAlreadyExists is expected after one success.
				if err == ErrCensusAlreadyExists {
					atomic.AddInt32(&failureCount, 1)
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	qt.Assert(t, successCount, qt.Equals, int32(1))
	qt.Assert(t, failureCount, qt.Equals, int32(numGoroutines-1))
}

func TestConcurrentExists(t *testing.T) {
	censusDB := NewCensusDB(newDatabase(t))
	censusID := uuid.New()
	const numGoroutines = 20

	var wg sync.WaitGroup

	// Concurrently check Exists before the census is created.
	wg.Add(numGoroutines)
	existsBefore := make(chan bool, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			existsBefore <- censusDB.Exists(censusID)
		}()
	}
	wg.Wait()
	close(existsBefore)
	for exists := range existsBefore {
		qt.Assert(t, exists, qt.IsFalse)
	}

	// Create the census.
	_, err := censusDB.New(censusID)
	qt.Assert(t, err, qt.IsNil)

	// Concurrently check Exists after creation.
	wg.Add(numGoroutines)
	existsAfter := make(chan bool, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			existsAfter <- censusDB.Exists(censusID)
		}()
	}
	wg.Wait()
	close(existsAfter)
	for exists := range existsAfter {
		qt.Assert(t, exists, qt.IsTrue)
	}
}

func TestMultipleCensuses(t *testing.T) {
	censusDB := NewCensusDB(newDatabase(t))
	const numCensuses = 20
	var wg sync.WaitGroup
	censusIDs := make([]uuid.UUID, numCensuses)

	// Concurrently create several censuses.
	wg.Add(numCensuses)
	for i := 0; i < numCensuses; i++ {
		censusIDs[i] = uuid.New()
		go func(id uuid.UUID) {
			defer wg.Done()
			ref, err := censusDB.New(id)
			qt.Assert(t, err, qt.IsNil)
			qt.Assert(t, ref, qt.IsNotNil)
		}(censusIDs[i])
	}
	wg.Wait()

	// Concurrently load each census.
	wg.Add(numCensuses)
	for i := 0; i < numCensuses; i++ {
		go func(id uuid.UUID) {
			defer wg.Done()
			ref, err := censusDB.Load(id)
			qt.Assert(t, err, qt.IsNil)
			qt.Assert(t, ref, qt.IsNotNil)
		}(censusIDs[i])
	}
	wg.Wait()
}

func TestCleanupWorkingCensus(t *testing.T) {
	t.Parallel()
	censusDB := NewCensusDB(newDatabase(t))
	censusID := uuid.New()

	// Create a working census
	_, err := censusDB.New(censusID)
	qt.Assert(t, err, qt.IsNil)

	// Verify it exists
	exists := censusDB.Exists(censusID)
	qt.Assert(t, exists, qt.IsTrue)

	// Clean it up
	err = censusDB.CleanupWorkingCensus(censusID)
	qt.Assert(t, err, qt.IsNil)

	// Verify it no longer exists
	exists = censusDB.Exists(censusID)
	qt.Assert(t, exists, qt.IsFalse)
}

func TestBasicProofGeneration(t *testing.T) {
	t.Parallel()
	censusDB := NewCensusDB(newDatabase(t))

	// Create a working census first
	censusID := uuid.New()
	ref, err := censusDB.New(censusID)
	qt.Assert(t, err, qt.IsNil)

	// Insert a key/value pair directly
	leafKey := []byte("myKey")
	value := []byte("myValue")
	err = ref.Insert(leafKey, value)
	qt.Assert(t, err, qt.IsNil)

	// Get the actual root after insertion
	actualRoot := ref.Root()
	qt.Assert(t, actualRoot, qt.Not(qt.IsNil))

	// Create a root-based census with the actual root
	rootRef, err := censusDB.NewByRoot(actualRoot)
	qt.Assert(t, err, qt.IsNil)

	// Insert the same data to the root-based census
	err = rootRef.Insert(leafKey, value)
	qt.Assert(t, err, qt.IsNil)

	// Test SizeByRoot
	size, err := censusDB.SizeByRoot(actualRoot)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, size, qt.Equals, 1)

	// Test ProofByRoot
	proof, err := censusDB.ProofByRoot(actualRoot, leafKey)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, proof, qt.Not(qt.IsNil))
	qt.Assert(t, string(proof.Key), qt.DeepEquals, string(leafKey))
	qt.Assert(t, string(proof.Value), qt.DeepEquals, string(value))

	// Verify the proof
	ok := censusDB.VerifyProof(proof)
	qt.Assert(t, ok, qt.IsTrue)
}

func TestPurgeWorkingCensusesBasic(t *testing.T) {
	t.Parallel()
	censusDB := NewCensusDB(newDatabase(t))

	// Create a few working censuses
	var censusIDs []uuid.UUID
	for i := 0; i < 3; i++ {
		censusID := uuid.New()
		censusIDs = append(censusIDs, censusID)
		_, err := censusDB.New(censusID)
		qt.Assert(t, err, qt.IsNil)
	}

	// Verify they all exist
	for _, censusID := range censusIDs {
		exists := censusDB.Exists(censusID)
		qt.Assert(t, exists, qt.IsTrue)
	}

	// Purge all working censuses (without goroutines to avoid race conditions)
	purged, err := censusDB.PurgeWorkingCensuses(1 * time.Nanosecond)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, purged, qt.Equals, 3)

	// Verify they no longer exist
	for _, censusID := range censusIDs {
		exists := censusDB.Exists(censusID)
		qt.Assert(t, exists, qt.IsFalse)
	}
}
