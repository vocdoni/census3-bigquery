package api

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/frankban/quicktest"
	"github.com/google/uuid"
	"github.com/vocdoni/census3-bigquery/storage"
	"github.com/vocdoni/davinci-node/census/censusdb"
	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/db/metadb"
	"github.com/vocdoni/davinci-node/types"
)

// mockStorage implements the SnapshotStorage interface for testing
type mockStorage struct {
	snapshots []storage.KVSnapshot
}

func (m *mockStorage) Snapshots() ([]storage.KVSnapshot, error) {
	return m.snapshots, nil
}

func (m *mockStorage) LatestSnapshot() (*storage.KVSnapshot, error) {
	if len(m.snapshots) == 0 {
		return nil, nil
	}
	// Return the first one (most recent)
	return &m.snapshots[0], nil
}

func (m *mockStorage) SnapshotCount() (int, error) {
	return len(m.snapshots), nil
}

func (m *mockStorage) SnapshotsByBalance(minBalance float64) ([]storage.KVSnapshot, error) {
	var filtered []storage.KVSnapshot
	for _, snapshot := range m.snapshots {
		if snapshot.MinBalance == minBalance {
			filtered = append(filtered, snapshot)
		}
	}
	return filtered, nil
}

func (m *mockStorage) SnapshotsByQuery(queryName string) ([]storage.KVSnapshot, error) {
	var filtered []storage.KVSnapshot
	for _, snapshot := range m.snapshots {
		if snapshot.QueryName == queryName {
			filtered = append(filtered, snapshot)
		}
	}
	return filtered, nil
}

// GetMetadata implements the SnapshotStorage interface for testing
func (m *mockStorage) GetMetadata(metadataType string, censusRoot types.HexBytes) ([]byte, error) {
	// For testing, return nil (no metadata)
	return nil, nil
}

// HasMetadata implements the SnapshotStorage interface for testing
func (m *mockStorage) HasMetadata(metadataType string, censusRoot types.HexBytes) (bool, error) {
	// For testing, return false (no metadata)
	return false, nil
}

// For testing census endpoints, we'll create a real censusDB instance with an in-memory database
func createTestCensusDB(t *testing.T) *censusdb.CensusDB {
	// Create an in-memory database for testing
	database, err := metadb.New(db.TypePebble, t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	return censusdb.NewCensusDB(database)
}

// makeTestAddress creates a 20-byte address for testing from a short byte slice
func makeTestAddress(b []byte) []byte {
	addr := make([]byte, 20)
	copy(addr, b)
	return addr
}

func createTestSnapshots() []storage.KVSnapshot {
	now := time.Now().Truncate(time.Minute)
	return []storage.KVSnapshot{
		{
			SnapshotDate:     now,
			CensusRoot:       types.HexBytes{0x01, 0x02, 0x03},
			ParticipantCount: 100,
			MinBalance:       0.25,
			QueryName:        "ethereum_balances",
		},
		{
			SnapshotDate:     now.Add(-time.Hour),
			CensusRoot:       types.HexBytes{0x04, 0x05, 0x06},
			ParticipantCount: 200,
			MinBalance:       0.5,
			QueryName:        "ethereum_balances",
		},
		{
			SnapshotDate:     now.Add(-2 * time.Hour),
			CensusRoot:       types.HexBytes{0x07, 0x08, 0x09},
			ParticipantCount: 150,
			MinBalance:       0.25,
			QueryName:        "ethereum_balances_recent",
		},
		{
			SnapshotDate:     now.Add(-3 * time.Hour),
			CensusRoot:       types.HexBytes{0x0a, 0x0b, 0x0c},
			ParticipantCount: 300,
			MinBalance:       1.0,
			QueryName:        "ethereum_balances",
		},
	}
}

func TestAPIServerSnapshots(t *testing.T) {
	c := quicktest.New(t)

	// Create mock storage with test data
	mockStore := &mockStorage{
		snapshots: createTestSnapshots(),
	}

	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080, 1000000)

	// Test basic snapshots endpoint
	req := httptest.NewRequest("GET", "/snapshots", nil)
	w := httptest.NewRecorder()
	server.handleSnapshots(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	var response SnapshotsListResponse
	err := json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	// Check response structure
	c.Assert(response.Total, quicktest.Equals, 4)
	c.Assert(response.Page, quicktest.Equals, 1)
	c.Assert(response.PageSize, quicktest.Equals, DefaultPageSize)
	c.Assert(response.HasNext, quicktest.IsFalse)
	c.Assert(response.HasPrev, quicktest.IsFalse)
	c.Assert(len(response.Snapshots), quicktest.Equals, 4)

	// Check that snapshots include all fields
	snapshot := response.Snapshots[0]
	c.Assert(snapshot.SnapshotDate, quicktest.Not(quicktest.Equals), "")
	c.Assert(snapshot.CensusRoot, quicktest.Not(quicktest.Equals), "")
	c.Assert(snapshot.ParticipantCount, quicktest.Equals, 100)
	c.Assert(snapshot.MinBalance, quicktest.Equals, 0.25)
	c.Assert(snapshot.QueryName, quicktest.Equals, "ethereum_balances")
	c.Assert(snapshot.CreatedAt, quicktest.Not(quicktest.Equals), "")
}

func TestAPIServerSnapshotsPagination(t *testing.T) {
	c := quicktest.New(t)

	// Create mock storage with test data
	mockStore := &mockStorage{
		snapshots: createTestSnapshots(),
	}

	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080, 1000000)

	// Test pagination - page 1, pageSize 2
	req := httptest.NewRequest("GET", "/snapshots?page=1&pageSize=2", nil)
	w := httptest.NewRecorder()
	server.handleSnapshots(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	var response SnapshotsListResponse
	err := json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response.Total, quicktest.Equals, 4)
	c.Assert(response.Page, quicktest.Equals, 1)
	c.Assert(response.PageSize, quicktest.Equals, 2)
	c.Assert(response.HasNext, quicktest.IsTrue)
	c.Assert(response.HasPrev, quicktest.IsFalse)
	c.Assert(len(response.Snapshots), quicktest.Equals, 2)

	// Test pagination - page 2, pageSize 2
	req = httptest.NewRequest("GET", "/snapshots?page=2&pageSize=2", nil)
	w = httptest.NewRecorder()
	server.handleSnapshots(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	err = json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response.Total, quicktest.Equals, 4)
	c.Assert(response.Page, quicktest.Equals, 2)
	c.Assert(response.PageSize, quicktest.Equals, 2)
	c.Assert(response.HasNext, quicktest.IsFalse)
	c.Assert(response.HasPrev, quicktest.IsTrue)
	c.Assert(len(response.Snapshots), quicktest.Equals, 2)
}

func TestAPIServerSnapshotsFiltering(t *testing.T) {
	c := quicktest.New(t)

	// Create mock storage with test data
	mockStore := &mockStorage{
		snapshots: createTestSnapshots(),
	}

	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080, 1000000)

	// Test filtering by minBalance
	req := httptest.NewRequest("GET", "/snapshots?minBalance=0.25", nil)
	w := httptest.NewRecorder()
	server.handleSnapshots(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	var response SnapshotsListResponse
	err := json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response.Total, quicktest.Equals, 2) // Two snapshots with 0.25 balance
	c.Assert(len(response.Snapshots), quicktest.Equals, 2)

	// Test filtering by queryName
	req = httptest.NewRequest("GET", "/snapshots?queryName=ethereum_balances", nil)
	w = httptest.NewRecorder()
	server.handleSnapshots(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	err = json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response.Total, quicktest.Equals, 3) // Three snapshots with ethereum_balances query
	c.Assert(len(response.Snapshots), quicktest.Equals, 3)
}

func TestAPIServerLatestSnapshot(t *testing.T) {
	c := quicktest.New(t)

	// Create mock storage with test data
	mockStore := &mockStorage{
		snapshots: createTestSnapshots(),
	}

	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080, 1000000)

	// Test latest snapshot endpoint
	req := httptest.NewRequest("GET", "/snapshots/latest", nil)
	w := httptest.NewRecorder()
	server.handleLatestSnapshot(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	var response SnapshotResponse
	err := json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	// Should return the first snapshot (most recent)
	c.Assert(response.ParticipantCount, quicktest.Equals, 100)
	c.Assert(response.MinBalance, quicktest.Equals, 0.25)
	c.Assert(response.QueryName, quicktest.Equals, "ethereum_balances")
}

func TestAPIServerLatestSnapshotEmpty(t *testing.T) {
	c := quicktest.New(t)

	// Create mock storage with no data
	mockStore := &mockStorage{
		snapshots: []storage.KVSnapshot{},
	}

	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080, 1000000)

	// Test latest snapshot endpoint with no data
	req := httptest.NewRequest("GET", "/snapshots/latest", nil)
	w := httptest.NewRecorder()
	server.handleLatestSnapshot(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusNotFound)
}

func TestAPIServerHealth(t *testing.T) {
	c := quicktest.New(t)

	mockStore := &mockStorage{}
	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080, 1000000)

	// Test health endpoint
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	server.handleHealth(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	var response HealthResponse
	err := json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response.Status, quicktest.Equals, "healthy")
	c.Assert(response.Service, quicktest.Equals, "census3-bigquery")
	c.Assert(response.Timestamp, quicktest.Not(quicktest.Equals), "")
}

func TestAPIServerMethodNotAllowed(t *testing.T) {
	c := quicktest.New(t)

	mockStore := &mockStorage{}
	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080, 1000000)

	// Test POST method on snapshots endpoint using the router
	req := httptest.NewRequest("POST", "/snapshots", nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusMethodNotAllowed)
}

func TestAPIServerInvalidPagination(t *testing.T) {
	c := quicktest.New(t)

	mockStore := &mockStorage{
		snapshots: createTestSnapshots(),
	}

	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080, 1000000)

	// Test with invalid page parameter
	req := httptest.NewRequest("GET", "/snapshots?page=0", nil)
	w := httptest.NewRecorder()
	server.handleSnapshots(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	var response SnapshotsListResponse
	err := json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	// Should default to page 1
	c.Assert(response.Page, quicktest.Equals, 1)

	// Test with pageSize exceeding maximum
	req = httptest.NewRequest("GET", "/snapshots?pageSize=200", nil)
	w = httptest.NewRecorder()
	server.handleSnapshots(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	err = json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	// Should be capped at MaxPageSize
	c.Assert(response.PageSize, quicktest.Equals, MaxPageSize)
}

func TestAPIServerCensusSize(t *testing.T) {
	c := quicktest.New(t)

	mockStore := &mockStorage{}
	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080, 1000000)

	// Create a test census with some data
	censusID := uuid.New()
	workingRef, err := testCensus.New(censusID)
	c.Assert(err, quicktest.IsNil)

	// Add some test participants (key must be 20 bytes for lean-imt)
	testKey := makeTestAddress([]byte{0x01, 0x02, 0x03})
	testValue := []byte{0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b}
	err = workingRef.Insert(testKey, testValue)
	c.Assert(err, quicktest.IsNil)

	// Get the census root
	root := workingRef.Root()

	// Create a root-based census and publish the working census to it
	rootRef, err := testCensus.NewByRoot(root)
	c.Assert(err, quicktest.IsNil)

	// Publish the working census to the root-based census
	err = testCensus.PublishCensus(censusID, rootRef)
	c.Assert(err, quicktest.IsNil)

	rootHex := hex.EncodeToString(root)

	// Test census size endpoint using the router
	req := httptest.NewRequest("GET", "/censuses/"+rootHex+"/size", nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	var response CensusSizeResponse
	err = json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response.Size, quicktest.Equals, 1) // Should have 1 participant

	// Cleanup: close trees to release Pebble locks
	_ = workingRef.Tree().Close()
	_ = rootRef.Tree().Close()
}

func TestAPIServerCensusProof(t *testing.T) {
	c := quicktest.New(t)

	mockStore := &mockStorage{}
	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080, 1000000)

	// Create a test census with some data
	censusID := uuid.New()
	workingRef, err := testCensus.New(censusID)
	c.Assert(err, quicktest.IsNil)

	// Add some test participants (key must be 20 bytes for lean-imt)
	testKey := makeTestAddress([]byte{0x01, 0x02, 0x03})
	testValue := []byte{0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b}
	err = workingRef.Insert(testKey, testValue)
	c.Assert(err, quicktest.IsNil)

	// Get the census root
	root := workingRef.Root()
	t.Logf("Working census root: %x", root)

	// Create a root-based census and transfer data using export/import
	rootRef, err := testCensus.NewByRoot(root)
	c.Assert(err, quicktest.IsNil)

	// Copy the data to the root-based census
	err = testCensus.PublishCensus(censusID, rootRef)
	c.Assert(err, quicktest.IsNil)

	// Verify the root matches
	finalRoot := rootRef.Root()
	t.Logf("Final root: %x", finalRoot)
	c.Assert(bytes.Equal(root, finalRoot), quicktest.IsTrue)

	// Verify the root-based census exists
	exists := testCensus.ExistsByRoot(root)
	t.Logf("Root-based census exists: %v", exists)
	c.Assert(exists, quicktest.IsTrue)

	rootHex := hex.EncodeToString(root)
	keyHex := hex.EncodeToString(testKey)
	t.Logf("Testing proof for root %s and key %s", rootHex, keyHex)

	// Test census proof endpoint using the router
	req := httptest.NewRequest("GET", "/censuses/"+rootHex+"/proof?key="+keyHex, nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Logf("Response body: %s", w.Body.String())
	}

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	var response types.CensusProof
	err = json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	// Verify the proof contains the expected data
	c.Assert(response.Root, quicktest.DeepEquals, types.HexBytes(root))
	c.Assert(response.Address, quicktest.DeepEquals, types.HexBytes(testKey))
	// Note: For lean-imt, the value is packed (address << 88 | weight), not the original value
	c.Assert(response.Weight, quicktest.IsNotNil)

	// Cleanup: close trees to release Pebble locks
	_ = workingRef.Tree().Close()
	_ = rootRef.Tree().Close()
}

func TestAPIServerCensusProofMissingKey(t *testing.T) {
	c := quicktest.New(t)

	mockStore := &mockStorage{}
	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080, 1000000)

	// Test census proof endpoint without key parameter using the router
	req := httptest.NewRequest("GET", "/censuses/010203/proof", nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusBadRequest)

	var response ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response.Code, quicktest.Equals, 40015) // ErrMalformedParam
}

func TestAPIServerCensusParticipants(t *testing.T) {
	c := quicktest.New(t)

	mockStore := &mockStorage{}
	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080, 1000000)

	// Create a test census with multiple participants
	censusID := uuid.New()
	workingRef, err := testCensus.New(censusID)
	c.Assert(err, quicktest.IsNil)

	// Add 2500 test participants to test pagination
	numParticipants := 2500
	for i := 0; i < numParticipants; i++ {
		// Create unique 20-byte addresses
		addr := make([]byte, 20)
		addr[0] = byte(i >> 8)
		addr[1] = byte(i & 0xff)

		// Create weight value
		weight := make([]byte, 32)
		weight[31] = byte(i + 1)

		err = workingRef.Insert(addr, weight)
		c.Assert(err, quicktest.IsNil)
	}

	// Get the census root
	root := workingRef.Root()

	// Create a root-based census and publish
	rootRef, err := testCensus.NewByRoot(root)
	c.Assert(err, quicktest.IsNil)

	err = testCensus.PublishCensus(censusID, rootRef)
	c.Assert(err, quicktest.IsNil)

	rootHex := hex.EncodeToString(root)

	// Test first page (default)
	req := httptest.NewRequest("GET", "/censuses/"+rootHex+"/participants", nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	var response CensusParticipantsResponse
	err = json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	// Check pagination metadata
	c.Assert(response.Total, quicktest.Equals, numParticipants)
	c.Assert(response.Page, quicktest.Equals, 1)
	c.Assert(response.PageSize, quicktest.Equals, 1000) // Fixed page size
	c.Assert(response.HasNext, quicktest.IsTrue)
	c.Assert(response.HasPrev, quicktest.IsFalse)
	c.Assert(len(response.Participants), quicktest.Equals, 1000)

	// Verify first participant
	c.Assert(len(response.Participants[0].Key), quicktest.Equals, 20)
	c.Assert(response.Participants[0].Weight, quicktest.IsNotNil)

	// Test second page
	req = httptest.NewRequest("GET", "/censuses/"+rootHex+"/participants?page=2", nil)
	w = httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	err = json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response.Page, quicktest.Equals, 2)
	c.Assert(response.HasNext, quicktest.IsTrue)
	c.Assert(response.HasPrev, quicktest.IsTrue)
	c.Assert(len(response.Participants), quicktest.Equals, 1000)

	// Test third page (last page with 500 participants)
	req = httptest.NewRequest("GET", "/censuses/"+rootHex+"/participants?page=3", nil)
	w = httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	err = json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response.Page, quicktest.Equals, 3)
	c.Assert(response.HasNext, quicktest.IsFalse)
	c.Assert(response.HasPrev, quicktest.IsTrue)
	c.Assert(len(response.Participants), quicktest.Equals, 500)

	// Test page beyond available data
	req = httptest.NewRequest("GET", "/censuses/"+rootHex+"/participants?page=10", nil)
	w = httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	err = json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response.Page, quicktest.Equals, 10)
	c.Assert(response.HasNext, quicktest.IsFalse)
	c.Assert(response.HasPrev, quicktest.IsTrue)
	c.Assert(len(response.Participants), quicktest.Equals, 0)

	// Cleanup
	_ = workingRef.Tree().Close()
	_ = rootRef.Tree().Close()
}

func TestAPIServerCensusParticipantsInvalidRoot(t *testing.T) {
	c := quicktest.New(t)

	mockStore := &mockStorage{}
	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080, 1000000)

	// Test census participants endpoint with short hex (should return invalid census ID)
	req := httptest.NewRequest("GET", "/censuses/010203/participants", nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusBadRequest)

	var response ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response.Code, quicktest.Equals, 40010) // ErrInvalidCensusID
}

func TestAPIServerCensusParticipantsNotFound(t *testing.T) {
	c := quicktest.New(t)

	mockStore := &mockStorage{}
	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080, 1000000)

	// Test census participants endpoint with non-existent root
	nonExistentRoot := make([]byte, 32)
	for i := range nonExistentRoot {
		nonExistentRoot[i] = 0xff
	}
	rootHex := hex.EncodeToString(nonExistentRoot)

	req := httptest.NewRequest("GET", "/censuses/"+rootHex+"/participants", nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusNotFound)

	var response ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response.Code, quicktest.Equals, 40011) // ErrCensusNotFound
}
