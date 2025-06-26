package api

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/frankban/quicktest"
	"github.com/google/uuid"
	"github.com/vocdoni/davinci-node/types"
	"go.vocdoni.io/dvote/db"
	"go.vocdoni.io/dvote/db/metadb"

	"census3-bigquery/censusdb"
	"census3-bigquery/storage"
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

// For testing census endpoints, we'll create a real censusDB instance with an in-memory database
func createTestCensusDB(t *testing.T) *censusdb.CensusDB {
	// Create an in-memory database for testing
	database, err := metadb.New(db.TypePebble, t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	return censusdb.NewCensusDB(database)
}

func createTestSnapshots() []storage.KVSnapshot {
	now := time.Now().Truncate(time.Minute)
	return []storage.KVSnapshot{
		{
			SnapshotDate:     now,
			CensusRoot:       types.HexBytes{0x01, 0x02, 0x03},
			ParticipantCount: 100,
			CreatedAt:        now,
			MinBalance:       0.25,
			QueryName:        "ethereum_balances",
		},
		{
			SnapshotDate:     now.Add(-time.Hour),
			CensusRoot:       types.HexBytes{0x04, 0x05, 0x06},
			ParticipantCount: 200,
			CreatedAt:        now.Add(-time.Hour),
			MinBalance:       0.5,
			QueryName:        "ethereum_balances",
		},
		{
			SnapshotDate:     now.Add(-2 * time.Hour),
			CensusRoot:       types.HexBytes{0x07, 0x08, 0x09},
			ParticipantCount: 150,
			CreatedAt:        now.Add(-2 * time.Hour),
			MinBalance:       0.25,
			QueryName:        "ethereum_balances_recent",
		},
		{
			SnapshotDate:     now.Add(-3 * time.Hour),
			CensusRoot:       types.HexBytes{0x0a, 0x0b, 0x0c},
			ParticipantCount: 300,
			CreatedAt:        now.Add(-3 * time.Hour),
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
	server := NewServer(mockStore, testCensus, 8080)

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
	server := NewServer(mockStore, testCensus, 8080)

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
	server := NewServer(mockStore, testCensus, 8080)

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
	server := NewServer(mockStore, testCensus, 8080)

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
	server := NewServer(mockStore, testCensus, 8080)

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
	server := NewServer(mockStore, testCensus, 8080)

	// Test health endpoint
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	server.handleHealth(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	var response map[string]interface{}
	err := json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response["status"], quicktest.Equals, "healthy")
	c.Assert(response["service"], quicktest.Equals, "census3-bigquery")
	c.Assert(response["timestamp"], quicktest.Not(quicktest.IsNil))
}

func TestAPIServerMethodNotAllowed(t *testing.T) {
	c := quicktest.New(t)

	mockStore := &mockStorage{}
	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080)

	// Test POST method on snapshots endpoint
	req := httptest.NewRequest("POST", "/snapshots", nil)
	w := httptest.NewRecorder()
	server.handleSnapshots(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusMethodNotAllowed)
}

func TestAPIServerInvalidPagination(t *testing.T) {
	c := quicktest.New(t)

	mockStore := &mockStorage{
		snapshots: createTestSnapshots(),
	}

	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080)

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
	server := NewServer(mockStore, testCensus, 8080)

	// Create a test census with some data
	censusID := uuid.New()
	workingRef, err := testCensus.New(censusID)
	c.Assert(err, quicktest.IsNil)

	// Add some test participants
	testKey := []byte{0x01, 0x02, 0x03}
	testValue := []byte{0x04, 0x05, 0x06}
	err = workingRef.Insert(testKey, testValue)
	c.Assert(err, quicktest.IsNil)

	// Get the census root
	root := workingRef.Root()

	// Create a root-based census and transfer data
	rootRef, err := testCensus.NewByRoot(root)
	c.Assert(err, quicktest.IsNil)

	// Insert the same data to the root-based census
	err = rootRef.Insert(testKey, testValue)
	c.Assert(err, quicktest.IsNil)

	rootHex := hex.EncodeToString(root)

	// Test census size endpoint using the router
	req := httptest.NewRequest("GET", "/censuses/"+rootHex+"/size", nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	var response map[string]interface{}
	err = json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response["size"], quicktest.Equals, float64(1)) // Should have 1 participant
}

func TestAPIServerCensusProof(t *testing.T) {
	c := quicktest.New(t)

	mockStore := &mockStorage{}
	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080)

	// Create a test census with some data
	censusID := uuid.New()
	workingRef, err := testCensus.New(censusID)
	c.Assert(err, quicktest.IsNil)

	// Add some test participants
	testKey := []byte{0x01, 0x02, 0x03}
	testValue := []byte{0x04, 0x05, 0x06}
	err = workingRef.Insert(testKey, testValue)
	c.Assert(err, quicktest.IsNil)

	// Get the census root
	root := workingRef.Root()

	// Create a root-based census and transfer data
	rootRef, err := testCensus.NewByRoot(root)
	c.Assert(err, quicktest.IsNil)

	// Insert the same data to the root-based census
	err = rootRef.Insert(testKey, testValue)
	c.Assert(err, quicktest.IsNil)

	rootHex := hex.EncodeToString(root)
	keyHex := hex.EncodeToString(testKey)

	// Test census proof endpoint using the router
	req := httptest.NewRequest("GET", "/censuses/"+rootHex+"/proof?key="+keyHex, nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusOK)

	var response censusdb.CensusProof
	err = json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	// Verify the proof contains the expected data
	c.Assert(response.Root, quicktest.DeepEquals, censusdb.HexBytes(root))
	c.Assert(response.Key, quicktest.DeepEquals, censusdb.HexBytes(testKey))
	c.Assert(response.Value, quicktest.DeepEquals, censusdb.HexBytes(testValue))
}

func TestAPIServerCensusProofMissingKey(t *testing.T) {
	c := quicktest.New(t)

	mockStore := &mockStorage{}
	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080)

	// Test census proof endpoint without key parameter using the router
	req := httptest.NewRequest("GET", "/censuses/010203/proof", nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusBadRequest)

	var response map[string]interface{}
	err := json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response["code"], quicktest.Equals, float64(40015)) // ErrMalformedParam
}

func TestAPIServerCensusParticipants(t *testing.T) {
	c := quicktest.New(t)

	mockStore := &mockStorage{}
	testCensus := createTestCensusDB(t)
	server := NewServer(mockStore, testCensus, 8080)

	// Test census participants endpoint using the router (should return not implemented for now)
	req := httptest.NewRequest("GET", "/censuses/010203/participants", nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	c.Assert(w.Code, quicktest.Equals, http.StatusInternalServerError)

	var response map[string]interface{}
	err := json.NewDecoder(w.Body).Decode(&response)
	c.Assert(err, quicktest.IsNil)

	c.Assert(response["code"], quicktest.Equals, float64(50002)) // ErrGenericInternalServerError
}
