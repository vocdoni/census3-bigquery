package api

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/vocdoni/arbo"
	"github.com/vocdoni/davinci-node/types"

	"census3-bigquery/censusdb"
	"census3-bigquery/log"
	"census3-bigquery/storage"
)

// SnapshotStorage interface for storage operations
type SnapshotStorage interface {
	Snapshots() ([]storage.KVSnapshot, error)
	LatestSnapshot() (*storage.KVSnapshot, error)
	SnapshotCount() (int, error)
	SnapshotsByBalance(minBalance float64) ([]storage.KVSnapshot, error)
	SnapshotsByQuery(queryName string) ([]storage.KVSnapshot, error)
}

// Server handles HTTP API requests
type Server struct {
	storage       SnapshotStorage
	censusDB      *censusdb.CensusDB
	port          int
	maxCensusSize int
	router        chi.Router
}

// CensusParticipant represents a participant in the census API response
type CensusParticipant struct {
	Key    types.HexBytes `json:"key"`
	Weight *types.BigInt  `json:"weight,omitempty"`
}

// CensusParticipantsRequest represents the request to add participants to a census
type CensusParticipantsRequest struct {
	Participants []CensusParticipant `json:"participants"`
}

// CensusParticipantsResponse represents the paginated participants response
type CensusParticipantsResponse struct {
	Participants []CensusParticipant `json:"participants"`
	Total        int                 `json:"total"`
	Page         int                 `json:"page"`
	PageSize     int                 `json:"pageSize"`
	HasNext      bool                `json:"hasNext"`
	HasPrev      bool                `json:"hasPrev"`
}

// NewCensusResponse represents the response when creating a new census
type NewCensusResponse struct {
	Census string `json:"census"` // UUID string
}

// PublishCensusResponse represents the response when publishing a census
type PublishCensusResponse struct {
	Root             types.HexBytes `json:"root"`
	ParticipantCount int            `json:"participantCount"`
	CreatedAt        string         `json:"createdAt"`
	PublishedAt      string         `json:"publishedAt"`
}

// SnapshotResponse represents the API response for snapshots
type SnapshotResponse struct {
	SnapshotDate     string  `json:"snapshotDate"`
	CensusRoot       string  `json:"censusRoot"`
	ParticipantCount int     `json:"participantCount"`
	MinBalance       float64 `json:"minBalance"`
	QueryName        string  `json:"queryName"`
	CreatedAt        string  `json:"createdAt"`
}

// SnapshotsListResponse represents the full response for the snapshots endpoint
type SnapshotsListResponse struct {
	Snapshots []SnapshotResponse `json:"snapshots"`
	Total     int                `json:"total"`
	Page      int                `json:"page"`
	PageSize  int                `json:"pageSize"`
	HasNext   bool               `json:"hasNext"`
	HasPrev   bool               `json:"hasPrev"`
}

// PaginationParams holds pagination parameters
type PaginationParams struct {
	Page     int
	PageSize int
	Offset   int
}

// Default pagination values
const (
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// NewServer creates a new API server
func NewServer(storage SnapshotStorage, censusDB *censusdb.CensusDB, port int, maxCensusSize int) *Server {
	s := &Server{
		storage:       storage,
		censusDB:      censusDB,
		port:          port,
		maxCensusSize: maxCensusSize,
	}
	s.setupRouter()
	return s
}

// setupRouter initializes the router with all endpoints
func (s *Server) setupRouter() {
	r := chi.NewRouter()

	// Add logging middleware
	r.Use(middleware.Logger)

	// Add CORS middleware
	r.Use(s.corsMiddleware)

	// Existing snapshot endpoints
	r.Get("/snapshots", s.handleSnapshots)
	r.Get("/snapshots/latest", s.handleLatestSnapshot)
	r.Get("/health", s.handleHealth)

	// Census CRUD endpoints
	r.Post("/censuses", s.handleCreateCensus)
	r.Post("/censuses/{censusId}/participants", s.handleAddParticipants)
	r.Get("/censuses/{censusId}/participants", s.handleGetParticipants)
	r.Get("/censuses/{censusId}/root", s.handleGetRoot)
	r.Post("/censuses/{censusId}/publish", s.handlePublishCensus)
	r.Delete("/censuses/{censusId}", s.handleDeleteCensus)

	// Census query endpoints (support both UUID and root)
	r.Get("/censuses/{censusId}/size", s.handleCensusSize)
	r.Get("/censuses/{censusRoot}/proof", s.handleCensusProof)
	r.Get("/censuses/{censusRoot}/participants", s.handleCensusParticipants)

	s.router = r
}

// Start starts the HTTP server
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	log.Info().Str("address", addr).Msg("HTTP server binding to address...")

	// Log when server is ready to accept connections
	go func() {
		// Small delay to ensure the server has started
		time.Sleep(50 * time.Millisecond)
		log.Info().
			Str("address", addr).
			Msg("API server ready to accept requests")
	}()

	log.Info().Str("address", addr).Msg("Starting HTTP server...")
	return http.ListenAndServe(addr, s.router)
}

// parsePaginationParams extracts pagination parameters from query string
func (s *Server) parsePaginationParams(r *http.Request) PaginationParams {
	params := PaginationParams{
		Page:     1,
		PageSize: DefaultPageSize,
	}

	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		if page, err := strconv.Atoi(pageStr); err == nil && page > 0 {
			params.Page = page
		}
	}

	if pageSizeStr := r.URL.Query().Get("pageSize"); pageSizeStr != "" {
		if pageSize, err := strconv.Atoi(pageSizeStr); err == nil && pageSize > 0 {
			if pageSize > MaxPageSize {
				pageSize = MaxPageSize
			}
			params.PageSize = pageSize
		}
	}

	params.Offset = (params.Page - 1) * params.PageSize
	return params
}

// paginateSnapshots applies pagination to a slice of snapshots
func (s *Server) paginateSnapshots(snapshots []storage.KVSnapshot, params PaginationParams) ([]storage.KVSnapshot, bool, bool) {
	total := len(snapshots)

	// Calculate pagination bounds
	start := params.Offset
	end := start + params.PageSize

	if start >= total {
		return []storage.KVSnapshot{}, false, params.Page > 1
	}

	if end > total {
		end = total
	}

	hasNext := end < total
	hasPrev := params.Page > 1

	return snapshots[start:end], hasNext, hasPrev
}

// handleSnapshots handles GET /snapshots with pagination support
func (s *Server) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	// Parse pagination parameters
	params := s.parsePaginationParams(r)

	// Parse filter parameters
	minBalanceStr := r.URL.Query().Get("minBalance")
	queryName := r.URL.Query().Get("queryName")

	var snapshots []storage.KVSnapshot
	var err error

	// Apply filters if provided
	if minBalanceStr != "" {
		if minBalance, parseErr := strconv.ParseFloat(minBalanceStr, 64); parseErr == nil {
			snapshots, err = s.storage.SnapshotsByBalance(minBalance)
		} else {
			http.Error(w, "Invalid minBalance parameter", http.StatusBadRequest)
			return
		}
	} else if queryName != "" {
		snapshots, err = s.storage.SnapshotsByQuery(queryName)
	} else {
		// Get all snapshots (already ordered by date, most recent first)
		snapshots, err = s.storage.Snapshots()
	}

	if err != nil {
		log.Error().Err(err).Msg("Error getting snapshots")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Apply pagination
	paginatedSnapshots, hasNext, hasPrev := s.paginateSnapshots(snapshots, params)

	// Convert to response format
	responseSnapshots := make([]SnapshotResponse, len(paginatedSnapshots))
	for i, snapshot := range paginatedSnapshots {
		responseSnapshots[i] = SnapshotResponse{
			SnapshotDate:     snapshot.SnapshotDate.Format(time.RFC3339),
			CensusRoot:       snapshot.CensusRoot.String(),
			ParticipantCount: snapshot.ParticipantCount,
			MinBalance:       snapshot.MinBalance,
			QueryName:        snapshot.QueryName,
			CreatedAt:        snapshot.CreatedAt.Format(time.RFC3339),
		}
	}

	response := SnapshotsListResponse{
		Snapshots: responseSnapshots,
		Total:     len(snapshots),
		Page:      params.Page,
		PageSize:  params.PageSize,
		HasNext:   hasNext,
		HasPrev:   hasPrev,
	}

	// Set content type and encode response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error().Err(err).Msg("Error encoding snapshots response")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleLatestSnapshot handles GET /snapshots/latest
func (s *Server) handleLatestSnapshot(w http.ResponseWriter, r *http.Request) {
	// Get latest snapshot from storage
	latest, err := s.storage.LatestSnapshot()
	if err != nil {
		log.Error().Err(err).Msg("Error getting latest snapshot")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if latest == nil {
		http.Error(w, "No snapshots found", http.StatusNotFound)
		return
	}

	// Convert to response format
	response := SnapshotResponse{
		SnapshotDate:     latest.SnapshotDate.Format(time.RFC3339),
		CensusRoot:       latest.CensusRoot.String(),
		ParticipantCount: latest.ParticipantCount,
		MinBalance:       latest.MinBalance,
		QueryName:        latest.QueryName,
		CreatedAt:        latest.CreatedAt.Format(time.RFC3339),
	}

	// Set content type and encode response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error().Err(err).Msg("Error encoding latest snapshot response")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleHealth handles GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := HealthResponse{
		Status:    "healthy",
		Timestamp: time.Now().Format(time.RFC3339),
		Service:   "census3-bigquery",
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error().Err(err).Msg("Error encoding health response")
	}
}

// handleCensusSize handles GET /censuses/{censusId}/size (supports both UUID and root)
func (s *Server) handleCensusSize(w http.ResponseWriter, r *http.Request) {
	censusParam := chi.URLParam(r, "censusId")

	var size int
	var err error

	// Try to parse as UUID first (for working censuses)
	if censusID, parseErr := uuid.Parse(censusParam); parseErr == nil {
		// Load working census and get size
		ref, loadErr := s.censusDB.Load(censusID)
		if loadErr == nil {
			size = ref.Size()
		} else {
			err = loadErr
		}
	} else {
		// Try to parse as root hex (for published censuses)
		root, hexErr := hex.DecodeString(strings.TrimPrefix(censusParam, "0x"))
		if hexErr != nil {
			ErrInvalidCensusID.WithErr(hexErr).Write(w)
			return
		}
		size, err = s.censusDB.SizeByRoot(root)
	}

	if err != nil {
		ErrGenericInternalServerError.WithErr(err).Write(w)
		return
	}

	// Return response
	w.Header().Set("Content-Type", "application/json")
	response := CensusSizeResponse{
		Size: size,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		ErrMarshalingServerJSONFailed.WithErr(err).Write(w)
		return
	}
}

// handleCensusProof handles GET /censuses/{censusRoot}/proof?key={hexKey}
func (s *Server) handleCensusProof(w http.ResponseWriter, r *http.Request) {
	rootHex := chi.URLParam(r, "censusRoot")

	// Parse the root from hex
	root, err := hex.DecodeString(strings.TrimPrefix(rootHex, "0x"))
	if err != nil {
		ErrInvalidCensusID.WithErr(err).Write(w)
		return
	}

	// Get key from query parameter
	keyHex := r.URL.Query().Get("key")
	if keyHex == "" {
		ErrMalformedParam.With("missing key parameter").Write(w)
		return
	}

	key, err := hex.DecodeString(strings.TrimPrefix(keyHex, "0x"))
	if err != nil {
		ErrMalformedBody.WithErr(err).Write(w)
		return
	}

	// Generate proof
	proof, err := s.censusDB.ProofByRoot(root, key)
	if err != nil {
		ErrResourceNotFound.WithErr(err).Write(w)
		return
	}

	// Return proof
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(proof); err != nil {
		ErrMarshalingServerJSONFailed.WithErr(err).Write(w)
		return
	}
}

// handleCensusParticipants handles GET /censuses/{censusRoot}/participants with pagination
func (s *Server) handleCensusParticipants(w http.ResponseWriter, r *http.Request) {
	rootHex := chi.URLParam(r, "censusRoot")

	// Parse the root from hex - validate format first
	root, err := hex.DecodeString(strings.TrimPrefix(rootHex, "0x"))
	if err != nil {
		ErrInvalidCensusID.WithErr(err).Write(w)
		return
	}

	// Check if the root length is reasonable (should be a hash)
	if len(root) < 16 { // Minimum reasonable hash length
		ErrInvalidCensusID.With("census root too short").Write(w)
		return
	}

	// For now, we need to find the census by root and get all participants
	// This is not the most efficient approach, but it works with the current censusdb API
	// TODO: Implement efficient pagination in censusdb

	// We need to find the census ID from the root first
	// Since we don't have a direct way to do this efficiently, we'll return an error for now
	// indicating that this endpoint needs the census to be loaded by ID

	ErrGenericInternalServerError.With("participants endpoint requires census ID, not root - not yet implemented").Write(w)
}

// handleCreateCensus handles POST /censuses
func (s *Server) handleCreateCensus(w http.ResponseWriter, r *http.Request) {
	// Create a new working census with UUID
	censusID := uuid.New()
	_, err := s.censusDB.New(censusID)
	if err != nil {
		ErrGenericInternalServerError.WithErr(err).Write(w)
		return
	}

	// Return the census UUID
	response := NewCensusResponse{
		Census: censusID.String(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		ErrMarshalingServerJSONFailed.WithErr(err).Write(w)
		return
	}
}

// handleAddParticipants handles POST /censuses/{censusId}/participants
func (s *Server) handleAddParticipants(w http.ResponseWriter, r *http.Request) {
	censusIDStr := chi.URLParam(r, "censusId")

	// Parse census ID
	censusID, err := uuid.Parse(censusIDStr)
	if err != nil {
		ErrInvalidCensusID.WithErr(err).Write(w)
		return
	}

	// Parse request body
	var req CensusParticipantsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ErrMalformedBody.WithErr(err).Write(w)
		return
	}

	if len(req.Participants) == 0 {
		ErrMalformedBody.With("no participants provided").Write(w)
		return
	}

	// Load the census
	ref, err := s.censusDB.Load(censusID)
	if err != nil {
		ErrCensusNotFound.WithErr(err).Write(w)
		return
	}

	// Check census size limit
	currentSize := ref.Size()
	if currentSize+len(req.Participants) > s.maxCensusSize {
		ErrMalformedBody.Withf("census size limit exceeded: current=%d, adding=%d, max=%d",
			currentSize, len(req.Participants), s.maxCensusSize).Write(w)
		return
	}

	// Build keys and values for batch insert
	keys := make([][]byte, len(req.Participants))
	values := make([][]byte, len(req.Participants))

	for i, participant := range req.Participants {
		// Set default weight if not provided
		if participant.Weight == nil {
			participant.Weight = new(types.BigInt).SetUint64(1)
		}

		// Process key (hash if too long)
		key := []byte(participant.Key)
		if len(key) > censusdb.CensusKeyMaxLen {
			key = s.censusDB.HashAndTrunkKey(key)
			if key == nil {
				ErrGenericInternalServerError.With("failed to hash participant key").Write(w)
				return
			}
		}

		keys[i] = key
		values[i] = arbo.BigIntToBytes(s.censusDB.HashLen(), participant.Weight.MathBigInt())
	}

	// Insert batch
	invalid, err := ref.InsertBatch(keys, values)
	if err != nil {
		ErrGenericInternalServerError.WithErr(err).Write(w)
		return
	}

	if len(invalid) > 0 {
		ErrMalformedBody.Withf("failed to insert %d participants", len(invalid)).Write(w)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleGetParticipants handles GET /censuses/{censusId}/participants
func (s *Server) handleGetParticipants(w http.ResponseWriter, r *http.Request) {
	censusIDStr := chi.URLParam(r, "censusId")

	// Parse census ID
	censusID, err := uuid.Parse(censusIDStr)
	if err != nil {
		ErrInvalidCensusID.WithErr(err).Write(w)
		return
	}

	// Load the census
	_, err = s.censusDB.Load(censusID)
	if err != nil {
		ErrGenericInternalServerError.WithErr(err).Write(w)
		return
	}

	// TODO: Implement efficient pagination
	// For now, return error as this requires implementing FetchKeysAndValues in censusdb
	ErrGenericInternalServerError.With("participants listing not yet implemented").Write(w)
}

// handleGetRoot handles GET /censuses/{censusId}/root
func (s *Server) handleGetRoot(w http.ResponseWriter, r *http.Request) {
	censusIDStr := chi.URLParam(r, "censusId")

	// Parse census ID
	censusID, err := uuid.Parse(censusIDStr)
	if err != nil {
		ErrInvalidCensusID.WithErr(err).Write(w)
		return
	}

	// Load the census
	ref, err := s.censusDB.Load(censusID)
	if err != nil {
		ErrGenericInternalServerError.WithErr(err).Write(w)
		return
	}

	// Get root
	root := ref.Root()
	response := CensusRootResponse{
		Root: types.HexBytes(root),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		ErrMarshalingServerJSONFailed.WithErr(err).Write(w)
		return
	}
}

// handleDeleteCensus handles DELETE /censuses/{censusId}
func (s *Server) handleDeleteCensus(w http.ResponseWriter, r *http.Request) {
	censusIDStr := chi.URLParam(r, "censusId")

	// Parse census ID
	censusID, err := uuid.Parse(censusIDStr)
	if err != nil {
		ErrInvalidCensusID.WithErr(err).Write(w)
		return
	}

	// Delete the census
	if err := s.censusDB.Del(censusID); err != nil {
		ErrGenericInternalServerError.WithErr(err).Write(w)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handlePublishCensus handles POST /censuses/{censusId}/publish
func (s *Server) handlePublishCensus(w http.ResponseWriter, r *http.Request) {
	censusIDStr := chi.URLParam(r, "censusId")

	// Parse census ID
	censusID, err := uuid.Parse(censusIDStr)
	if err != nil {
		ErrInvalidCensusID.WithErr(err).Write(w)
		return
	}

	// Load working census
	workingRef, err := s.censusDB.Load(censusID)
	if err != nil {
		ErrCensusNotFound.WithErr(err).Write(w)
		return
	}

	// Get current root and participant count
	root := workingRef.Root()
	participantCount := workingRef.Size()
	createdAt := workingRef.LastUsed // Use LastUsed as creation time
	publishedAt := time.Now()

	// Create root-based census
	rootRef, err := s.censusDB.NewByRoot(root)
	if err != nil && err != censusdb.ErrCensusAlreadyExists {
		ErrGenericInternalServerError.WithErr(err).Write(w)
		return
	}

	// If census already exists, load it
	if err == censusdb.ErrCensusAlreadyExists {
		rootRef, err = s.censusDB.LoadByRoot(root)
		if err != nil {
			ErrGenericInternalServerError.WithErr(err).Write(w)
			return
		}
	}

	// Copy participants from working census to root-based census
	if err := s.censusDB.PublishCensus(censusID, rootRef); err != nil {
		ErrGenericInternalServerError.WithErr(err).Write(w)
	}

	// Verify root matches
	finalRoot := rootRef.Root()
	if !bytes.Equal(root, finalRoot) {
		ErrGenericInternalServerError.Withf("root verification failed: expected %x, got %x", root, finalRoot).Write(w)
		return
	}

	// Schedule cleanup of working census
	go func() {
		if err := s.censusDB.CleanupWorkingCensus(censusID); err != nil {
			log.Error().Str("census_id", censusID.String()).Err(err).Msg("Failed to cleanup working census")
		}
	}()

	// Return response
	response := PublishCensusResponse{
		Root:             types.HexBytes(root),
		ParticipantCount: participantCount,
		CreatedAt:        createdAt.Format(time.RFC3339),
		PublishedAt:      publishedAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		ErrMarshalingServerJSONFailed.WithErr(err).Write(w)
		return
	}
}

// corsMiddleware adds CORS headers to responses
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// Handle preflight requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Call the next handler
		next.ServeHTTP(w, r)
	})
}
