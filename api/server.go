package api

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
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
	storage  SnapshotStorage
	censusDB *censusdb.CensusDB
	port     int
	router   *mux.Router
}

// CensusParticipant represents a participant in the census API response
type CensusParticipant struct {
	Key    types.HexBytes `json:"key"`
	Weight *types.BigInt  `json:"weight,omitempty"`
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
func NewServer(storage SnapshotStorage, censusDB *censusdb.CensusDB, port int) *Server {
	s := &Server{
		storage:  storage,
		censusDB: censusDB,
		port:     port,
	}
	s.setupRouter()
	return s
}

// setupRouter initializes the router with all endpoints
func (s *Server) setupRouter() {
	s.router = mux.NewRouter()

	// Existing snapshot endpoints
	s.router.HandleFunc("/snapshots", s.handleSnapshots).Methods("GET")
	s.router.HandleFunc("/snapshots/latest", s.handleLatestSnapshot).Methods("GET")
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")

	// New census endpoints
	s.router.HandleFunc("/censuses/{root}/size", s.handleCensusSize).Methods("GET")
	s.router.HandleFunc("/censuses/{root}/proof", s.handleCensusProof).Methods("GET")
	s.router.HandleFunc("/censuses/{root}/participants", s.handleCensusParticipants).Methods("GET")
}

// Start starts the HTTP server
func (s *Server) Start() error {
	// Add CORS middleware
	handler := s.corsMiddleware(s.router)

	addr := fmt.Sprintf(":%d", s.port)
	log.Info().Str("address", addr).Msg("Starting API server")

	return http.ListenAndServe(addr, handler)
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
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

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
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

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
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
		"service":   "census3-bigquery",
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error().Err(err).Msg("Error encoding health response")
	}
}

// handleCensusSize handles GET /censuses/{root}/size
func (s *Server) handleCensusSize(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rootHex := vars["root"]

	// Parse the root from hex
	root, err := hex.DecodeString(strings.TrimPrefix(rootHex, "0x"))
	if err != nil {
		ErrInvalidCensusID.WithErr(err).Write(w)
		return
	}

	// Get size by root
	size, err := s.censusDB.SizeByRoot(root)
	if err != nil {
		ErrGenericInternalServerError.WithErr(err).Write(w)
		return
	}

	// Return response
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"size": size,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		ErrMarshalingServerJSONFailed.WithErr(err).Write(w)
		return
	}
}

// handleCensusProof handles GET /censuses/{root}/proof?key={hexKey}
func (s *Server) handleCensusProof(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rootHex := vars["root"]

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

// handleCensusParticipants handles GET /censuses/{root}/participants with pagination
func (s *Server) handleCensusParticipants(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rootHex := vars["root"]

	// Parse the root from hex
	_, err := hex.DecodeString(strings.TrimPrefix(rootHex, "0x"))
	if err != nil {
		ErrInvalidCensusID.WithErr(err).Write(w)
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
