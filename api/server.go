package api

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/vocdoni/census3-bigquery/censusdb"
	"github.com/vocdoni/census3-bigquery/storage"
	"github.com/vocdoni/davinci-node/log"
	"github.com/vocdoni/davinci-node/types"
)

// SnapshotStorage interface for storage operations
type SnapshotStorage interface {
	Snapshots() ([]storage.KVSnapshot, error)
	LatestSnapshot() (*storage.KVSnapshot, error)
	SnapshotCount() (int, error)
	SnapshotsByBalance(minBalance float64) ([]storage.KVSnapshot, error)
	SnapshotsByQuery(queryName string) ([]storage.KVSnapshot, error)
	// Metadata operations
	GetMetadata(metadataType string, censusRoot types.HexBytes) ([]byte, error)
	HasMetadata(metadataType string, censusRoot types.HexBytes) (bool, error)
}

// Server handles HTTP API requests
type Server struct {
	storage       SnapshotStorage
	censusDB      *censusdb.CensusDB
	port          int
	maxCensusSize int
	router        chi.Router
	httpServer    *http.Server
}

// Default pagination values
const (
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// Weight strategy constants
const (
	WeightStrategyConstant = "constant"
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
	r.Get("/censuses/{censusRoot}/dump", s.handleGetDump)

	// Metadata endpoints
	r.Get("/metadata/farcaster/{censusRoot}", s.handleFarcasterMetadata)

	s.router = r
}

// Start starts the HTTP server
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	log.Infow("HTTP server binding to address...", "address", addr)

	// Log when server is ready to accept connections
	go func() {
		// Small delay to ensure the server has started
		time.Sleep(50 * time.Millisecond)
		log.Infow("API server ready to accept requests", "address", addr)
	}()

	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: s.router,
	}

	log.Infow("starting HTTP server...", "address", addr)
	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	log.Infow("HTTP server stopped", "address", addr)
	return nil
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}

	log.Infow("stopping HTTP server", "address", s.httpServer.Addr)
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown HTTP server: %w", err)
	}

	return nil
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
		log.Errorw(err, "error getting snapshots")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Apply pagination
	paginatedSnapshots, hasNext, hasPrev := s.paginateSnapshots(snapshots, params)

	// Convert to response format
	responseSnapshots := make([]SnapshotResponse, len(paginatedSnapshots))
	for i, snapshot := range paginatedSnapshots {
		response := SnapshotResponse{
			SnapshotDate:     snapshot.SnapshotDate.Format(time.RFC3339),
			CreatedAt:        time.Now().Format(time.RFC3339),
			CensusRoot:       snapshot.CensusRoot.String(),
			ParticipantCount: snapshot.ParticipantCount,
			MinBalance:       snapshot.MinBalance,
			QueryName:        snapshot.QueryName,
			DisplayName:      snapshot.DisplayName,
			DisplayAvatar:    snapshot.DisplayAvatar,
			WeightStrategy:   mapWeightStrategy(snapshot.WeightConfig),
		}

		// Check for available metadata and add links
		metadata := make(map[string]string)

		// Check for Farcaster metadata
		if hasFarcaster, err := s.storage.HasMetadata("meta_farcaster", snapshot.CensusRoot); err == nil && hasFarcaster {
			metadata["farcaster"] = fmt.Sprintf("/metadata/farcaster/%s", snapshot.CensusRoot.String())
		}

		if len(metadata) > 0 {
			response.Metadata = metadata
		}

		responseSnapshots[i] = response
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
		log.Errorw(err, "error encoding snapshots response")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleLatestSnapshot handles GET /snapshots/latest
func (s *Server) handleLatestSnapshot(w http.ResponseWriter, r *http.Request) {
	// Get latest snapshot from storage
	latest, err := s.storage.LatestSnapshot()
	if err != nil {
		log.Errorw(err, "error getting latest snapshot")
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
		CreatedAt:        time.Now().Format(time.RFC3339),
		CensusRoot:       latest.CensusRoot.String(),
		ParticipantCount: latest.ParticipantCount,
		MinBalance:       latest.MinBalance,
		QueryName:        latest.QueryName,
		DisplayName:      latest.DisplayName,
		DisplayAvatar:    latest.DisplayAvatar,
		WeightStrategy:   mapWeightStrategy(latest.WeightConfig),
	}

	// Set content type and encode response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Errorw(err, "error encoding latest snapshot response")
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
		log.Errorw(err, "error encoding health response")
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
			ErrInvalidCensusRoot.WithErr(hexErr).Write(w)
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
		ErrInvalidCensusRoot.WithErr(err).Write(w)
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
		ErrInvalidCensusRoot.WithErr(err).Write(w)
		return
	}

	// Check if the root length is reasonable (should be a hash)
	if len(root) < 16 { // Minimum reasonable hash length
		ErrInvalidCensusRoot.With("census root too short").Write(w)
		return
	}

	// Load census by root
	ref, err := s.censusDB.LoadByRoot(root)
	if err != nil {
		ErrCensusNotFound.WithErr(err).Write(w)
		return
	}

	// Parse pagination parameters with custom defaults for this endpoint
	page := 1
	pageSize := 1000 // Fixed page size as requested

	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}

	// Calculate offset
	offset := (page - 1) * pageSize

	// Get total size
	totalSize := ref.Size()

	// Check if offset is beyond total size
	if offset >= totalSize {
		// Return empty response
		response := CensusParticipantsResponse{
			Participants: []CensusParticipant{},
			Total:        totalSize,
			Page:         page,
			PageSize:     pageSize,
			HasNext:      false,
			HasPrev:      page > 1,
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			ErrMarshalingServerJSONFailed.WithErr(err).Write(w)
		}
		return
	}

	// Use DumpRange to get participants for this page
	// DumpRange returns an io.Reader that streams JSON Lines
	reader := ref.Tree().DumpRange(offset, pageSize)

	// Decode participants from JSON Lines
	participants := make([]CensusParticipant, 0, pageSize)
	decoder := json.NewDecoder(reader)

	for decoder.More() {
		var entry struct {
			Index   uint64         `json:"index"`
			Address common.Address `json:"address"`
			Weight  *big.Int       `json:"weight"`
		}
		if err := decoder.Decode(&entry); err != nil {
			ErrGenericInternalServerError.WithErr(err).Write(w)
			return
		}

		// Convert to API response format
		participant := CensusParticipant{
			Key:    types.HexBytes(entry.Address.Bytes()),
			Weight: (*types.BigInt)(entry.Weight),
		}
		participants = append(participants, participant)
	}

	// Calculate pagination flags
	hasNext := offset+pageSize < totalSize
	hasPrev := page > 1

	// Build response
	response := CensusParticipantsResponse{
		Participants: participants,
		Total:        totalSize,
		Page:         page,
		PageSize:     pageSize,
		HasNext:      hasNext,
		HasPrev:      hasPrev,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		ErrMarshalingServerJSONFailed.WithErr(err).Write(w)
		return
	}
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
			key = s.censusDB.TrunkKey(key)
			if key == nil {
				ErrGenericInternalServerError.With("failed to trunk participant key").Write(w)
				return
			}
		}

		keys[i] = key
		values[i] = bigIntToBytes(s.censusDB.HashLen(), participant.Weight.MathBigInt())
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

// handleGetDump handles GET /censuses/{censusRoot}/dump to stream census dump
// as a JSONL response
func (s *Server) handleGetDump(w http.ResponseWriter, r *http.Request) {
	// Stream census dump as JSON Lines
	w.Header().Set("Content-Type", "application/x-ndjson")

	// Parse census root
	censusRootHex := chi.URLParam(r, "censusRoot")
	if censusRootHex == "" {
		ErrInvalidCensusRoot.With("missing censusRoot parameter").Write(w)
		return
	}
	bCensusRoot, err := hex.DecodeString(strings.TrimPrefix(censusRootHex, "0x"))
	if err != nil {
		ErrInvalidCensusRoot.WithErr(err).Write(w)
		return
	}
	censusRoot := types.HexBytes(bCensusRoot)
	// Load the census
	ref, err := s.censusDB.LoadByRoot(censusRoot)
	if err != nil {
		ErrGenericInternalServerError.WithErr(err).Write(w)
		return
	}
	// Get the dump reader to stream the census dump
	dumpReader := ref.Tree().Dump()
	if closer, ok := dumpReader.(io.Closer); ok {
		defer func() {
			if err := closer.Close(); err != nil {
				log.Warnw("error closing dump reader", "err", err, "root", censusRoot.String())
			}
		}()
	}
	// Crate a single buffer to reuse
	buf := make([]byte, 32*1024) // 32KB
	for {
		// Check if the request context is done (client disconnected)
		select {
		case <-r.Context().Done():
			log.Infow("request cancelled by client", "root", censusRoot.String())
			return
		default:
		}
		// Read from dump and write to response
		nRead, readErr := dumpReader.Read(buf)
		if nRead > 0 {
			if _, writeErr := w.Write(buf[:nRead]); writeErr != nil {
				log.Errorf("error writing census dump with root '%s' to response: %v", censusRoot.String(), writeErr)
				return
			}
			// Flush the response writer if it supports flushing
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		// Handle read errors
		if readErr != nil {
			if readErr != io.EOF {
				log.Errorf("error reading census dump with root '%s': %v", censusRoot.String(), readErr)
			}
			return
		}
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
			log.Errorw(err, "failed to cleanup working census")
		}
	}()

	// Return response
	response := PublishCensusResponse{
		Root:             types.HexBytes(root),
		ParticipantCount: participantCount,
		CreatedAt:        createdAt.Format(time.RFC3339),
		PublishedAt:      publishedAt.Format(time.RFC3339),
		CensusURI:        censusURIByRoot(root),
		Size:             rootRef.Size(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		ErrMarshalingServerJSONFailed.WithErr(err).Write(w)
		return
	}
}

// mapWeightStrategy maps internal weight strategy to API response format
func mapWeightStrategy(weightConfig *storage.WeightConfig) string {
	if weightConfig == nil || weightConfig.Strategy == "" {
		return WeightStrategyConstant
	}

	switch weightConfig.Strategy {
	case WeightStrategyConstant:
		return WeightStrategyConstant
	case "proportional_auto", "proportional_manual":
		return "proportional"
	default:
		return WeightStrategyConstant
	}
}

// handleFarcasterMetadata handles GET /metadata/farcaster/{censusRoot}
func (s *Server) handleFarcasterMetadata(w http.ResponseWriter, r *http.Request) {
	rootHex := chi.URLParam(r, "censusRoot")

	// Parse the root from hex
	root, err := hex.DecodeString(strings.TrimPrefix(rootHex, "0x"))
	if err != nil {
		ErrInvalidCensusRoot.WithErr(err).Write(w)
		return
	}

	// Convert to types.HexBytes
	censusRoot := types.HexBytes(root)

	// Get Farcaster metadata from storage
	metadataJSON, err := s.storage.GetMetadata("meta_farcaster", censusRoot)
	if err != nil {
		log.Errorw(err, "error getting Farcaster metadata")
		ErrGenericInternalServerError.WithErr(err).Write(w)
		return
	}

	if metadataJSON == nil {
		ErrResourceNotFound.With("Farcaster metadata not found for census root").Write(w)
		return
	}

	// Return the JSON metadata directly
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(metadataJSON); err != nil {
		log.Errorw(err, "error writing Farcaster metadata response")
		ErrGenericInternalServerError.WithErr(err).Write(w)
		return
	}
}

// bigIntToBytes converts a big.Int to a byte slice of the specified length in big-endian format.
// Big-endian is used for Ethereum/Solidity compatibility.
func bigIntToBytes(length int, value *big.Int) []byte {
	if value == nil {
		return make([]byte, length)
	}

	// Get bytes from big.Int (already big-endian from Go)
	valueBytes := value.Bytes()

	// Create result with fixed length, padding with zeros on the left
	result := make([]byte, length)

	// Copy value bytes to the end (right-aligned, big-endian)
	if len(valueBytes) <= length {
		copy(result[length-len(valueBytes):], valueBytes)
	} else {
		// If value is too large, copy only the least significant bytes
		copy(result, valueBytes[len(valueBytes)-length:])
	}

	return result
}

// censusURIByRoot constructs the census URI given a census root
func censusURIByRoot(censusRoot types.HexBytes) string {
	return fmt.Sprintf("/censuses/%s/dump", censusRoot.String())
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
