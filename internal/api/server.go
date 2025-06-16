package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"census3-bigquery/internal/storage"
)

// Server handles HTTP API requests
type Server struct {
	storage *storage.SnapshotStorage
	port    int
}

// SnapshotResponse represents the API response for snapshots
type SnapshotResponse struct {
	SnapshotDate string `json:"snapshotDate"`
	CensusRoot   string `json:"censusRoot"`
}

// SnapshotsListResponse represents the full response for the snapshots endpoint
type SnapshotsListResponse struct {
	Snapshots []SnapshotResponse `json:"snapshots"`
	Total     int                `json:"total"`
}

// NewServer creates a new API server
func NewServer(storage *storage.SnapshotStorage, port int) *Server {
	return &Server{
		storage: storage,
		port:    port,
	}
}

// Start starts the HTTP server
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// Register endpoints
	mux.HandleFunc("/snapshots", s.handleSnapshots)
	mux.HandleFunc("/health", s.handleHealth)

	// Add CORS middleware
	handler := s.corsMiddleware(mux)

	addr := fmt.Sprintf(":%d", s.port)
	log.Printf("Starting API server on %s", addr)

	return http.ListenAndServe(addr, handler)
}

// handleSnapshots handles GET /snapshots
func (s *Server) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get snapshots from storage
	snapshots := s.storage.GetSnapshots()

	// Convert to response format
	responseSnapshots := make([]SnapshotResponse, len(snapshots))
	for i, snapshot := range snapshots {
		responseSnapshots[i] = SnapshotResponse{
			SnapshotDate: snapshot.SnapshotDate.Format(time.RFC3339),
			CensusRoot:   snapshot.CensusRoot.String(),
		}
	}

	response := SnapshotsListResponse{
		Snapshots: responseSnapshots,
		Total:     len(responseSnapshots),
	}

	// Set content type and encode response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding snapshots response: %v", err)
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
		log.Printf("Error encoding health response: %v", err)
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
