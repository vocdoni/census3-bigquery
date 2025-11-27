package api

import (
	"github.com/vocdoni/davinci-node/types"
)

// HealthResponse represents the response for the health endpoint
type HealthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
	Service   string `json:"service"`
}

// CensusSizeResponse represents the response for the census size endpoint
type CensusSizeResponse struct {
	Size int `json:"size"`
}

// CensusURIResponse represents the response for the census URI endpoint
type CensusURIResponse struct {
	URI string `json:"uri"`
}

// CensusRootResponse represents the response for the census root endpoint
type CensusRootResponse struct {
	Root types.HexBytes `json:"root"`
}

// ErrorResponse represents the response for API errors
type ErrorResponse struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
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
	Root          types.HexBytes `json:"root"`
	CreatedAt     string         `json:"createdAt"`
	PublishedAt   string         `json:"publishedAt"`
	CensusURIPath string         `json:"censusUriPath,omitempty"`
	Size          int            `json:"size"`
}

// SnapshotResponse represents the API response for snapshots
type SnapshotResponse struct {
	SnapshotDate   string            `json:"snapshotDate"`
	CensusRoot     string            `json:"censusRoot"`
	Size           int               `json:"size"`
	MinBalance     float64           `json:"minBalance"`
	QueryName      string            `json:"queryName"`
	CreatedAt      string            `json:"createdAt"`
	DisplayName    string            `json:"displayName"`
	DisplayAvatar  string            `json:"displayAvatar"`
	WeightStrategy string            `json:"weightStrategy"`
	Metadata       map[string]string `json:"metadata,omitempty"` // Map of metadata type to API path
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
