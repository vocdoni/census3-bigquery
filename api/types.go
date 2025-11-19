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
