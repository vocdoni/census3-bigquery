package neynar

import (
	"census3-bigquery/log"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// NeynarAPIBaseURL is the base URL for Neynar API
	NeynarAPIBaseURL = "https://api.neynar.com/v2"

	// MaxAddressesPerRequest is the maximum number of addresses per API call
	MaxAddressesPerRequest = 200

	// DefaultTimeout for HTTP requests
	DefaultTimeout = 30 * time.Second
)

// Client represents a Neynar API client
type Client struct {
	apiKey     string
	httpClient *http.Client
	baseURL    string
}

// BulkByAddressResponse represents the response from the bulk-by-address endpoint
type BulkByAddressResponse map[string][]User

type User struct {
	Object           string             `json:"object"` // "user"
	FID              int64              `json:"fid"`
	Username         string             `json:"username"`
	DisplayName      string             `json:"display_name"`
	PFPURL           string             `json:"pfp_url"`
	CustodyAddress   string             `json:"custody_address"`
	Profile          *Profile           `json:"profile,omitempty"`
	FollowerCount    int64              `json:"follower_count"`
	FollowingCount   int64              `json:"following_count"`
	Verifications    []string           `json:"verifications,omitempty"`
	VerifiedAddrs    *VerifiedAddresses `json:"verified_addresses,omitempty"`
	AuthAddresses    []AuthAddress      `json:"auth_addresses,omitempty"`
	VerifiedAccounts []VerifiedAccount  `json:"verified_accounts,omitempty"`
	PowerBadge       bool               `json:"power_badge"`
	Experimental     *Experimental      `json:"experimental,omitempty"`
	Score            float64            `json:"score,omitempty"`
}

type Profile struct {
	Bio      *Bio      `json:"bio,omitempty"`
	Location *Location `json:"location,omitempty"`
}

type Bio struct {
	Text string `json:"text"`
}

type Location struct {
	Latitude  float64  `json:"latitude"`
	Longitude float64  `json:"longitude"`
	Address   *Address `json:"address,omitempty"`
}

type Address struct {
	City        string `json:"city"`
	State       string `json:"state"`
	Country     string `json:"country"`
	CountryCode string `json:"country_code"`
}

type VerifiedAddresses struct {
	EthAddresses []string         `json:"eth_addresses,omitempty"`
	SolAddresses []string         `json:"sol_addresses,omitempty"`
	Primary      *PrimaryVerified `json:"primary,omitempty"`
}

type PrimaryVerified struct {
	EthAddress string `json:"eth_address"`
	SolAddress string `json:"sol_address"`
}

type AuthAddress struct {
	Address string         `json:"address"`
	App     *DehydratedApp `json:"app,omitempty"`
}

type DehydratedApp struct {
	Object string `json:"object"` // "user_dehydrated"
	FID    int64  `json:"fid"`
}

type VerifiedAccount struct {
	Platform string `json:"platform"` // e.g., "x"
	Username string `json:"username"` // e.g., "wildp4u"
}

type Experimental struct {
	NeynarUserScore   float64 `json:"neynar_user_score"`
	DeprecationNotice string  `json:"deprecation_notice"`
}

// NewClient creates a new Neynar API client
func NewClient(apiKey string) *Client {
	return &Client{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: DefaultTimeout,
		},
		baseURL: NeynarAPIBaseURL,
	}
}

// GetUsersByAddresses retrieves Farcaster users by their Ethereum addresses
// Automatically batches requests to respect the 350 address limit
func (c *Client) GetUsersByAddresses(ctx context.Context, addresses []string) (map[string][]User, error) {
	if len(addresses) == 0 {
		return make(map[string][]User), nil
	}

	log.Info().
		Int("total_addresses", len(addresses)).
		Int("max_per_request", MaxAddressesPerRequest).
		Msg("Starting Neynar API batch requests")

	allUsers := make(map[string][]User)

	// Process addresses in batches of MaxAddressesPerRequest
	for i := 0; i < len(addresses); i += MaxAddressesPerRequest {
		end := i + MaxAddressesPerRequest
		if end > len(addresses) {
			end = len(addresses)
		}

		batch := addresses[i:end]
		batchNum := (i / MaxAddressesPerRequest) + 1
		totalBatches := (len(addresses) + MaxAddressesPerRequest - 1) / MaxAddressesPerRequest

		log.Info().
			Int("batch", batchNum).
			Int("total_batches", totalBatches).
			Int("batch_size", len(batch)).
			Msg("Processing Neynar API batch")

		batchUsers, err := c.getUsersByAddressesBatch(ctx, batch)
		if err != nil {
			log.Error().
				Err(err).
				Int("batch", batchNum).
				Int("batch_size", len(batch)).
				Msg("Neynar API batch request failed")
			return nil, fmt.Errorf("batch %d failed: %w", batchNum, err)
		}

		// Merge batch results into final result
		maps.Copy(allUsers, batchUsers)

		log.Info().
			Int("batch", batchNum).
			Int("users_found", len(batchUsers)).
			Msg("Neynar API batch completed successfully")
	}

	log.Info().
		Int("total_addresses_processed", len(addresses)).
		Int("addresses_with_users", len(allUsers)).
		Msg("Neynar API batch processing completed")

	return allUsers, nil
}

// getUsersByAddressesBatch handles a single batch request to Neynar API
func (c *Client) getUsersByAddressesBatch(ctx context.Context, addresses []string) (map[string][]User, error) {
	if len(addresses) > MaxAddressesPerRequest {
		return nil, fmt.Errorf("batch size %d exceeds maximum %d", len(addresses), MaxAddressesPerRequest)
	}

	// Build the request URL
	endpoint := fmt.Sprintf("%s/farcaster/user/bulk-by-address", c.baseURL)

	// Create URL with query parameters
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint URL: %w", err)
	}

	// Join addresses with commas (don't URL encode to preserve commas)
	addressesParam := strings.Join(addresses, ",")

	// Manually build query string to avoid URL encoding the commas
	u.RawQuery = "addresses=" + addressesParam

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set headers
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "census3-bigquery/1.0")

	log.Debug().
		Str("url", u.String()).
		Int("address_count", len(addresses)).
		Msg("Making Neynar API request")

	// Make the request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Warn().Err(closeErr).Msg("Failed to close response body")
		}
	}()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("neynar API returned status %d: %s", resp.StatusCode, resp.Status)
	}

	// Parse response
	var result BulkByAddressResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	log.Debug().
		Int("addresses_requested", len(addresses)).
		Int("addresses_with_users", len(result)).
		Msg("Neynar API response parsed successfully")

	return result, nil
}

// Close closes the client (no-op for HTTP client)
func (c *Client) Close() error {
	return nil
}
