package alchemy

import (
	"census3-bigquery/log"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Client wraps Alchemy API operations
type Client struct {
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a new Alchemy client
func NewClient(ctx context.Context, apiKey string) (*Client, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("alchemy API key is required")
	}

	// Create HTTP client for direct API calls
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	return &Client{
		apiKey:     apiKey,
		httpClient: httpClient,
	}, nil
}

// Close closes the Alchemy client
func (c *Client) Close() error {
	// No specific cleanup needed for HTTP client
	return nil
}

// StreamBalances streams balance data from Alchemy to a channel
func (c *Client) StreamBalances(ctx context.Context, cfg Config, participantCh chan<- Participant, errorCh chan<- error) {
	// Don't close channels here - let the streaming functions handle it
	// This ensures multi_nft_holders can complete before channels are closed

	// Validate network
	network, exists := GetNetwork(cfg.Network)
	if !exists {
		errorCh <- fmt.Errorf("invalid network: %s", cfg.Network)
		close(participantCh)
		close(errorCh)
		return
	}

	// Get the query from the registry
	query, err := GetQuery(cfg.QueryName)
	if err != nil {
		errorCh <- fmt.Errorf("failed to get query: %w", err)
		close(participantCh)
		close(errorCh)
		return
	}

	// Prepare query parameters
	queryParams := map[string]interface{}{
		"min_balance": cfg.MinBalance,
		"network":     cfg.Network,
	}

	// Add contract address from config
	if cfg.ContractAddress != "" {
		queryParams["contract_address"] = cfg.ContractAddress
	}

	// Add any additional query parameters
	for key, value := range cfg.QueryParams {
		queryParams[key] = value
	}

	// Validate that all required parameters are provided
	if err := ValidateQueryParameters(query, queryParams); err != nil {
		errorCh <- fmt.Errorf("query parameter validation failed: %w", err)
		close(participantCh)
		close(errorCh)
		return
	}

	log.Info().
		Str("query_name", cfg.QueryName).
		Str("network", cfg.Network).
		Str("contract_address", cfg.ContractAddress).
		Float64("min_balance", cfg.MinBalance).
		Msg("Starting Alchemy streaming query")

	// Execute the appropriate query based on type
	// Each streaming function is responsible for closing the channels when done
	switch cfg.QueryName {
	case "nft_holders", "nft_holders_with_metadata":
		defer close(participantCh)
		defer close(errorCh)
		c.streamNFTHolders(ctx, network, cfg, participantCh, errorCh)
	case "erc20_holders":
		defer close(participantCh)
		defer close(errorCh)
		c.streamERC20Holders(ctx, network, cfg, participantCh, errorCh)
	case "multi_nft_holders":
		// multi_nft_holders needs to block until completion
		c.streamMultiNFTHolders(ctx, network, cfg, participantCh, errorCh)
		close(participantCh)
		close(errorCh)
	default:
		errorCh <- fmt.Errorf("unsupported query type: %s", cfg.QueryName)
		close(participantCh)
		close(errorCh)
	}
}

// streamNFTHolders streams NFT holders from Alchemy's getOwnersForContract API
func (c *Client) streamNFTHolders(ctx context.Context, network NetworkInfo, cfg Config, participantCh chan<- Participant, errorCh chan<- error) {
	endpoint := BuildEndpointURL(network, c.apiKey, Query{Endpoint: "/nft/v3/{apiKey}/getOwnersForContract"})

	pageKey := ""
	totalProcessed := 0
	startTime := time.Now()
	lastLogTime := startTime

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			log.Info().
				Int("total_processed", totalProcessed).
				Msg("Alchemy streaming cancelled by context")
			return
		default:
		}

		// Build query parameters
		params := url.Values{}
		params.Set("contractAddress", cfg.ContractAddress)
		params.Set("withTokenBalances", "true")
		if pageKey != "" {
			params.Set("pageKey", pageKey)
		}
		if cfg.PageSize > 0 {
			params.Set("pageSize", strconv.Itoa(cfg.PageSize))
		}

		// Make API request
		requestURL := endpoint + "?" + params.Encode()

		resp, err := c.makeRequest(ctx, "GET", requestURL, nil)
		if err != nil {
			errorCh <- fmt.Errorf("failed to make request: %w", err)
			return
		}

		// Parse response
		var ownersResp OwnersResponse
		if err := json.Unmarshal(resp, &ownersResp); err != nil {
			errorCh <- fmt.Errorf("failed to parse response: %w", err)
			return
		}

		// Process owners
		for _, owner := range ownersResp.Owners {
			// Calculate total NFT balance by summing all token balances
			balance := int64(0)
			for _, tokenBalance := range owner.TokenBalances {
				if parsed, err := strconv.ParseInt(tokenBalance.Balance, 10, 64); err == nil {
					balance += parsed
				} else {
					log.Debug().
						Err(err).
						Str("address", owner.OwnerAddress).
						Str("balance", tokenBalance.Balance).
						Msg("Failed to parse token balance")
				}
			}

			// Apply minimum balance filter
			if float64(balance) < cfg.MinBalance {
				continue
			}

			// Validate address
			if !common.IsHexAddress(owner.OwnerAddress) {
				log.Warn().
					Str("address", owner.OwnerAddress).
					Msg("Skipping invalid address format")
				continue
			}

			address := common.HexToAddress(owner.OwnerAddress)

			// Calculate weight based on configuration
			weight, err := calculateWeight(balance, cfg)
			if err != nil {
				log.Warn().
					Err(err).
					Str("address", owner.OwnerAddress).
					Msg("Failed to calculate weight, skipping")
				continue
			}

			participant := Participant{
				Address: address,
				Balance: big.NewInt(weight),
			}

			select {
			case participantCh <- participant:
				totalProcessed++
			case <-ctx.Done():
				return
			}
		}

		// Log progress every 10 seconds
		currentTime := time.Now()
		if currentTime.Sub(lastLogTime) >= 10*time.Second {
			elapsed := currentTime.Sub(startTime)
			rate := float64(totalProcessed) / elapsed.Seconds()
			log.Info().
				Int("processed", totalProcessed).
				Str("elapsed", elapsed.String()).
				Float64("addr_per_sec", rate).
				Str("network", cfg.Network).
				Msg("Alchemy streaming progress")
			lastLogTime = currentTime
		}

		// Check if there are more pages
		if ownersResp.PageKey == "" {
			break
		}
		pageKey = ownersResp.PageKey
	}

	elapsed := time.Since(startTime)
	rate := float64(totalProcessed) / elapsed.Seconds()
	log.Info().
		Int("total_processed", totalProcessed).
		Str("elapsed", elapsed.String()).
		Float64("addr_per_sec", rate).
		Str("network", cfg.Network).
		Msg("Alchemy streaming completed")
}

// streamERC20Holders streams ERC20 token holders (placeholder for future implementation)
func (c *Client) streamERC20Holders(_ context.Context, _ NetworkInfo, _ Config, _ chan<- Participant, errorCh chan<- error) {
	// This would require using Alchemy's token balance APIs
	// For now, this is a placeholder
	errorCh <- fmt.Errorf("ERC20 holder queries not yet implemented for Alchemy")
}

// streamMultiNFTHolders streams holders across multiple NFT contracts
func (c *Client) streamMultiNFTHolders(ctx context.Context, network NetworkInfo, cfg Config, participantCh chan<- Participant, errorCh chan<- error) {
	// Get contract addresses from parameters
	var contractAddresses []string

	if addresses, ok := cfg.QueryParams["contract_addresses"]; ok {
		switch v := addresses.(type) {
		case []string:
			contractAddresses = v
		case []interface{}:
			for _, addr := range v {
				if s, ok := addr.(string); ok {
					contractAddresses = append(contractAddresses, s)
				}
			}
		case string:
			// Handle comma-separated addresses
			contractAddresses = strings.Split(v, ",")
		}
	} else if cfg.ContractAddress != "" {
		contractAddresses = []string{cfg.ContractAddress}
	}

	if len(contractAddresses) == 0 {
		errorCh <- fmt.Errorf("no contract addresses provided for multi NFT query")
		return
	}

	// Track unique addresses across all contracts
	uniqueHolders := make(map[common.Address]*big.Int)
	var mu sync.Mutex

	// Create a wait group for concurrent processing
	var wg sync.WaitGroup
	errChan := make(chan error, len(contractAddresses))

	// Process each contract concurrently
	for _, contractAddr := range contractAddresses {
		contractAddr = strings.TrimSpace(contractAddr)
		if !common.IsHexAddress(contractAddr) {
			log.Warn().
				Str("contract", contractAddr).
				Msg("Skipping invalid contract address")
			continue
		}

		wg.Add(1)
		go func(contract string) {
			defer wg.Done()

			// Create a temporary channel to collect participants
			tempCh := make(chan Participant, 100)
			tempErrCh := make(chan error, 1)

			// Create config for this specific contract
			singleCfg := cfg
			singleCfg.ContractAddress = contract
			singleCfg.QueryName = "nft_holders"

			// Stream holders for this contract in a goroutine that closes channels when done
			go func() {
				c.streamNFTHolders(ctx, network, singleCfg, tempCh, tempErrCh)
				close(tempCh)
				close(tempErrCh)
			}()

			// Collect results
			for {
				select {
				case participant, ok := <-tempCh:
					if !ok {
						// Channel closed, check for errors before returning
						select {
						case err := <-tempErrCh:
							if err != nil {
								errChan <- fmt.Errorf("error processing contract %s: %w", contract, err)
							}
						default:
							// No error, processing completed successfully
						}
						return
					}
					mu.Lock()
					if existing, exists := uniqueHolders[participant.Address]; exists {
						// Add balances for holders across multiple contracts
						uniqueHolders[participant.Address] = new(big.Int).Add(existing, participant.Balance)
					} else {
						uniqueHolders[participant.Address] = participant.Balance
					}
					mu.Unlock()
				case err := <-tempErrCh:
					if err != nil {
						errChan <- fmt.Errorf("error processing contract %s: %w", contract, err)
					}
					// Continue processing even if there's an error on the error channel
					// The participant channel might still have data
				case <-ctx.Done():
					return
				}
			}
		}(contractAddr)
	}

	// Wait for all contracts to be processed
	log.Debug().
		Int("contracts", len(contractAddresses)).
		Msg("Waiting for all contracts to be processed")
	wg.Wait()

	log.Debug().
		Int("unique_holders_collected", len(uniqueHolders)).
		Msg("All contracts processed, checking for errors")

	close(errChan)

	// Check for errors
	hasErrors := false
	for err := range errChan {
		if err != nil {
			hasErrors = true
			errorCh <- err
		}
	}

	if hasErrors {
		log.Error().Msg("Errors encountered during multi-NFT processing, exiting")
		return
	}

	log.Debug().
		Int("unique_holders", len(uniqueHolders)).
		Msg("No errors found, sending unique holders to channel")

	// Send unique holders to the channel
	sentCount := 0
	for address, balance := range uniqueHolders {
		participant := Participant{
			Address: address,
			Balance: balance,
		}
		select {
		case participantCh <- participant:
			sentCount++
		case <-ctx.Done():
			log.Warn().
				Int("sent", sentCount).
				Int("total", len(uniqueHolders)).
				Msg("Context cancelled while sending participants")
			return
		}
	}

	log.Info().
		Int("unique_holders", len(uniqueHolders)).
		Int("sent_to_channel", sentCount).
		Int("contracts_processed", len(contractAddresses)).
		Msg("Multi-NFT holder query completed - all participants sent")
}

// makeRequest makes an HTTP request to Alchemy API
func (c *Client) makeRequest(ctx context.Context, method, url string, body io.Reader) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Warn().Err(closeErr).Msg("Failed to close response body")
		}
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	return respBody, nil
}

// calculateWeight calculates the weight for a given balance based on the weight configuration
func calculateWeight(balance int64, cfg Config) (int64, error) {
	balanceFloat := float64(balance)
	var weight int64

	switch cfg.WeightConfig.Strategy {
	case "constant":
		if cfg.WeightConfig.ConstantWeight == nil {
			return 0, fmt.Errorf("constant_weight is required for constant strategy")
		}
		weight = int64(*cfg.WeightConfig.ConstantWeight)

	case "proportional_auto":
		// Calculate weight based on target_min_weight
		// weight = (balance / min_balance) * target_min_weight
		if cfg.MinBalance <= 0 {
			return 0, fmt.Errorf("min_balance must be positive for proportional_auto strategy")
		}
		if cfg.WeightConfig.TargetMinWeight == nil {
			return 0, fmt.Errorf("target_min_weight is required for proportional_auto strategy")
		}
		ratio := balanceFloat / cfg.MinBalance
		weight = int64(ratio * float64(*cfg.WeightConfig.TargetMinWeight))

	case "proportional_manual":
		// Apply manual multiplier
		if cfg.WeightConfig.Multiplier == nil {
			return 0, fmt.Errorf("multiplier is required for proportional_manual strategy")
		}
		weight = int64(balanceFloat * *cfg.WeightConfig.Multiplier)

	default:
		return 0, fmt.Errorf("unknown weight strategy: %s", cfg.WeightConfig.Strategy)
	}

	// Apply max weight cap if specified
	if cfg.WeightConfig.MaxWeight != nil && weight > int64(*cfg.WeightConfig.MaxWeight) {
		weight = int64(*cfg.WeightConfig.MaxWeight)
	}

	// Ensure weight is at least 1 (no zero weights)
	if weight < 1 {
		weight = 1
	}

	return weight, nil
}
