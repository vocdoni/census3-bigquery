package bigquery

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/ethereum/go-ethereum/common"
	"google.golang.org/api/iterator"

	"census3-bigquery/log"
)

// BigQuery's column is NUMERIC (gwei), 1 ETH = 1 000 000 000 gwei.
var weiPerETH = big.NewRat(1_000_000_000_000_000_000, 1)

// ethToWei converts an ETH amount (from -min) to the wei NUMERIC BigQuery stores.
func ethToWei(e *big.Rat) *big.Rat { return new(big.Rat).Mul(e, weiPerETH) }

type balanceRow struct {
	Address string   `bigquery:"address"`
	Balance *big.Rat `bigquery:"eth_balance"` // wei inside BigQuery
}

// QueryEstimate holds the results of a BigQuery dry run estimation
type QueryEstimate struct {
	BytesProcessed   int64   `json:"bytes_processed"`
	EstimatedCostUSD float64 `json:"estimated_cost_usd,omitempty"`
	IsValid          bool    `json:"is_valid"`
	ValidationError  error   `json:"validation_error,omitempty"`
	QuerySQL         string  `json:"query_sql,omitempty"`
}

// CostThresholds defines limits for query execution (internal use)
type CostThresholds struct {
	MaxBytesProcessed   *int64            `json:"max_bytes_processed,omitempty"`
	MaxEstimatedCostUSD *float64          `json:"max_estimated_cost_usd,omitempty"`
	WarnThresholdBytes  *int64            `json:"warn_threshold_bytes,omitempty"`
	MaxBytesBilled      *int64            `json:"max_bytes_billed,omitempty"` // BigQuery job-level safety limit
	UseResultCache      *bool             `json:"use_result_cache,omitempty"` // Enable/disable result caching
	JobLabels           map[string]string `json:"job_labels,omitempty"`       // Labels for job tracking
}

// BigQueryPricing holds pricing information for cost estimation
type BigQueryPricing struct {
	PricePerTBProcessed float64 `json:"price_per_tb_processed"`
}

// Client wraps BigQuery operations
type Client struct {
	client *bigquery.Client
}

// Config holds BigQuery configuration
type Config struct {
	Project         string
	MinBalance      float64
	QueryName       string
	QueryParams     map[string]interface{} // Additional query parameters
	Decimals        int                    // Token decimals for conversion
	WeightConfig    WeightConfig           // Weight calculation configuration
	EstimateFirst   bool                   // Whether to estimate query cost before execution
	CostPreset      string                 // Simple cost preset: "conservative", "default", "high_volume", "none"
	BigQueryPricing *BigQueryPricing       // Pricing information for cost estimation (internal use)
}

// WeightConfig represents weight calculation configuration
type WeightConfig struct {
	Strategy        string // "constant", "proportional_auto", "proportional_manual"
	ConstantWeight  *int
	TargetMinWeight *int
	Multiplier      *float64
	MaxWeight       *int
}

// NewClient creates a new BigQuery client
func NewClient(ctx context.Context, projectID string) (*Client, error) {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	return &Client{client: client}, nil
}

// Close closes the BigQuery client
func (c *Client) Close() error {
	return c.client.Close()
}

// Participant represents a census participant with address and balance
type Participant struct {
	Address common.Address
	Balance *big.Int
}

// StreamBalances streams balance data from BigQuery to a channel
func (c *Client) StreamBalances(ctx context.Context, cfg Config, participantCh chan<- Participant, errorCh chan<- error) {
	defer close(participantCh)
	defer close(errorCh)

	// Get the query from the registry
	query, err := GetQuery(cfg.QueryName)
	if err != nil {
		errorCh <- fmt.Errorf("failed to get query: %w", err)
		return
	}

	// Validate minimum balance
	minEth, ok := new(big.Rat).SetString(fmt.Sprintf("%.18f", cfg.MinBalance))
	if !ok {
		errorCh <- fmt.Errorf("invalid minimum balance: %f", cfg.MinBalance)
		return
	}

	// Prepare query parameters
	queryParams := map[string]interface{}{
		"min_balance": ethToWei(minEth), // compare apples-to-apples in wei
	}

	// Add any additional query parameters
	for key, value := range cfg.QueryParams {
		queryParams[key] = value
	}

	// Validate that all required parameters are provided
	if err := ValidateQueryParameters(query, queryParams); err != nil {
		errorCh <- fmt.Errorf("query parameter validation failed: %w", err)
		return
	}

	// Process the SQL template (no LIMIT functionality)
	sql := ProcessQueryTemplate(query.SQL, false)

	// Convert parameters to BigQuery format
	var bqParams []bigquery.QueryParameter
	for key, value := range queryParams {
		bqParams = append(bqParams, bigquery.QueryParameter{
			Name:  key,
			Value: value,
		})
	}

	// Resolve cost thresholds from simple preset
	resolvedThresholds := ResolveCostPreset(cfg.CostPreset)

	// Estimate query cost if requested
	if cfg.EstimateFirst {
		log.Info().Str("query_name", cfg.QueryName).Msg("Estimating streaming query cost before execution")

		estimate, err := c.EstimateQuerySize(ctx, sql, bqParams, cfg.BigQueryPricing)
		if err != nil {
			errorCh <- fmt.Errorf("failed to estimate query cost: %w", err)
			return
		}

		// Log estimation results
		log.Info().
			Int64("bytes_processed", estimate.BytesProcessed).
			Str("bytes_formatted", formatBytes(estimate.BytesProcessed)).
			Float64("estimated_cost_usd", estimate.EstimatedCostUSD).
			Bool("is_valid", estimate.IsValid).
			Str("query_name", cfg.QueryName).
			Msg("Streaming query cost estimation completed")

		// Check if query is valid
		if !estimate.IsValid {
			errorCh <- fmt.Errorf("query validation failed: %w", estimate.ValidationError)
			return
		}

		// Check cost thresholds using resolved thresholds
		if err := c.checkCostThresholds(estimate, resolvedThresholds); err != nil {
			errorCh <- err
			return
		}
	}

	log.Info().Str("query_name", cfg.QueryName).Interface("parameters", queryParams).Msg("Executing streaming query")

	// Execute query with enhanced cost controls
	q := c.client.Query(sql)
	q.Parameters = bqParams

	// Apply cost control settings from resolved thresholds
	if resolvedThresholds.MaxBytesBilled != nil {
		q.MaxBytesBilled = *resolvedThresholds.MaxBytesBilled
	}

	// Set result cache preference (default to true for cost savings)
	if resolvedThresholds.UseResultCache != nil && !*resolvedThresholds.UseResultCache {
		q.DisableQueryCache = true
	}

	// Add job labels for tracking and cost attribution
	jobLabels := make(map[string]string)
	if resolvedThresholds.JobLabels != nil {
		for k, v := range resolvedThresholds.JobLabels {
			jobLabels[k] = v
		}
	}

	// Add default labels for cost tracking
	jobLabels["query_type"] = cfg.QueryName
	jobLabels["cost_optimized"] = "true"
	jobLabels["created_by"] = "census3-bigquery"
	q.Labels = jobLabels

	// Location must match dataset location (US for public datasets)
	q.Location = "US"

	it, err := q.Read(ctx)
	if err != nil {
		errorCh <- fmt.Errorf("failed to execute query '%s': %w", cfg.QueryName, err)
		return
	}

	// Create channels for parallel processing
	const maxWorkers = 10
	balanceRowCh := make(chan balanceRow, maxWorkers*2) // Buffer to prevent blocking

	// Use sync.WaitGroup to wait for all workers to complete
	var wg sync.WaitGroup

	// Start worker goroutines for parallel processing
	for range maxWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.processBalanceRows(ctx, balanceRowCh, participantCh, errorCh, cfg)
		}()
	}

	// Read from iterator and send to workers (iterator must be read sequentially)
	go func() {
		defer close(balanceRowCh)
		for {
			var r balanceRow
			switch err := it.Next(&r); err {
			case iterator.Done:
				return
			case nil:
				select {
				case balanceRowCh <- r:
					// Successfully sent to workers
				case <-ctx.Done():
					return
				}
			default:
				select {
				case errorCh <- fmt.Errorf("iterator error: %w", err):
				case <-ctx.Done():
				}
				return
			}
		}
	}()

	// Wait for all workers to complete
	wg.Wait()
}

// processBalanceRows processes balance rows from a channel and sends participants to output channel
func (c *Client) processBalanceRows(ctx context.Context, balanceRowCh <-chan balanceRow, participantCh chan<- Participant, errorCh chan<- error, cfg Config) {
	for {
		select {
		case r, ok := <-balanceRowCh:
			if !ok {
				// Channel closed, worker should exit
				return
			}

			// Convert to participant and send to channel
			if !common.IsHexAddress(r.Address) {
				log.Warn().Str("address", r.Address).Msg("Skipping invalid address format")
				continue
			}

			address := common.HexToAddress(r.Address)

			// Calculate weight based on configuration
			weight, err := calculateWeight(r.Balance, cfg)
			if err != nil {
				log.Warn().Err(err).Str("address", r.Address).Msg("Failed to calculate weight, skipping")
				continue
			}

			participant := Participant{
				Address: address,
				Balance: big.NewInt(weight),
			}

			select {
			case participantCh <- participant:
				// Successfully sent
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

// calculateWeight calculates the weight for a given balance based on the weight configuration
func calculateWeight(balance *big.Rat, cfg Config) (int64, error) {
	// Convert balance from wei to human-readable units using decimals
	decimalsMultiplier := new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(cfg.Decimals)), nil))
	humanBalance := new(big.Rat).Quo(balance, decimalsMultiplier)
	humanBalanceFloat, _ := humanBalance.Float64()

	var weight int64

	switch cfg.WeightConfig.Strategy {
	case "constant":
		weight = int64(*cfg.WeightConfig.ConstantWeight)

	case "proportional_auto":
		// Calculate weight based on target_min_weight
		// weight = (balance / min_balance) * target_min_weight
		if cfg.MinBalance <= 0 {
			return 0, fmt.Errorf("min_balance must be positive for proportional_auto strategy")
		}
		ratio := humanBalanceFloat / cfg.MinBalance
		weight = int64(ratio * float64(*cfg.WeightConfig.TargetMinWeight))

	case "proportional_manual":
		// Apply manual multiplier
		weight = int64(humanBalanceFloat * *cfg.WeightConfig.Multiplier)

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

// Error variables for cost control
var (
	ErrQueryTooExpensive = fmt.Errorf("query exceeds cost thresholds")
	ErrEstimationFailed  = fmt.Errorf("failed to estimate query cost")
)

// EstimateQuerySize performs a dry run to estimate query processing requirements
func (c *Client) EstimateQuerySize(ctx context.Context, sql string, params []bigquery.QueryParameter, pricing *BigQueryPricing) (*QueryEstimate, error) {
	// Create a query job with dry run enabled
	q := c.client.Query(sql)
	q.Parameters = params
	q.DryRun = true
	// Location must match that of the dataset(s) referenced in the query
	// BigQuery public datasets are in US location
	q.Location = "US"

	// Run the dry run
	job, err := q.Run(ctx)
	if err != nil {
		// Return invalid estimate with error details instead of failing completely
		return &QueryEstimate{
			IsValid:         false,
			ValidationError: fmt.Errorf("failed to start dry run: %w", err),
			QuerySQL:        sql,
		}, nil
	}

	// Dry run is not asynchronous, so get the latest status and statistics directly
	status := job.LastStatus()
	if err := status.Err(); err != nil {
		return &QueryEstimate{
			IsValid:         false,
			ValidationError: err,
			QuerySQL:        sql,
		}, nil
	}

	// Get job statistics
	if status.Statistics == nil {
		return &QueryEstimate{
			IsValid:         false,
			ValidationError: fmt.Errorf("no statistics available from dry run"),
			QuerySQL:        sql,
		}, nil
	}

	// Extract bytes processed directly from statistics
	bytesProcessed := status.Statistics.TotalBytesProcessed

	// Calculate estimated cost
	var estimatedCost float64
	if pricing != nil {
		estimatedCost = calculateEstimatedCost(bytesProcessed, pricing)
	}

	return &QueryEstimate{
		BytesProcessed:   bytesProcessed,
		EstimatedCostUSD: estimatedCost,
		IsValid:          true,
		ValidationError:  nil,
		QuerySQL:         sql,
	}, nil
}

// checkCostThresholds validates if a query estimate is within acceptable limits
func (c *Client) checkCostThresholds(estimate *QueryEstimate, thresholds CostThresholds) error {
	// Check maximum bytes processed
	if thresholds.MaxBytesProcessed != nil && estimate.BytesProcessed > *thresholds.MaxBytesProcessed {
		return fmt.Errorf("%w: query would process %s, exceeds limit of %s",
			ErrQueryTooExpensive,
			formatBytes(estimate.BytesProcessed),
			formatBytes(*thresholds.MaxBytesProcessed))
	}

	// Check maximum estimated cost
	if thresholds.MaxEstimatedCostUSD != nil && estimate.EstimatedCostUSD > *thresholds.MaxEstimatedCostUSD {
		return fmt.Errorf("%w: estimated cost $%.4f exceeds limit of $%.4f",
			ErrQueryTooExpensive,
			estimate.EstimatedCostUSD,
			*thresholds.MaxEstimatedCostUSD)
	}

	// Log warning if above warning threshold
	if thresholds.WarnThresholdBytes != nil && estimate.BytesProcessed > *thresholds.WarnThresholdBytes {
		log.Warn().
			Str("bytes_processed", formatBytes(estimate.BytesProcessed)).
			Str("bytes_formatted", formatBytes(estimate.BytesProcessed)).
			Float64("estimated_cost_usd", estimate.EstimatedCostUSD).
			Msg("Query will process large amount of data")
	}

	return nil
}

// formatBytes converts bytes to human-readable format (KB, MB, GB, TB)
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// calculateEstimatedCost computes USD cost from bytes processed
func calculateEstimatedCost(bytesProcessed int64, pricing *BigQueryPricing) float64 {
	if pricing == nil {
		pricing = getDefaultPricing()
	}

	// Convert bytes to TB (1 TB = 1024^4 bytes)
	tbProcessed := float64(bytesProcessed) / (1024.0 * 1024.0 * 1024.0 * 1024.0)
	return tbProcessed * pricing.PricePerTBProcessed
}

// getDefaultPricing returns default BigQuery pricing
func getDefaultPricing() *BigQueryPricing {
	return &BigQueryPricing{
		PricePerTBProcessed: 5,
	}
}

// Int64Ptr returns a pointer to an int64 value (utility function)
func Int64Ptr(v int64) *int64 {
	return &v
}

// Float64Ptr returns a pointer to a float64 value (utility function)
func Float64Ptr(v float64) *float64 {
	return &v
}

// GenerateCSVFileName generates a timestamped CSV filename
func GenerateCSVFileName() string {
	timestamp := time.Now().Format("20060102_150405")
	return fmt.Sprintf("addresses_%s.csv", timestamp)
}

// NewDefaultCostThresholds returns cost thresholds optimized for typical usage
func NewDefaultCostThresholds() CostThresholds {
	return CostThresholds{
		MaxBytesProcessed:   Int64Ptr(100 * 1024 * 1024 * 1024), // 100 GB limit
		MaxEstimatedCostUSD: Float64Ptr(5.0),                    // $5 USD limit
		WarnThresholdBytes:  Int64Ptr(10 * 1024 * 1024 * 1024),  // Warn at 10 GB
		MaxBytesBilled:      Int64Ptr(200 * 1024 * 1024 * 1024), // Hard limit at 200 GB
		UseResultCache:      BoolPtr(true),                      // Enable caching for cost savings
		JobLabels: map[string]string{
			"environment": "production",
			"cost_tier":   "standard",
		},
	}
}

// NewConservativeCostThresholds returns very strict cost thresholds for development/testing
func NewConservativeCostThresholds() CostThresholds {
	return CostThresholds{
		MaxBytesProcessed:   Int64Ptr(1 * 1024 * 1024 * 1024), // 1 GB limit
		MaxEstimatedCostUSD: Float64Ptr(0.10),                 // $0.10 USD limit
		WarnThresholdBytes:  Int64Ptr(100 * 1024 * 1024),      // Warn at 100 MB
		MaxBytesBilled:      Int64Ptr(2 * 1024 * 1024 * 1024), // Hard limit at 2 GB
		UseResultCache:      BoolPtr(true),                    // Enable caching
		JobLabels: map[string]string{
			"environment": "development",
			"cost_tier":   "conservative",
		},
	}
}

// NewHighVolumeCostThresholds returns cost thresholds for high-volume production usage
func NewHighVolumeCostThresholds() CostThresholds {
	return CostThresholds{
		MaxBytesProcessed:   Int64Ptr(1024 * 1024 * 1024 * 1024),     // 1 TB limit
		MaxEstimatedCostUSD: Float64Ptr(50.0),                        // $50 USD limit
		WarnThresholdBytes:  Int64Ptr(100 * 1024 * 1024 * 1024),      // Warn at 100 GB
		MaxBytesBilled:      Int64Ptr(2 * 1024 * 1024 * 1024 * 1024), // Hard limit at 2 TB
		UseResultCache:      BoolPtr(true),                           // Enable caching
		JobLabels: map[string]string{
			"environment": "production",
			"cost_tier":   "high_volume",
		},
	}
}

// NewOptimizedConfig returns a Config with cost optimization defaults enabled
func NewOptimizedConfig(project, queryName string, minBalance float64) Config {
	return Config{
		Project:         project,
		MinBalance:      minBalance,
		QueryName:       queryName,
		QueryParams:     make(map[string]interface{}),
		Decimals:        18, // Default for ETH
		WeightConfig:    WeightConfig{Strategy: "constant", ConstantWeight: IntPtr(1)},
		EstimateFirst:   true, // Always estimate first for cost control
		CostPreset:      "default",
		BigQueryPricing: getDefaultPricing(),
	}
}

// BoolPtr returns a pointer to a bool value (utility function)
func BoolPtr(v bool) *bool {
	return &v
}

// IntPtr returns a pointer to an int value (utility function)
func IntPtr(v int) *int {
	return &v
}

// GetMonthlyFreeTierRemaining estimates remaining free tier capacity (1TB per month)
func GetMonthlyFreeTierRemaining(bytesUsedThisMonth int64) int64 {
	freeTierBytes := int64(1024 * 1024 * 1024 * 1024) // 1 TB in bytes
	remaining := freeTierBytes - bytesUsedThisMonth
	if remaining < 0 {
		return 0
	}
	return remaining
}

// EstimateMonthlyCost calculates estimated monthly cost based on usage patterns
func EstimateMonthlyCost(avgDailyBytes int64, pricing *BigQueryPricing) float64 {
	if pricing == nil {
		pricing = getDefaultPricing()
	}

	monthlyBytes := avgDailyBytes * 30
	freeTierBytes := int64(1024 * 1024 * 1024 * 1024) // 1 TB free tier

	billableBytes := monthlyBytes - freeTierBytes
	if billableBytes <= 0 {
		return 0.0 // Within free tier
	}

	return calculateEstimatedCost(billableBytes, pricing)
}

// ResolveCostPreset resolves a simple cost preset string to full cost thresholds
func ResolveCostPreset(preset string) CostThresholds {
	switch preset {
	case "conservative":
		return NewConservativeCostThresholds()
	case "default", "standard", "":
		return NewDefaultCostThresholds()
	case "high_volume", "enterprise":
		return NewHighVolumeCostThresholds()
	case "none":
		// No cost limits
		return CostThresholds{
			UseResultCache: BoolPtr(true), // Still enable caching for performance
			JobLabels: map[string]string{
				"cost_tier": "unlimited",
			},
		}
	default:
		// Unknown preset, use default
		return NewDefaultCostThresholds()
	}
}
