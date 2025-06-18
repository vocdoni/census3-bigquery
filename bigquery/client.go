package bigquery

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/ethereum/go-ethereum/common"
	"google.golang.org/api/iterator"

	"census3-bigquery/log"
)

// BigQuery's column is NUMERIC (gwei), 1 ETH = 1 000 000 000 gwei.
var weiPerETH = big.NewRat(1_000_000_000_000_000_000, 1)

// weiToETH converts the NUMERIC coming from BigQuery to an ETH-denominated *big.Rat.
func weiToETH(w *big.Rat) *big.Rat { return new(big.Rat).Quo(w, weiPerETH) }

// ethToWei converts an ETH amount (from -min) to the wei NUMERIC BigQuery stores.
func ethToWei(e *big.Rat) *big.Rat { return new(big.Rat).Mul(e, weiPerETH) }

// fmtETH prints an *big.Rat with up to 18 decimals (full wei precision) without
// trailing zeros.
func fmtETH(r *big.Rat) string { return r.FloatString(18) }

type balanceRow struct {
	Address string   `bigquery:"address"`
	Balance *big.Rat `bigquery:"eth_balance"` // wei inside BigQuery
}

// Client wraps BigQuery operations
type Client struct {
	client *bigquery.Client
}

// Config holds BigQuery configuration
type Config struct {
	Project     string
	MinBalance  float64
	QueryName   string
	QueryParams map[string]interface{} // Additional query parameters
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

// FetchBalancesToCSV fetches balances from BigQuery using modular queries and saves them to a CSV file
func (c *Client) FetchBalancesToCSV(ctx context.Context, cfg Config, csvPath string) (int, error) {
	// Get the query from the registry
	query, err := GetQuery(cfg.QueryName)
	if err != nil {
		return 0, fmt.Errorf("failed to get query: %w", err)
	}

	// Validate minimum balance
	minEth, ok := new(big.Rat).SetString(fmt.Sprintf("%.18f", cfg.MinBalance))
	if !ok {
		return 0, fmt.Errorf("invalid minimum balance: %f", cfg.MinBalance)
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
		return 0, fmt.Errorf("query parameter validation failed: %w", err)
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

	log.Info().Str("query_name", cfg.QueryName).Interface("parameters", queryParams).Msg("Executing query")

	// Execute query
	q := c.client.Query(sql)
	q.Parameters = bqParams
	it, err := q.Read(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to execute query '%s': %w", cfg.QueryName, err)
	}

	// Create CSV file
	file, err := os.Create(csvPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Warn().Err(err).Msg("Failed to close CSV file")
		}
	}()

	// Write results to CSV
	count := 0
	for {
		var r balanceRow
		switch err := it.Next(&r); err {
		case iterator.Done:
			return count, nil
		case nil:
			// Write address,balance format
			line := fmt.Sprintf("%s,%s\n", r.Address, fmtETH(weiToETH(r.Balance)))
			if _, err := file.WriteString(line); err != nil {
				return count, fmt.Errorf("failed to write to CSV: %w", err)
			}
			count++
		default:
			return count, fmt.Errorf("iterator error: %w", err)
		}
	}
}

// FetchSingleAddress fetches balance for a specific address
func (c *Client) FetchSingleAddress(ctx context.Context, address string) (*balanceRow, error) {
	sql := `
		SELECT address, eth_balance
		FROM ` + "`bigquery-public-data.crypto_ethereum.balances`" + `
		WHERE address = @addr`

	params := []bigquery.QueryParameter{
		{Name: "addr", Value: strings.ToLower(address)},
	}

	q := c.client.Query(sql)
	q.Parameters = params
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	var r balanceRow
	switch err := it.Next(&r); err {
	case iterator.Done:
		return nil, fmt.Errorf("address not found: %s", address)
	case nil:
		return &r, nil
	default:
		return nil, fmt.Errorf("iterator error: %w", err)
	}
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

	log.Info().Str("query_name", cfg.QueryName).Interface("parameters", queryParams).Msg("Executing streaming query")

	// Execute query
	q := c.client.Query(sql)
	q.Parameters = bqParams
	it, err := q.Read(ctx)
	if err != nil {
		errorCh <- fmt.Errorf("failed to execute query '%s': %w", cfg.QueryName, err)
		return
	}

	// Stream results to channel
	for {
		var r balanceRow
		switch err := it.Next(&r); err {
		case iterator.Done:
			return
		case nil:
			// Convert to participant and send to channel
			if !common.IsHexAddress(r.Address) {
				log.Warn().Str("address", r.Address).Msg("Skipping invalid address format")
				continue
			}

			address := common.HexToAddress(r.Address)
			// Convert balance from wei to integer (multiply by 100 as in original code)
			balanceETH := weiToETH(r.Balance)
			balanceFloat, _ := balanceETH.Float64()
			balanceInt := big.NewInt(int64(balanceFloat * 100))

			participant := Participant{
				Address: address,
				Balance: balanceInt,
			}

			select {
			case participantCh <- participant:
				// Successfully sent
			case <-ctx.Done():
				errorCh <- ctx.Err()
				return
			}
		default:
			errorCh <- fmt.Errorf("iterator error: %w", err)
			return
		}
	}
}

// GenerateCSVFileName generates a timestamped CSV filename
func GenerateCSVFileName() string {
	timestamp := time.Now().Format("20060102_150405")
	return fmt.Sprintf("addresses_%s.csv", timestamp)
}
