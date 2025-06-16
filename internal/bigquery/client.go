package bigquery

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
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
	Project    string
	MinBalance float64
	MaxCount   int
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

// FetchBalancesToCSV fetches balances from BigQuery and saves them to a CSV file
func (c *Client) FetchBalancesToCSV(ctx context.Context, cfg Config, csvPath string) (int, error) {
	// Validate minimum balance
	minEth, ok := new(big.Rat).SetString(fmt.Sprintf("%.18f", cfg.MinBalance))
	if !ok {
		return 0, fmt.Errorf("invalid minimum balance: %f", cfg.MinBalance)
	}

	// Build SQL query
	sql := `
		SELECT address, eth_balance
		FROM ` + "`bigquery-public-data.crypto_ethereum.balances`" + `
		WHERE eth_balance >= @min
		ORDER BY eth_balance DESC`

	params := []bigquery.QueryParameter{
		{Name: "min", Value: ethToWei(minEth)}, // compare apples-to-apples in wei
	}

	if cfg.MaxCount > 0 {
		sql += fmt.Sprintf("\n		LIMIT %d", cfg.MaxCount)
	}

	// Execute query
	q := c.client.Query(sql)
	q.Parameters = params
	it, err := q.Read(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to execute query: %w", err)
	}

	// Create CSV file
	file, err := os.Create(csvPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Warning: failed to close CSV file: %v", err)
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

// GenerateCSVFileName generates a timestamped CSV filename
func GenerateCSVFileName() string {
	timestamp := time.Now().Format("20060102_150405")
	return fmt.Sprintf("addresses_%s.csv", timestamp)
}
