package bigquery

import (
	"fmt"
	"strings"
)

// Query represents a BigQuery query definition
type Query struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	SQL         string   `json:"sql"`
	Parameters  []string `json:"parameters"`
}

// QueryRegistry contains all available BigQuery queries
var QueryRegistry = map[string]Query{
	"ethereum_balances": {
		Name:        "ethereum_balances",
		Description: "Fetch Ethereum addresses with balance above minimum threshold",
		SQL: `			
			SELECT address, eth_balance
			FROM ` + "`bigquery-public-data.crypto_ethereum.balances`" + `
			WHERE eth_balance >= @min_balance
			ORDER BY eth_balance DESC`,
		Parameters: []string{"min_balance"},
	},
	"ethereum_balances_recent": {
		Name:        "ethereum_balances_recent",
		Description: "Fetch Ethereum addresses with recent activity and balance above threshold",
		SQL: `
			-- Cost-optimized query with partition pruning on transactions table only
			DECLARE snap_date DATE DEFAULT CURRENT_DATE();
			DECLARE lookback_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);
			
			WITH recent_active_addresses AS (
				SELECT DISTINCT 
					CASE 
						WHEN from_address IS NOT NULL THEN from_address
						WHEN to_address IS NOT NULL THEN to_address
					END AS address
				FROM ` + "`bigquery-public-data.crypto_ethereum.transactions`" + `
				WHERE DATE(block_timestamp) >= lookback_date
				  AND DATE(block_timestamp) <= snap_date
				  AND (from_address IS NOT NULL OR to_address IS NOT NULL)
			)
			SELECT b.address, b.eth_balance
			FROM ` + "`bigquery-public-data.crypto_ethereum.balances`" + ` b
			INNER JOIN recent_active_addresses r ON b.address = r.address
			WHERE b.eth_balance >= @min_balance
			ORDER BY b.eth_balance DESC`,
		Parameters: []string{"min_balance"},
	},
	"erc20_holders": {
		Name:        "erc20_holders",
		Description: "Fetch ERC20 token holders above minimum balance threshold",
		SQL: `
			-- 1. Fetch the contract's creation (genesis) block once
			DECLARE genesis_block_number INT64 DEFAULT (
				SELECT MIN(block_number)
				FROM ` + "`bigquery-public-data.crypto_ethereum.contracts`" + `
				WHERE address = LOWER(@token_address)  -- contracts table stores mixed-case; LOWER() is harmless
			);
			
			-- 2. Aggregate inbound transfers from that block forward 
			WITH token_aggregates AS (
				SELECT
					to_address AS address,
					SUM(SAFE_CAST(quantity AS NUMERIC)) AS token_balance
				FROM
					` + "`bigquery-public-data.goog_blockchain_ethereum_mainnet_us.token_transfers`" + `
				WHERE
					address = LOWER(@token_address)  -- token column is called "address"; dataset stores it in lowercase
					AND to_address IS NOT NULL
					AND block_number >= genesis_block_number
				GROUP BY
					to_address
			)
			
			-- 3. Return holders ≥ threshold
			SELECT
				address,
				token_balance AS eth_balance  -- keep original aliasing for compatibility
			FROM
				token_aggregates
			WHERE
				token_balance >= @min_balance
				AND token_balance IS NOT NULL
			ORDER BY
				token_balance DESC`,
		Parameters: []string{"token_address", "min_balance"},
	},
	"ethereum_recent_activity": {
		Name:        "ethereum_recent_activity",
		Description: "Fetch Ethereum addresses with activity in the last 90 days (no balance requirement)",
		SQL: `
			-- Cost-optimized query for recent activity without balance checks
			DECLARE snap_date DATE DEFAULT CURRENT_DATE();
			DECLARE lookback_date DATE DEFAULT DATE_SUB(snap_date, INTERVAL 90 DAY);

			WITH recent_active_addresses AS (
				SELECT DISTINCT
					CASE
						WHEN from_address IS NOT NULL THEN from_address
						ELSE to_address
					END AS address
				FROM ` + "`bigquery-public-data.goog_blockchain_ethereum_mainnet_us.transactions`" + `
				WHERE DATE(block_timestamp) BETWEEN lookback_date AND snap_date
					AND (from_address IS NOT NULL OR to_address IS NOT NULL)
			)

			SELECT
				address,
				CAST(1 AS NUMERIC) AS eth_balance          -- placeholder value; balances not queried
			FROM
				recent_active_addresses
			ORDER BY
				address`,
		Parameters: []string{},
	},
}

// GetQuery retrieves a query by name from the registry
func GetQuery(name string) (Query, error) {
	query, exists := QueryRegistry[name]
	if !exists {
		return Query{}, fmt.Errorf("query '%s' not found in registry", name)
	}
	return query, nil
}

// ListQueries returns all available query names and descriptions
func ListQueries() map[string]string {
	queries := make(map[string]string)
	for name, query := range QueryRegistry {
		queries[name] = query.Description
	}
	return queries
}

// PrintAvailableQueries prints all available queries to stdout
func PrintAvailableQueries() {
	fmt.Println("Available BigQuery queries:")
	fmt.Println()

	for name, query := range QueryRegistry {
		fmt.Printf("  %s:\n", name)
		fmt.Printf("    Description: %s\n", query.Description)
		fmt.Printf("    Parameters: %v\n", query.Parameters)
		fmt.Println()
	}
}

// ProcessQueryTemplate processes template placeholders in the SQL query
func ProcessQueryTemplate(sql string, hasLimit bool) string {
	// Handle LIMIT clause template
	limitClause := ""
	if hasLimit {
		limitClause = "LIMIT @limit"
	}

	// Replace template placeholders
	processedSQL := strings.ReplaceAll(sql, "{{LIMIT_CLAUSE}}", limitClause)

	// Clean up extra whitespace and newlines
	lines := strings.Split(processedSQL, "\n")
	var cleanLines []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			cleanLines = append(cleanLines, trimmed)
		}
	}

	return strings.Join(cleanLines, "\n")
}

// ValidateQueryParameters checks if all required parameters are provided
func ValidateQueryParameters(query Query, providedParams map[string]interface{}) error {
	for _, requiredParam := range query.Parameters {
		if _, exists := providedParams[requiredParam]; !exists {
			return fmt.Errorf("required parameter '%s' not provided for query '%s'", requiredParam, query.Name)
		}
	}
	return nil
}
