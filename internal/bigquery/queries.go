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
			SELECT DISTINCT b.address, b.eth_balance
			FROM ` + "`bigquery-public-data.crypto_ethereum.balances`" + ` b
			JOIN ` + "`bigquery-public-data.crypto_ethereum.transactions`" + ` t
			ON (b.address = t.from_address OR b.address = t.to_address)
			WHERE b.eth_balance >= @min_balance
			AND t.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
			ORDER BY b.eth_balance DESC`,
		Parameters: []string{"min_balance"},
	},
	"erc20_holders": {
		Name:        "erc20_holders",
		Description: "Fetch ERC20 token holders above minimum balance threshold",
		SQL: `
			SELECT token_address as address, SUM(value) as eth_balance
			FROM ` + "`bigquery-public-data.crypto_ethereum.token_transfers`" + `
			WHERE token_address = @token_address
			GROUP BY token_address
			HAVING SUM(value) >= @min_balance
			ORDER BY SUM(value) DESC`,
		Parameters: []string{"token_address", "min_balance"},
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
