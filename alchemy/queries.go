package alchemy

import (
	"fmt"
	"strings"
)

// Query represents an Alchemy query definition
type Query struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Endpoint    string   `json:"endpoint"`
	Method      string   `json:"method"`
	Parameters  []string `json:"parameters"`
}

// QueryRegistry contains all available Alchemy queries
var QueryRegistry = map[string]Query{
	"nft_holders": {
		Name:        "nft_holders",
		Description: "Fetch NFT holders for a specific contract",
		Endpoint:    "/nft/v3/{apiKey}/getOwnersForContract",
		Method:      "GET",
		Parameters:  []string{"contract_address", "min_balance"},
	},
	"nft_holders_with_metadata": {
		Name:        "nft_holders_with_metadata",
		Description: "Fetch NFT holders with token metadata",
		Endpoint:    "/nft/v3/{apiKey}/getOwnersForContract",
		Method:      "GET",
		Parameters:  []string{"contract_address", "min_balance"},
	},
	"erc20_holders": {
		Name:        "erc20_holders",
		Description: "Fetch ERC20 token holders (using token balances)",
		Endpoint:    "/v2/{apiKey}",
		Method:      "POST",
		Parameters:  []string{"contract_address", "min_balance"},
	},
	"multi_nft_holders": {
		Name:        "multi_nft_holders",
		Description: "Fetch holders across multiple NFT contracts",
		Endpoint:    "/nft/v3/{apiKey}/getOwnersForContract",
		Method:      "GET",
		Parameters:  []string{"contract_addresses", "min_balance"},
	},
}

// GetQuery retrieves a query by name from the registry
func GetQuery(name string) (Query, error) {
	query, exists := QueryRegistry[name]
	if !exists {
		return Query{}, fmt.Errorf("query '%s' not found in Alchemy registry", name)
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
	fmt.Println("Available Alchemy queries:")
	fmt.Println()

	for name, query := range QueryRegistry {
		fmt.Printf("  %s:\n", name)
		fmt.Printf("    Description: %s\n", query.Description)
		fmt.Printf("    Method: %s\n", query.Method)
		fmt.Printf("    Parameters: %v\n", query.Parameters)
		fmt.Println()
	}
}

// ValidateQueryParameters checks if all required parameters are provided
func ValidateQueryParameters(query Query, providedParams map[string]interface{}) error {
	for _, requiredParam := range query.Parameters {
		// Special handling for contract_addresses (plural) when contract_address is provided
		if requiredParam == "contract_addresses" {
			if _, hasPlural := providedParams["contract_addresses"]; hasPlural {
				continue
			}
			if _, hasSingular := providedParams["contract_address"]; hasSingular {
				continue
			}
			return fmt.Errorf("required parameter '%s' not provided for query '%s'", requiredParam, query.Name)
		}

		if _, exists := providedParams[requiredParam]; !exists {
			// min_balance is optional for some queries
			if requiredParam == "min_balance" {
				continue
			}
			return fmt.Errorf("required parameter '%s' not provided for query '%s'", requiredParam, query.Name)
		}
	}
	return nil
}

// BuildEndpointURL builds the full endpoint URL for a query
func BuildEndpointURL(network NetworkInfo, apiKey string, query Query) string {
	endpoint := strings.ReplaceAll(query.Endpoint, "{apiKey}", apiKey)
	return network.Endpoint + endpoint
}
