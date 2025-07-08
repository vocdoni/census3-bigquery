package alchemy

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// Participant represents a census participant with address and balance
type Participant struct {
	Address common.Address
	Balance *big.Int
}

// NetworkInfo represents an Alchemy network configuration
type NetworkInfo struct {
	Name     string
	Endpoint string
	ChainID  int
}

// NFTOwner represents an NFT owner with their token balance
type NFTOwner struct {
	OwnerAddress  string `json:"ownerAddress"`
	TokenBalances []struct {
		TokenId string `json:"tokenId"`
		Balance string `json:"balance"`
	} `json:"tokenBalances"`
}

// OwnersResponse represents the response from Alchemy's getOwnersForContract API
type OwnersResponse struct {
	Owners       []NFTOwner `json:"owners"`
	TotalCount   int        `json:"totalCount"`
	PageKey      string     `json:"pageKey,omitempty"`
	ContractData struct {
		Name   string `json:"name"`
		Symbol string `json:"symbol"`
	} `json:"contractData"`
}

// WeightConfig represents weight calculation configuration
type WeightConfig struct {
	Strategy        string // "constant", "proportional_auto", "proportional_manual"
	ConstantWeight  *int
	TargetMinWeight *int
	Multiplier      *float64
	MaxWeight       *int
}

// Config holds Alchemy configuration
type Config struct {
	APIKey          string
	Network         string                 // Network name (e.g., "eth-mainnet", "base-mainnet")
	ContractAddress string                 // NFT contract address
	MinBalance      float64                // Minimum NFT balance
	QueryName       string                 // Query identifier
	QueryParams     map[string]interface{} // Additional query parameters
	WeightConfig    WeightConfig           // Weight calculation configuration
	PageSize        int                    // Results per page for pagination
}
