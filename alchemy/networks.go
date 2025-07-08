package alchemy

// Supported Alchemy networks
var Networks = map[string]NetworkInfo{
	"eth-mainnet": {
		Name:     "Ethereum Mainnet",
		Endpoint: "https://eth-mainnet.g.alchemy.com",
		ChainID:  1,
	},
	"eth-sepolia": {
		Name:     "Ethereum Sepolia",
		Endpoint: "https://eth-sepolia.g.alchemy.com",
		ChainID:  11155111,
	},
	"polygon-mainnet": {
		Name:     "Polygon Mainnet",
		Endpoint: "https://polygon-mainnet.g.alchemy.com",
		ChainID:  137,
	},
	"polygon-amoy": {
		Name:     "Polygon Amoy",
		Endpoint: "https://polygon-amoy.g.alchemy.com",
		ChainID:  80002,
	},
	"arb-mainnet": {
		Name:     "Arbitrum Mainnet",
		Endpoint: "https://arb-mainnet.g.alchemy.com",
		ChainID:  42161,
	},
	"arb-sepolia": {
		Name:     "Arbitrum Sepolia",
		Endpoint: "https://arb-sepolia.g.alchemy.com",
		ChainID:  421614,
	},
	"opt-mainnet": {
		Name:     "Optimism Mainnet",
		Endpoint: "https://opt-mainnet.g.alchemy.com",
		ChainID:  10,
	},
	"opt-sepolia": {
		Name:     "Optimism Sepolia",
		Endpoint: "https://opt-sepolia.g.alchemy.com",
		ChainID:  11155420,
	},
	"base-mainnet": {
		Name:     "Base Mainnet",
		Endpoint: "https://base-mainnet.g.alchemy.com",
		ChainID:  8453,
	},
	"base-sepolia": {
		Name:     "Base Sepolia",
		Endpoint: "https://base-sepolia.g.alchemy.com",
		ChainID:  84532,
	},
	"blast-mainnet": {
		Name:     "Blast Mainnet",
		Endpoint: "https://blast-mainnet.g.alchemy.com",
		ChainID:  81457,
	},
	"blast-sepolia": {
		Name:     "Blast Sepolia",
		Endpoint: "https://blast-sepolia.g.alchemy.com",
		ChainID:  168587773,
	},
	"linea-mainnet": {
		Name:     "Linea Mainnet",
		Endpoint: "https://linea-mainnet.g.alchemy.com",
		ChainID:  59144,
	},
	"linea-sepolia": {
		Name:     "Linea Sepolia",
		Endpoint: "https://linea-sepolia.g.alchemy.com",
		ChainID:  59141,
	},
	"zetachain-mainnet": {
		Name:     "ZetaChain Mainnet",
		Endpoint: "https://zetachain-mainnet.g.alchemy.com",
		ChainID:  7000,
	},
	"zetachain-testnet": {
		Name:     "ZetaChain Testnet",
		Endpoint: "https://zetachain-testnet.g.alchemy.com",
		ChainID:  7001,
	},
	"shape-mainnet": {
		Name:     "Shape Mainnet",
		Endpoint: "https://shape-mainnet.g.alchemy.com",
		ChainID:  360,
	},
}

// GetNetwork returns network information for a given network name
func GetNetwork(networkName string) (NetworkInfo, bool) {
	network, exists := Networks[networkName]
	return network, exists
}

// ListNetworks returns all available network names
func ListNetworks() []string {
	networks := make([]string, 0, len(Networks))
	for name := range Networks {
		networks = append(networks, name)
	}
	return networks
}

// IsValidNetwork checks if a network name is valid
func IsValidNetwork(networkName string) bool {
	_, exists := Networks[networkName]
	return exists
}
