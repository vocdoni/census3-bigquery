package censusdb

import (
	davincitypes "github.com/vocdoni/davinci-node/types"
)

// Re-export davinci-node types for convenience
type (
	HexBytes = davincitypes.HexBytes
	BigInt   = davincitypes.BigInt
)

// CensusProof is an alias to davinci-node's CensusProof type
type CensusProof = davincitypes.CensusProof
