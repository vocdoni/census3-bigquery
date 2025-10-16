package censusdb

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"

	davincitypes "github.com/vocdoni/davinci-node/types"
)

// CensusProof is the struct to represent a proof of inclusion in the census
// merkle tree. For example, it will be provided by the user to verify that he
// or she can vote in the process.
type CensusProof struct {
	// Generic fields
	CensusOrigin davincitypes.CensusOrigin `json:"censusOrigin"`
	Root         HexBytes                  `json:"root"`
	Address      HexBytes                  `json:"address"`
	Weight       *BigInt                   `json:"weight,omitempty"`
	// Merkletree related fields
	Siblings HexBytes `json:"siblings,omitempty"`
	Value    HexBytes `json:"value,omitempty"`
	Index    uint64   `json:"index,omitempty"`
	// CSP related fields
	ProcessID HexBytes `json:"processId,omitempty"`
	PublicKey HexBytes `json:"publicKey,omitempty"`
	Signature HexBytes `json:"signature,omitempty"`
}

// Valid checks that the CensusProof is well-formed
func (cp *CensusProof) Valid() bool {
	if cp == nil || cp.Root == nil && cp.Address == nil && cp.Value != nil &&
		cp.Siblings != nil && cp.Weight != nil {
		return false
	}
	switch cp.CensusOrigin {
	case davincitypes.CensusOriginMerkleTree:
		return cp.Root != nil && cp.Address != nil && cp.Value != nil &&
			cp.Siblings != nil && cp.Weight != nil
	case davincitypes.CensusOriginCSPEdDSABLS12377:
		return cp.Root != nil && cp.Address != nil && cp.ProcessID != nil &&
			cp.PublicKey != nil && cp.Signature != nil
	default:
		return false
	}
}

// String returns a string representation of the CensusProof
// in JSON format. It returns an empty string if the JSON marshaling fails.
func (cp *CensusProof) String() string {
	data, err := json.Marshal(cp)
	if err != nil {
		return ""
	}
	return string(data)
}

// HexBytes is a []byte which encodes as hexadecimal in json, as opposed to the
// base64 default.
type HexBytes []byte

func (b *HexBytes) String() string {
	return "0x" + hex.EncodeToString(*b)
}

func (b *HexBytes) BigInt() *BigInt {
	return new(BigInt).SetBytes(*b)
}

func (b HexBytes) MarshalJSON() ([]byte, error) {
	enc := make([]byte, hex.EncodedLen(len(b))+4)
	enc[0] = '"'
	enc[1] = '0'
	enc[2] = 'x'
	hex.Encode(enc[3:], b)
	enc[len(enc)-1] = '"'
	return enc, nil
}

func (b *HexBytes) UnmarshalJSON(data []byte) error {
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return fmt.Errorf("invalid JSON string: %q", data)
	}
	data = data[1 : len(data)-1]

	// Strip a leading "0x" prefix, for backwards compatibility.
	if len(data) >= 2 && data[0] == '0' && (data[1] == 'x' || data[1] == 'X') {
		data = data[2:]
	}

	decLen := hex.DecodedLen(len(data))
	if cap(*b) < decLen {
		*b = make([]byte, decLen)
	}
	if _, err := hex.Decode(*b, data); err != nil {
		return err
	}
	return nil
}

// BigInt is a big.Int wrapper which marshals JSON to a string representation of
// the big number. Note that a nil pointer value marshals as the empty string.
type BigInt big.Int

// MarshalText returns the decimal string representation of the big number.
// If the receiver is nil, we return "0".
func (i *BigInt) MarshalText() ([]byte, error) {
	if i == nil {
		return []byte("0"), nil
	}
	return (*big.Int)(i).MarshalText()
}

// UnmarshalText parses the text representation into the big number.
func (i *BigInt) UnmarshalText(data []byte) error {
	if i == nil {
		return fmt.Errorf("cannot unmarshal into nil BigInt")
	}
	return (*big.Int)(i).UnmarshalText(data)
}

// UnmarshalJSON implements the json.Unmarshaler interface.
// It supports both string and numeric JSON representations.
func (i *BigInt) UnmarshalJSON(data []byte) error {
	if i == nil {
		return fmt.Errorf("cannot unmarshal into nil BigInt")
	}

	// If it's a string representation (with double quotes)
	if len(data) > 0 && data[0] == '"' {
		// Remove the quotes and use UnmarshalText
		return i.UnmarshalText(data[1 : len(data)-1])
	}

	// If it's a numeric representation (without quotes)
	return i.UnmarshalText(data)
}

// String returns the string representation of the big number
func (i *BigInt) String() string {
	return (*big.Int)(i).String()
}

// SetBytes interprets buf as big-endian unsigned integer
func (i *BigInt) SetBytes(buf []byte) *BigInt {
	return (*BigInt)(i.MathBigInt().SetBytes(buf))
}

// Bytes returns the bytes representation of the big number
func (i *BigInt) Bytes() []byte {
	return (*big.Int)(i).Bytes()
}

// MathBigInt converts b to a math/big *Int.
func (i *BigInt) MathBigInt() *big.Int {
	return (*big.Int)(i)
}
