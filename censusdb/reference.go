package censusdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vocdoni/arbo"
	"github.com/vocdoni/davinci-node/db"
)

// CensusRef is a reference to a census. It holds the Merkle tree.
// All accesses to the underlying tree (and its currentRoot) are protected by treeMu.
type CensusRef struct {
	ID          uuid.UUID
	MaxLevels   int
	HashType    string
	LastUsed    time.Time
	currentRoot []byte
	tree        *arbo.Tree  `gob:"-"`
	db          db.Database `gob:"-"` // The database where the census is stored.
	// treeMu protects all access to the underlying Merkle tree.
	treeMu sync.Mutex `gob:"-"`
}

// Tree returns the underlying arbo.Tree pointer.
// (Not concurrency‑safe; use Insert, Root, or GenProof.)
func (cr *CensusRef) Tree() *arbo.Tree {
	return cr.tree
}

// SetTree sets the arbo.Tree pointer.
func (cr *CensusRef) SetTree(tree *arbo.Tree) {
	cr.tree = tree
}

// Insert safely inserts a key/value pair into the Merkle tree.
// It holds treeMu during the Add and Root calls.
func (cr *CensusRef) Insert(key, value []byte) error {
	cr.treeMu.Lock()
	defer cr.treeMu.Unlock()

	err := cr.tree.Add(key, value)
	if err != nil {
		return err
	}

	// Update the current root
	newRoot, err := cr.tree.Root()
	if err != nil {
		return err
	}
	cr.currentRoot = newRoot

	return nil
}

// InsertBatch safely inserts a batch of key/value pairs into the Merkle tree.
func (cr *CensusRef) InsertBatch(keys, values [][]byte) ([]arbo.Invalid, error) {
	cr.treeMu.Lock()
	defer cr.treeMu.Unlock()

	if len(keys) != len(values) {
		return nil, fmt.Errorf("keys and values must have the same length: %d != %d", len(keys), len(values))
	}

	wtx := cr.tree.Database().WriteTx()
	defer wtx.Discard()
	invalids, err := cr.tree.AddBatchWithTx(wtx, keys, values)
	if err != nil {
		return nil, fmt.Errorf("failed to add batch: %w", err)
	}

	// Update the current root
	newRoot, err := cr.tree.RootWithTx(wtx)
	if err != nil {
		return invalids, fmt.Errorf("failed to get new root: %w", err)
	}
	if err := wtx.Commit(); err != nil {
		return invalids, fmt.Errorf("failed to commit transaction: %w", err)
	}

	cr.currentRoot = newRoot

	return invalids, nil
}

// FetchKeysAndValues fetches all keys and values from the Merkle tree.
// Returns the keys as byte arrays and the values as BigInts.
func (cr *CensusRef) FetchKeysAndValues() ([]HexBytes, []*BigInt, error) {
	cr.treeMu.Lock()
	defer cr.treeMu.Unlock()

	buf := new(bytes.Buffer)
	err := cr.tree.DumpWriter(nil, buf)
	if err != nil {
		return nil, nil, err
	}

	var keys []HexBytes
	var values []*BigInt

	for {
		l := make([]byte, 3)
		_, err = io.ReadFull(buf, l)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, err
		}
		lenK := int(l[0])
		k := make([]byte, lenK)
		_, err = io.ReadFull(buf, k)
		if err != nil {
			return nil, nil, err
		}
		lenV := binary.LittleEndian.Uint16(l[1:3])
		v := make([]byte, lenV)
		_, err = io.ReadFull(buf, v)
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, k)
		values = append(values, (*BigInt)(arbo.BytesToBigInt(v)))
	}

	return keys, values, nil
}

// Root safely returns the current Merkle tree root.
func (cr *CensusRef) Root() []byte {
	cr.treeMu.Lock()
	defer cr.treeMu.Unlock()
	root, err := cr.tree.Root()
	if err != nil {
		return nil
	}
	return root
}

// Size safely returns the number of leaves in the Merkle tree.
func (cr *CensusRef) Size() int {
	cr.treeMu.Lock()
	defer cr.treeMu.Unlock()
	size, err := cr.tree.GetNLeafs()
	if err != nil {
		return 0
	}
	return size
}

// GenProof safely generates a Merkle proof for the given leaf key.
// It returns the proof components and an inclusion boolean.
func (cr *CensusRef) GenProof(key []byte) ([]byte, []byte, []byte, bool, error) {
	cr.treeMu.Lock()
	defer cr.treeMu.Unlock()
	return cr.tree.GenProof(key)
}

// VerifyProof verifies a Merkle proof for the given leaf key.
func VerifyProof(key, value, root, siblings []byte) bool {
	valid, err := arbo.CheckProof(defaultHashFunction, key, value, root, siblings)
	if err != nil {
		return false
	}
	return valid
}

// BigIntSiblings unpacks a serialized siblings array using the default hash
// function and returns the individual sibling leaves as big.Ints in
// little-endian format.
func BigIntSiblings(siblings []byte) ([]*big.Int, error) {
	unpackedSiblings, err := arbo.UnpackSiblings(defaultHashFunction, siblings)
	if err != nil {
		return nil, err
	}
	bigSiblings := []*big.Int{}
	for _, sibling := range unpackedSiblings {
		bigSiblings = append(bigSiblings, arbo.BytesToBigInt(sibling))
	}
	return bigSiblings, nil
}
