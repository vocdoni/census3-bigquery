package storage

import (
	"testing"

	"github.com/frankban/quicktest"
	"go.vocdoni.io/dvote/db/metadb"
)

func TestSimpleDeletion(t *testing.T) {
	c := quicktest.New(t)

	// Create test database
	database := metadb.NewTest(t)

	// Test basic set/get/delete operations
	wtx := database.WriteTx()
	defer wtx.Discard()

	// Set a key
	err := wtx.Set([]byte("test_key"), []byte("test_value"))
	c.Assert(err, quicktest.IsNil)

	// Commit the transaction
	err = wtx.Commit()
	c.Assert(err, quicktest.IsNil)

	// Verify the key exists
	value, err := database.Get([]byte("test_key"))
	c.Assert(err, quicktest.IsNil)
	c.Assert(string(value), quicktest.Equals, "test_value")

	// Delete the key
	wtx2 := database.WriteTx()
	defer wtx2.Discard()

	err = wtx2.Delete([]byte("test_key"))
	c.Assert(err, quicktest.IsNil)

	err = wtx2.Commit()
	c.Assert(err, quicktest.IsNil)

	// Verify the key is deleted
	_, err = database.Get([]byte("test_key"))
	c.Assert(err, quicktest.Not(quicktest.IsNil)) // Should return an error (key not found)
}
