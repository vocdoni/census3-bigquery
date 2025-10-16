package service

import (
	"census3-bigquery/log"
	"census3-bigquery/storage"
	"sync"

	"github.com/vocdoni/davinci-node/types"
)

// AddressCollector manages parallel collection and storage of addresses with weights during census creation
type AddressCollector struct {
	storage    *storage.KVSnapshotStorage
	censusRoot types.HexBytes
	pageSize   int
	enabled    bool

	// Internal state
	mu          sync.Mutex
	entries     []storage.AddressEntry
	currentPage int
}

// NewAddressCollector creates a new address collector
func NewAddressCollector(kvStorage *storage.KVSnapshotStorage, censusRoot types.HexBytes, pageSize int, enabled bool) *AddressCollector {
	return &AddressCollector{
		storage:     kvStorage,
		censusRoot:  censusRoot,
		pageSize:    pageSize,
		enabled:     enabled,
		entries:     make([]storage.AddressEntry, 0, pageSize),
		currentPage: 0,
	}
}

// AddAddressWithWeight adds an address and weight to the collector and stores pages when full
func (ac *AddressCollector) AddAddressWithWeight(address string, weight float64) error {
	if !ac.enabled {
		return nil // Address storage is disabled
	}

	ac.mu.Lock()
	defer ac.mu.Unlock()

	entry := storage.AddressEntry{
		Address: address,
		Weight:  weight,
	}
	ac.entries = append(ac.entries, entry)

	// Store page when it reaches the configured size
	if len(ac.entries) >= ac.pageSize {
		if err := ac.storePage(); err != nil {
			return err
		}
	}

	return nil
}

// AddAddress adds an address with default weight (for backward compatibility)
func (ac *AddressCollector) AddAddress(address string) error {
	return ac.AddAddressWithWeight(address, 1.0)
}

// Finalize stores any remaining addresses and completes the collection process
func (ac *AddressCollector) Finalize() error {
	if !ac.enabled {
		return nil // Address storage is disabled
	}

	ac.mu.Lock()
	defer ac.mu.Unlock()

	// Store any remaining entries
	if len(ac.entries) > 0 {
		if err := ac.storePage(); err != nil {
			return err
		}
	}

	log.Debug().
		Str("census_root", ac.censusRoot.String()).
		Int("total_pages", ac.currentPage).
		Msg("Address collection completed")

	return nil
}

// storePage stores the current page of address entries (must be called with mutex held)
func (ac *AddressCollector) storePage() error {
	if len(ac.entries) == 0 {
		return nil
	}

	// Store the current page
	if err := ac.storage.StoreAddressPage(ac.censusRoot, ac.currentPage, ac.entries); err != nil {
		return err
	}

	log.Debug().
		Str("census_root", ac.censusRoot.String()).
		Int("page_number", ac.currentPage).
		Int("entries_in_page", len(ac.entries)).
		Msg("Stored address page")

	// Reset for next page
	ac.entries = ac.entries[:0] // Clear slice but keep capacity
	ac.currentPage++

	return nil
}
