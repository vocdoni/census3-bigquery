package service

import (
	"sync"

	"github.com/vocdoni/davinci-node/types"

	"census3-bigquery/log"
	"census3-bigquery/storage"
)

// AddressCollector manages parallel collection and storage of addresses during census creation
type AddressCollector struct {
	storage    *storage.KVSnapshotStorage
	censusRoot types.HexBytes
	pageSize   int
	enabled    bool

	// Internal state
	mu          sync.Mutex
	addresses   []string
	currentPage int
}

// NewAddressCollector creates a new address collector
func NewAddressCollector(storage *storage.KVSnapshotStorage, censusRoot types.HexBytes, pageSize int, enabled bool) *AddressCollector {
	return &AddressCollector{
		storage:     storage,
		censusRoot:  censusRoot,
		pageSize:    pageSize,
		enabled:     enabled,
		addresses:   make([]string, 0, pageSize),
		currentPage: 0,
	}
}

// AddAddress adds an address to the collector and stores pages when full
func (ac *AddressCollector) AddAddress(address string) error {
	if !ac.enabled {
		return nil // Address storage is disabled
	}

	ac.mu.Lock()
	defer ac.mu.Unlock()

	ac.addresses = append(ac.addresses, address)

	// Store page when it reaches the configured size
	if len(ac.addresses) >= ac.pageSize {
		if err := ac.storePage(); err != nil {
			return err
		}
	}

	return nil
}

// Finalize stores any remaining addresses and completes the collection process
func (ac *AddressCollector) Finalize() error {
	if !ac.enabled {
		return nil // Address storage is disabled
	}

	ac.mu.Lock()
	defer ac.mu.Unlock()

	// Store any remaining addresses
	if len(ac.addresses) > 0 {
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

// storePage stores the current page of addresses (must be called with mutex held)
func (ac *AddressCollector) storePage() error {
	if len(ac.addresses) == 0 {
		return nil
	}

	// Store the current page
	if err := ac.storage.StoreAddressPage(ac.censusRoot, ac.currentPage, ac.addresses); err != nil {
		return err
	}

	log.Debug().
		Str("census_root", ac.censusRoot.String()).
		Int("page_number", ac.currentPage).
		Int("addresses_in_page", len(ac.addresses)).
		Msg("Stored address page")

	// Reset for next page
	ac.addresses = ac.addresses[:0] // Clear slice but keep capacity
	ac.currentPage++

	return nil
}
