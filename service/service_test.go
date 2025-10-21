package service

import (
	"github.com/vocdoni/census3-bigquery/bigquery"
	"github.com/vocdoni/census3-bigquery/censusdb"
	"github.com/vocdoni/census3-bigquery/config"
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/frankban/quicktest"
	"github.com/google/uuid"
	"github.com/vocdoni/davinci-node/db/metadb"
)

// mockBigQueryClient implements a mock BigQuery client for testing
type mockBigQueryClient struct {
	participants []bigquery.Participant
}

func (m *mockBigQueryClient) StreamBalances(ctx context.Context, cfg bigquery.Config, participantCh chan<- bigquery.Participant, errorCh chan<- error) {
	defer close(participantCh)
	defer close(errorCh)

	for _, participant := range m.participants {
		select {
		case participantCh <- participant:
			// Add a small delay to allow context cancellation to work
			time.Sleep(1 * time.Millisecond)
		case <-ctx.Done():
			errorCh <- ctx.Err()
			return
		}
	}
}

func (m *mockBigQueryClient) FetchBalancesToCSV(ctx context.Context, cfg bigquery.Config, csvPath string) (int, error) {
	return len(m.participants), nil
}

func (m *mockBigQueryClient) Close() error {
	return nil
}

func TestQueryRunnerStreamAndCreateCensus(t *testing.T) {
	c := quicktest.New(t)

	// Create test database
	database := metadb.NewTest(t)
	censusDB := censusdb.NewCensusDB(database)

	// Create test service with mock BigQuery client
	testParticipants := []bigquery.Participant{
		{
			Address: common.HexToAddress("0x1234567890123456789012345678901234567890"),
			Balance: big.NewInt(100),
		},
		{
			Address: common.HexToAddress("0x2345678901234567890123456789012345678901"),
			Balance: big.NewInt(200),
		},
		{
			Address: common.HexToAddress("0x3456789012345678901234567890123456789012"),
			Balance: big.NewInt(300),
		},
	}

	mockClient := &mockBigQueryClient{participants: testParticipants}

	service := &Service{
		config: &config.Config{
			BatchSize: 2, // Small batch size for testing
		},
		censusDB:       censusDB,
		bigqueryClient: mockClient,
		ctx:            context.Background(),
	}

	// Create query runner
	queryConfig := &config.QueryConfig{
		Name:   "test_query",
		Period: time.Hour,
		Parameters: map[string]interface{}{
			"min_balance": 0.1,
		},
	}

	queryRunner := &QueryRunner{
		config:  queryConfig,
		service: service,
		ctx:     context.Background(),
	}

	// Create a new census
	censusID := uuid.New()
	censusRef, err := censusDB.New(censusID)
	c.Assert(err, quicktest.IsNil)

	// Test streaming and census creation
	count, err := queryRunner.streamAndCreateCensusBigQuery(censusRef)
	c.Assert(err, quicktest.IsNil)
	c.Assert(count, quicktest.Equals, len(testParticipants))

	// Verify census was created correctly
	censusSize := censusRef.Size()
	c.Assert(censusSize, quicktest.Equals, len(testParticipants))

	// Verify census root is not nil
	root := censusRef.Root()
	c.Assert(root, quicktest.Not(quicktest.IsNil))
	c.Assert(len(root), quicktest.Not(quicktest.Equals), 0)
}

func TestQueryRunnerStreamAndCreateCensusWithLargeBatch(t *testing.T) {
	c := quicktest.New(t)

	// Create test database
	database := metadb.NewTest(t)
	censusDB := censusdb.NewCensusDB(database)

	// Create many test participants
	var testParticipants []bigquery.Participant
	for i := 0; i < 1000; i++ {
		addr := common.BigToAddress(big.NewInt(int64(i + 1)))
		balance := big.NewInt(int64((i + 1) * 100))
		testParticipants = append(testParticipants, bigquery.Participant{
			Address: addr,
			Balance: balance,
		})
	}

	mockClient := &mockBigQueryClient{participants: testParticipants}

	service := &Service{
		config: &config.Config{
			BatchSize: 100, // Larger batch size
		},
		censusDB:       censusDB,
		bigqueryClient: mockClient,
		ctx:            context.Background(),
	}

	// Create query runner
	queryConfig := &config.QueryConfig{
		Name:   "test_query",
		Period: time.Hour,
		Parameters: map[string]interface{}{
			"min_balance": 0.1,
		},
	}

	queryRunner := &QueryRunner{
		config:  queryConfig,
		service: service,
		ctx:     context.Background(),
	}

	// Create a new census
	censusID := uuid.New()
	censusRef, err := censusDB.New(censusID)
	c.Assert(err, quicktest.IsNil)

	// Test streaming and census creation
	start := time.Now()
	count, err := queryRunner.streamAndCreateCensusBigQuery(censusRef)
	elapsed := time.Since(start)

	c.Assert(err, quicktest.IsNil)
	c.Assert(count, quicktest.Equals, len(testParticipants))

	// Verify census was created correctly
	censusSize := censusRef.Size()
	c.Assert(censusSize, quicktest.Equals, len(testParticipants))

	// Verify census root is not nil
	root := censusRef.Root()
	c.Assert(root, quicktest.Not(quicktest.IsNil))
	c.Assert(len(root), quicktest.Not(quicktest.Equals), 0)

	t.Logf("Processed %d participants in %v (%.2f participants/sec)",
		count, elapsed, float64(count)/elapsed.Seconds())
}

func TestQueryRunnerStreamAndCreateCensusContextCancellation(t *testing.T) {
	c := quicktest.New(t)

	// Create test database
	database := metadb.NewTest(t)
	censusDB := censusdb.NewCensusDB(database)

	// Create test participants
	var testParticipants []bigquery.Participant
	for i := 0; i < 100; i++ {
		addr := common.BigToAddress(big.NewInt(int64(i + 1)))
		balance := big.NewInt(int64((i + 1) * 100))
		testParticipants = append(testParticipants, bigquery.Participant{
			Address: addr,
			Balance: balance,
		})
	}

	mockClient := &mockBigQueryClient{participants: testParticipants}

	// Create context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	service := &Service{
		config: &config.Config{
			BatchSize: 10,
		},
		censusDB:       censusDB,
		bigqueryClient: mockClient,
		ctx:            ctx,
	}

	// Create query runner
	queryConfig := &config.QueryConfig{
		Name:   "test_query",
		Period: time.Hour,
		Parameters: map[string]interface{}{
			"min_balance": 0.1,
		},
	}

	queryRunner := &QueryRunner{
		config:  queryConfig,
		service: service,
		ctx:     ctx,
	}

	// Create a new census
	censusID := uuid.New()
	censusRef, err := censusDB.New(censusID)
	c.Assert(err, quicktest.IsNil)

	// Cancel context immediately
	go func() {
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()

	// Test streaming and census creation with cancellation
	_, err = queryRunner.streamAndCreateCensusBigQuery(censusRef)
	c.Assert(err, quicktest.Not(quicktest.IsNil))
	c.Assert(err.Error(), quicktest.Contains, "context cancelled")
}

func TestQueryConfigGetQueryID(t *testing.T) {
	c := quicktest.New(t)

	// Test basic query ID generation
	queryConfig := &config.QueryConfig{
		Name: "ethereum_balances",
		Parameters: map[string]interface{}{
			"min_balance": 1.0,
		},
	}

	queryID := queryConfig.GetQueryID()
	c.Assert(queryID, quicktest.Equals, "ethereum_balances_mb1.00")

	// Test with token address
	queryConfig2 := &config.QueryConfig{
		Name: "erc20_holders",
		Parameters: map[string]interface{}{
			"min_balance":   100.0,
			"token_address": "0x1234567890123456789012345678901234567890",
		},
	}

	queryID2 := queryConfig2.GetQueryID()
	c.Assert(queryID2, quicktest.Equals, "erc20_holders_mb100.00_token0x1234567890123456789012345678901234567890")

	// Test without parameters
	queryConfig3 := &config.QueryConfig{
		Name:       "custom_query",
		Parameters: map[string]interface{}{},
	}

	queryID3 := queryConfig3.GetQueryID()
	c.Assert(queryID3, quicktest.Equals, "custom_query")
}

func TestQueryConfigGetMinBalance(t *testing.T) {
	c := quicktest.New(t)

	// Test with float64
	queryConfig := &config.QueryConfig{
		Parameters: map[string]interface{}{
			"min_balance": 1.5,
		},
	}
	c.Assert(queryConfig.GetMinBalance(), quicktest.Equals, 1.5)

	// Test with int
	queryConfig2 := &config.QueryConfig{
		Parameters: map[string]interface{}{
			"min_balance": 2,
		},
	}
	c.Assert(queryConfig2.GetMinBalance(), quicktest.Equals, 2.0)

	// Test with int64
	queryConfig3 := &config.QueryConfig{
		Parameters: map[string]interface{}{
			"min_balance": int64(3),
		},
	}
	c.Assert(queryConfig3.GetMinBalance(), quicktest.Equals, 3.0)

	// Test without min_balance
	queryConfig4 := &config.QueryConfig{
		Parameters: map[string]interface{}{},
	}
	c.Assert(queryConfig4.GetMinBalance(), quicktest.Equals, 0.0)
}
