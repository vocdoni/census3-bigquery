// census3-bigquery
//
// Usage examples:
//
// Query BigQuery and dump balances:
//
//	go run . -project my-gcp-project -address 0x3ac1699659be5f41a0e57e179d6cb42e00b8311d
//	go run . -project my-gcp-project -min 0.25 -count 100
//
// Create census from CSV file:
//
//	go run . -command=census -csv=addresses.list -host=http://localhost:8080 -batch-size=1000
//
// Run service (merges both functionalities):
//
//	go run . -command=service -project my-gcp-project -period=1h
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"

	"census3-bigquery/internal/config"
	"census3-bigquery/internal/service"
)

// BigQuery's column is NUMERIC (gwei), 1 ETH = 1 000 000 000 gwei.
var weiPerETH = big.NewRat(1_000_000_000_000_000_000, 1)

// weiToETH converts the NUMERIC coming from BigQuery to an ETH-denominated *big.Rat.
func weiToETH(w *big.Rat) *big.Rat { return new(big.Rat).Quo(w, weiPerETH) }

// ethToWei converts an ETH amount (from -min) to the wei NUMERIC BigQuery stores.
func ethToWei(e *big.Rat) *big.Rat { return new(big.Rat).Mul(e, weiPerETH) }

// fmtETH prints an *big.Rat with up to 18 decimals (full wei precision) without
// trailing zeros.
func fmtETH(r *big.Rat) string { return r.FloatString(18) }

type balanceRow struct {
	Address string   `bigquery:"address"`
	Balance *big.Rat `bigquery:"eth_balance"` // wei inside BigQuery
}

func main() {
	// Command selection flags
	command := flag.String("command", "query", "Command to execute: 'query', 'census', or 'service'")

	// BigQuery flags
	project := flag.String("project", "", "Billing-enabled GCP project (required for query command)")
	addrFlag := flag.String("address", "", "Exact Ethereum address to look up")
	minFlag := flag.String("min", "0", "Minimum balance in ETH (ignored with -address)")
	maxFlag := flag.Int("count", 0, "Maximum rows to print (0 = unlimited)")

	// Census creation flags
	csvFile := flag.String("csv", "", "CSV file path for census creation (required for census command)")
	host := flag.String("host", "http://localhost:8080", "Vocdoni node host URL for census creation")
	batchSizeFlag := flag.Int("batch-size", defaultBatchSize, "Batch size for census creation")

	flag.Parse()

	switch *command {
	case "query":
		runBigQueryCommand(project, addrFlag, minFlag, maxFlag)
	case "census":
		runCensusCommand(csvFile, host, batchSizeFlag)
	case "service":
		runServiceCommand()
	default:
		log.Fatalf("Unknown command: %s. Use 'query', 'census', or 'service'", *command)
	}
}

func runServiceCommand() {
	// Load configuration using viper and pflag
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create and start service
	svc, err := service.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create service: %v", err)
	}

	// Start service (blocks until shutdown)
	if err := svc.Start(); err != nil {
		log.Fatalf("Service error: %v", err)
	}

	os.Exit(0)
}

func runBigQueryCommand(project, addrFlag, minFlag *string, maxFlag *int) {
	if *project == "" {
		log.Fatal("missing -project flag for query command")
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *project) // Application-Default Credentials
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("Failed to close BigQuery client: %v", err)
		}
	}()

	var sql string
	var params []bigquery.QueryParameter

	if *addrFlag != "" { // single-address mode
		sql = `
		  SELECT address, eth_balance
		  FROM  ` + "`bigquery-public-data.crypto_ethereum.balances`" + `
		  WHERE address = @addr`
		params = []bigquery.QueryParameter{
			{Name: "addr", Value: strings.ToLower(*addrFlag)},
		}
	} else { // filtered stream mode
		minEth, ok := new(big.Rat).SetString(*minFlag)
		if !ok {
			log.Fatalf("invalid -min value: %q", *minFlag)
		}
		sql = `
		  SELECT address, eth_balance
		  FROM  ` + "`bigquery-public-data.crypto_ethereum.balances`" + `
		  WHERE eth_balance >= @min
		  ORDER BY eth_balance DESC`
		params = []bigquery.QueryParameter{
			{Name: "min", Value: ethToWei(minEth)}, // compare apples-to-apples in wei
		}
		if *maxFlag > 0 {
			sql += fmt.Sprintf("\n		  LIMIT %d", *maxFlag)
		}
	}

	q := client.Query(sql)
	q.Parameters = params
	it, err := q.Read(ctx)
	if err != nil {
		log.Fatalf("query: %v", err)
	}

	printed := 0
	for *addrFlag != "" || *maxFlag == 0 || printed < *maxFlag {
		var r balanceRow
		switch err := it.Next(&r); err {
		case iterator.Done:
			return
		case nil:
			fmt.Printf("%s,%s\n", r.Address, fmtETH(weiToETH(r.Balance)))
			printed++
		default:
			log.Fatalf("iterator: %v", err)
		}
	}
}

func runCensusCommand(csvFile, host *string, batchSizeFlag *int) {
	if *csvFile == "" {
		log.Fatal("missing -csv flag for census command")
	}

	if *host == "" {
		log.Fatal("missing -host flag for census command")
	}

	// Check if CSV file exists
	if _, err := os.Stat(*csvFile); os.IsNotExist(err) {
		log.Fatalf("CSV file does not exist: %s", *csvFile)
	}

	// Update the global batchSize constant
	if *batchSizeFlag > 0 {
		// We can't modify the const, but we can pass it to the function
		fmt.Printf("Creating census from CSV file: %s\n", *csvFile)
		fmt.Printf("Host: %s\n", *host)
		fmt.Printf("Batch size: %d\n", *batchSizeFlag)
	}

	// Call the census creation function
	root, err := createCensusFromCSV(*csvFile, *host, *batchSizeFlag)
	if err != nil {
		log.Fatalf("Failed to create census: %v", err)
	}

	fmt.Printf("Census created successfully!\n")
	fmt.Printf("Census root: %s\n", root.String())
}
