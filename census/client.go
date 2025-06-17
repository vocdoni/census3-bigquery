package census

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/vocdoni/davinci-node/api"
	"github.com/vocdoni/davinci-node/api/client"
	"github.com/vocdoni/davinci-node/types"
)

// Default batch size for census creation - optimized for large datasets
const defaultBatchSize = 5000

// Buffer sizes optimized for large files
const (
	scannerBufferSize = 256 * 1024      // 256KB initial buffer
	maxLineSize       = 2 * 1024 * 1024 // 2MB max line size
)

// HTTPClient interface for census operations
type HTTPClient interface {
	Request(method string, jsonBody any, queryParams []string, endpoint ...string) ([]byte, int, error)
}

// Client wraps census operations
type Client struct {
	client HTTPClient
}

// Config holds census configuration
type Config struct {
	Host      string
	BatchSize int
}

// NewClient creates a new census client
func NewClient(host string) (*Client, error) {
	cli, err := client.New(host)
	if err != nil {
		return nil, fmt.Errorf("failed to create census client: %w", err)
	}

	return &Client{client: cli}, nil
}

// CreateCensusFromCSV creates a census from a CSV file and returns the census root
func (c *Client) CreateCensusFromCSV(csvPath string, batchSize int) (types.HexBytes, int, error) {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	// Create a new census
	body, code, err := c.client.Request(http.MethodPost, nil, nil, api.NewCensusEndpoint)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create census: %w", err)
	}
	if code != http.StatusOK {
		return nil, 0, fmt.Errorf("failed to create census: %s", string(body))
	}

	var resp api.NewCensus
	err = json.NewDecoder(bytes.NewReader(body)).Decode(&resp)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode new census response: %w", err)
	}
	censusId := resp.Census

	// Count total lines in file for progress tracking
	totalLines, err := c.countLinesInFile(csvPath)
	if err != nil {
		fmt.Printf("Warning: could not count lines in CSV file: %v\n", err)
		fmt.Printf("Starting census creation (progress percentage unavailable)\n")
		totalLines = -1 // Indicate unknown total
	} else {
		fmt.Printf("Starting census creation with %d participants\n", totalLines)
	}

	// Read the CSV file and add participants by batches
	fd, err := os.Open(csvPath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer func() {
		if err := fd.Close(); err != nil {
			fmt.Printf("Warning: failed to close CSV file: %v\n", err)
		}
	}()

	// Use a buffered reader for efficient reading of large files
	reader := bufio.NewScanner(fd)
	reader.Buffer(make([]byte, 0, scannerBufferSize), maxLineSize)

	// Progress tracking variables
	var totalProcessed int
	var skippedLines int
	startTime := time.Now()
	lastDebugTime := startTime

	// Pre-allocate slice with capacity to avoid reallocations
	participants := make([]*api.CensusParticipant, 0, batchSize)

	for {
		// Reset slice length but keep capacity
		participants = participants[:0]

		// Read up to batchSize participants
		for len(participants) < batchSize {
			if !reader.Scan() {
				// End of file or error
				if err := reader.Err(); err != nil {
					return nil, 0, fmt.Errorf("error reading CSV file: %w", err)
				}
				// If we have participants in the current batch, process them
				if len(participants) > 0 {
					break
				}
				// No more data and no participants in current batch, we're done
				goto finalize
			}

			line := strings.TrimSpace(reader.Text())
			if line == "" {
				continue // Skip empty lines without counting
			}

			// Parse the line: address,balance ETH - use more efficient parsing
			commaIndex := strings.IndexByte(line, ',')
			if commaIndex == -1 {
				fmt.Printf("Warning: skipping invalid CSV format at line: %s\n", line)
				skippedLines++
				continue
			}

			addressStr := strings.TrimSpace(line[:commaIndex])
			balanceStr := strings.TrimSpace(line[commaIndex+1:])

			// Remove " ETH" suffix if present
			balanceStr = strings.TrimSuffix(balanceStr, " ETH")

			// Parse address
			if !common.IsHexAddress(addressStr) {
				fmt.Printf("Warning: skipping invalid address format: %s\n", addressStr)
				skippedLines++
				continue
			}
			address := common.HexToAddress(addressStr)

			// Parse balance (float) and convert to integer by multiplying by 100
			balanceFloat, err := strconv.ParseFloat(balanceStr, 64)
			if err != nil {
				fmt.Printf("Warning: skipping invalid balance format: %s (error: %v)\n", balanceStr, err)
				skippedLines++
				continue
			}

			// Convert to integer by multiplying by 100 (as mentioned in comments)
			balanceInt := int64(balanceFloat * 100)
			weight := (*types.BigInt)(big.NewInt(balanceInt))

			participants = append(participants, &api.CensusParticipant{
				Key:    address.Bytes(),
				Weight: weight,
			})
		}

		// If no participants were added, we're done
		if len(participants) == 0 {
			break
		}

		// Create census participants struct
		censusParticipants := api.CensusParticipants{Participants: participants}

		// Update progress tracking
		totalProcessed += len(participants)

		// Add participants to census
		addEndpoint := api.EndpointWithParam(api.AddCensusParticipantsEndpoint, api.CensusURLParam, censusId.String())
		_, code, err = c.client.Request(http.MethodPost, censusParticipants, nil, addEndpoint)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to add participants to census: %w", err)
		}
		if code != http.StatusOK {
			return nil, 0, fmt.Errorf("failed to add participants to census, status code: %d", code)
		}

		// Debug print every 10 seconds
		currentTime := time.Now()
		if currentTime.Sub(lastDebugTime) >= 10*time.Second {
			elapsed := currentTime.Sub(startTime)
			participantsPerSecond := float64(totalProcessed) / elapsed.Seconds()

			if totalLines > 0 {
				remaining := totalLines - totalProcessed
				progressPercent := float64(totalProcessed) / float64(totalLines) * 100
				fmt.Printf("Progress: %d/%d participants (%.2f%%) | Remaining: %d | Rate: %.2f participants/sec | Elapsed: %v\n",
					totalProcessed, totalLines, progressPercent, remaining, participantsPerSecond, elapsed.Truncate(time.Second))
			} else {
				fmt.Printf("Progress: %d participants processed | Rate: %.2f participants/sec | Elapsed: %v\n",
					totalProcessed, participantsPerSecond, elapsed.Truncate(time.Second))
			}

			lastDebugTime = currentTime
		}
	}

finalize:
	// Final progress report
	elapsed := time.Since(startTime)
	participantsPerSecond := float64(totalProcessed) / elapsed.Seconds()
	fmt.Printf("Completed: %d participants processed in %v (%.2f participants/sec)\n",
		totalProcessed, elapsed.Truncate(time.Second), participantsPerSecond)
	if skippedLines > 0 {
		fmt.Printf("Warning: %d lines were skipped due to invalid format\n", skippedLines)
	}

	// Get census root
	getRootEndpoint := api.EndpointWithParam(api.GetCensusRootEndpoint, api.CensusURLParam, resp.Census.String())
	body, code, err = c.client.Request(http.MethodGet, nil, nil, getRootEndpoint)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get census root: %w", err)
	}
	if code != http.StatusOK {
		return nil, 0, fmt.Errorf("failed to get census root, status code: %d", code)
	}

	var rootResp api.CensusRoot
	err = json.NewDecoder(bytes.NewReader(body)).Decode(&rootResp)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode census root response: %w", err)
	}

	return rootResp.Root, totalProcessed, nil
}

// countLinesInFile counts the number of non-empty lines in a file
func (c *Client) countLinesInFile(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Printf("Warning: failed to close file: %v\n", err)
		}
	}()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, scannerBufferSize), maxLineSize)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			count++
		}
	}
	return count, scanner.Err()
}
