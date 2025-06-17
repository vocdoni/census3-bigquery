# Census3 BigQuery Service

A service that automatically creates Ethereum census snapshots by querying BigQuery for ETH balances and creating census merkle-trees for Davinci.vote. Supports both local storage and GitHub repository storage with compressed CSV files.

## Features

- **Automated Snapshots**: Periodic creation of census snapshots from Ethereum balance data
- **Multiple Balance Thresholds**: Support for multiple minimum ETH balance filters in a single run
- **Modular BigQuery System**: Choose from multiple predefined queries or add custom ones
- **Dual Storage Modes**: 
  - Local storage with HTTP API
  - GitHub repository storage with compressed CSV files
- **BigQuery Integration**: Efficient querying of Ethereum balance data
- **Vocdoni Census Creation**: Automatic census tree creation on Davinci.vote nodes

## Quick Start

### Using Docker Compose (Recommended)

1. **Clone and configure**:
   ```bash
   git clone <repository-url>
   cd census3-bigquery
   cp .env.example .env
   # Edit .env with your configuration
   ```

2. **Start the service**:
   ```bash
   docker-compose up -d
   ```

### Using Go Binary

1. **Install dependencies**:
   ```bash
   go mod download
   ```

2. **Run the service**:
   ```bash
   go run ./cmd/service --project=your-gcp-project --min-balances=0.25,1.0,5.0
   ```

## Configuration

### Command Line Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--project` | GCP project ID for BigQuery (required) | - |
| `--min-balances` | Minimum ETH balances (comma-separated) | `[0.25]` |
| `--period` | Sync period (e.g., 1h, 30m) | `1h` |
| `--api-port` | API server port | `8080` |
| `--storage-path` | Path to store snapshots | `./snapshots.json` |
| `--query-name` | BigQuery query to execute | `ethereum_balances` |
| `--host` | Vocdoni node host URL | `http://localhost:8080` |
| `--batch-size` | Batch size for census creation | `5000` |
| `--list-queries` | List available BigQuery queries and exit | - |
| `--github-enabled` | Enable GitHub storage mode | `false` |
| `--github-pat` | GitHub Personal Access Token | - |
| `--github-repo` | GitHub repository URL | - |
| `--max-snapshots-to-keep` | Maximum snapshots to keep in repository | `5` |
| `--skip-csv-upload` | Skip uploading CSV files (metadata only) | `false` |

### Environment Variables

All flags can be configured via environment variables with the `CENSUS3_` prefix:

| Environment Variable | Flag Equivalent |
|---------------------|-----------------|
| `CENSUS3_PROJECT` | `--project` |
| `CENSUS3_MIN_BALANCES` | `--min-balances` |
| `CENSUS3_PERIOD` | `--period` |
| `CENSUS3_API_PORT` | `--api-port` |
| `CENSUS3_STORAGE_PATH` | `--storage-path` |
| `CENSUS3_HOST` | `--host` |
| `CENSUS3_BATCH_SIZE` | `--batch-size` |
| `CENSUS3_GITHUB_ENABLED` | `--github-enabled` |
| `CENSUS3_GITHUB_PAT` | `--github-pat` |
| `CENSUS3_GITHUB_REPO` | `--github-repo` |
| `CENSUS3_MAX_SNAPSHOTS_TO_KEEP` | `--max-snapshots-to-keep` |
| `CENSUS3_SKIP_CSV_UPLOAD` | `--skip-csv-upload` |
| `CENSUS3_QUERY_NAME` | `--query-name` |

## BigQuery Query System

The service features a modular BigQuery system that allows you to choose from multiple predefined queries or easily add custom ones.

### Available Queries

Use `--list-queries` to see all available queries:

```bash
go run ./cmd/service --list-queries
```

**Output:**
```
Available BigQuery queries:

  ethereum_balances:
    Description: Fetch Ethereum addresses with balance above minimum threshold
    Parameters: [min_balance]

  ethereum_balances_recent:
    Description: Fetch Ethereum addresses with recent activity and balance above threshold
    Parameters: [min_balance]

  erc20_holders:
    Description: Fetch ERC20 token holders above minimum balance threshold
    Parameters: [token_address min_balance]
```

### Query Selection

Select a query using the `--query-name` flag:

```bash
# Use default ethereum_balances query
go run ./cmd/service --project=my-project --min-balances=1.0

# Use recent activity query
go run ./cmd/service --project=my-project --query-name=ethereum_balances_recent --min-balances=0.5

# Use ERC20 holders query (requires additional parameters)
go run ./cmd/service --project=my-project --query-name=erc20_holders --min-balances=100
```

### Query Details

#### `ethereum_balances` (Default)
- **Description**: Fetches Ethereum addresses with balance above minimum threshold
- **Parameters**: `min_balance`
- **Use Case**: Standard ETH balance snapshots
- **SQL**: Queries `bigquery-public-data.crypto_ethereum.balances`

#### `ethereum_balances_recent`
- **Description**: Fetches Ethereum addresses with recent activity (last 30 days) and balance above threshold
- **Parameters**: `min_balance`
- **Use Case**: Active addresses only, excludes dormant wallets
- **SQL**: Joins balances with recent transactions

#### `erc20_holders`
- **Description**: Fetches ERC20 token holders above minimum balance threshold
- **Parameters**: `token_address`, `min_balance`
- **Use Case**: Token-specific census creation
- **SQL**: Queries `bigquery-public-data.crypto_ethereum.token_transfers`
- **Note**: Requires additional `token_address` parameter

### Template System

Queries use a template system with parameter substitution:

- **`@min_balance`**: Replaced with the minimum balance in wei
- **`@token_address`**: Custom parameter for ERC20 queries

### Adding Custom Queries

To add a new query, edit `internal/bigquery/queries.go`:

```go
"custom_query": {
    Name: "custom_query",
    Description: "Your custom BigQuery description",
    SQL: `
        SELECT address, eth_balance
        FROM your_custom_table
        WHERE eth_balance >= @min_balance
        AND custom_field = @custom_param
        ORDER BY eth_balance DESC`,
    Parameters: []string{"min_balance", "custom_param"},
},
```

### Query Configuration Examples

**Environment Variables:**
```bash
# Use recent activity query
export CENSUS3_QUERY_NAME=ethereum_balances_recent
export CENSUS3_MIN_BALANCES=0.25,1.0,5.0
go run ./cmd/service --project=my-project
```

**Docker Compose:**
```yaml
environment:
  - CENSUS3_QUERY_NAME=ethereum_balances_recent
  - CENSUS3_MIN_BALANCES=0.5,2.0
```

**Command Line:**
```bash
# Multiple balance thresholds with recent activity query
go run ./cmd/service \
  --project=my-gcp-project \
  --query-name=ethereum_balances_recent \
  --min-balances=0.1,0.5,1.0,5.0 \
  --github-enabled \
  --github-pat=ghp_xxxxxxxxxxxxxxxxxxxx \
  --github-repo=https://github.com/org/eth-census-snapshots
```

## Storage Modes

### Local Storage Mode (Default)

- Stores snapshots in a local JSON file
- Provides HTTP API for accessing snapshots
- Suitable for development and single-instance deployments

**API Endpoints:**
- `GET /snapshots` - List all snapshots (ordered by most recent)
- `GET /health` - Health check endpoint

**Example snapshots.json:**
```json
{
  "snapshots": [
    {
      "snapshotDate": "2025-06-16T18:00:00Z",
      "censusRoot": "0x832f31d1490ea413864da0be8ec8e962ab0e208a0ca25178c908b5ad22c83f12",
      "participantCount": 100,
      "createdAt": "2025-06-16T18:01:23Z",
      "minBalance": 0.25,
      "queryName": "ethereum_balances"
    }
  ]
}
```

### GitHub Storage Mode

- Stores snapshots in a Git repository
- Compresses CSV files with gzip
- Disables HTTP API
- Suitable for distributed access and data sharing

**Repository Structure:**
```
repository/
├── snapshots.json           # Metadata with census roots and filenames
└── snapshots/
    ├── 2025-06-16-180000-ethereum_balances-0.25.gz
    ├── 2025-06-16-180000-ethereum_balances-1.00.gz
    ├── 2025-06-16-190000-ethereum_balances_recent-0.50.gz
    └── ...
```

**snapshots.json:**
```json
{
  "snapshots": [
    {
      "snapshotDate": "2025-06-16T18:00:00Z",
      "censusRoot": "0x832f31d1490ea413864da0be8ec8e962ab0e208a0ca25178c908b5ad22c83f12",
      "participantCount": 100,
      "createdAt": "2025-06-16T18:01:23Z",
      "minBalance": 0.25,
      "queryName": "ethereum_balances",
      "filename": "2025-06-16-180000-ethereum_balances-0.25.gz"
    }
  ]
}
```

## Multiple Balance Thresholds

The service supports processing multiple minimum balance thresholds in a single run:

```bash
# Process three different balance thresholds
go run ./cmd/service \
  --project=my-gcp-project \
  --min-balances=0.25,1.0,5.0 \
  --github-enabled \
  --github-pat=ghp_xxxxxxxxxxxxxxxxxxxx \
  --github-repo=https://github.com/org/eth-census-snapshots
```

This will create separate snapshots for each balance threshold:
- `2025-06-16-180000-ethereum_balances-0.25.gz` (addresses with ≥0.25 ETH)
- `2025-06-16-180000-ethereum_balances-1.00.gz` (addresses with ≥1.0 ETH)  
- `2025-06-16-180000-ethereum_balances-5.00.gz` (addresses with ≥5.0 ETH)

## Snapshot Retention & Repository Management

### Automatic Cleanup
The service automatically manages repository size by limiting the number of stored snapshots:

- **Default Limit**: 5 snapshots total (configurable via `--max-snapshots-to-keep`)
- **Cleanup Strategy**: Keeps the most recent snapshots, removes oldest ones
- **Cross-Balance**: Retention applies across all balance thresholds combined

### Force Push Strategy
To maintain minimal repository size:

- **Force Push**: Uses `git push --force` to overwrite remote history
- **Clean State**: Repository always contains only the current snapshots
- **No History Bloat**: Previous commits are discarded, keeping repository minimal
- **GitHub Friendly**: Repository size stays constant regardless of runtime

### Example Retention Behavior
With `--max-snapshots-to-keep=5` and multiple balance thresholds:

```bash
# After several sync cycles, repository contains:
snapshots/
├── 2025-06-16-200000-ethereum_balances-0.25.gz  # Most recent
├── 2025-06-16-200000-ethereum_balances-1.00.gz
├── 2025-06-16-190000-ethereum_balances_recent-0.25.gz
├── 2025-06-16-190000-ethereum_balances_recent-1.00.gz
└── 2025-06-16-180000-ethereum_balances-0.25.gz  # Oldest kept

# Older files automatically removed:
# ❌ 2025-06-16-180000-ethereum_balances-1.00.gz (deleted)
# ❌ 2025-06-16-170000-ethereum_balances-*.gz (deleted)
```

### Cleanup Logging
The service provides detailed logging during cleanup:

```json
{"level":"info","removed_count":3,"kept_count":5,"max_limit":5,"message":"Cleaning up old snapshots"}
{"level":"info","filename":"2025-06-16-200000-ethereum_balances-0.25.gz","message":"Successfully pushed snapshot to Git repository"}
```

### Metadata-Only Mode
For ultra-minimal repository size, you can skip uploading CSV files entirely:

```bash
# Only store metadata (census roots) without CSV files
go run ./cmd/service \
  --project=my-gcp-project \
  --github-enabled \
  --github-pat=ghp_xxxxxxxxxxxxxxxxxxxx \
  --github-repo=https://github.com/org/eth-census-snapshots \
  --skip-csv-upload
```

**Benefits:**
- **Minimal Storage**: Repository contains only snapshots.json (few KB)
- **Fast Clones**: No large binary files to download
- **GitHub Friendly**: Stays well under any size limits
- **Census Roots Preserved**: All merkle tree roots still available for verification

**Repository Structure (Metadata-Only):**
```
repository/
└── snapshots.json           # Only metadata, no CSV files
```

**snapshots.json (Metadata-Only):**
```json
{
  "snapshots": [
    {
      "snapshotDate": "2025-06-16T18:00:00Z",
      "censusRoot": "0x832f31d1490ea413864da0be8ec8e962ab0e208a0ca25178c908b5ad22c83f12",
      "participantCount": 100,
      "createdAt": "2025-06-16T18:01:23Z",
      "minBalance": 0.25,
      "queryName": "ethereum_balances",
      "filename": ""
    }
  ]
}
```

## Deployment Examples

### Local Development

```bash
export CENSUS3_PROJECT=my-gcp-project
export CENSUS3_MIN_BALANCES=0.25,1.0
export CENSUS3_HOST=https://sequencer1.davinci.vote
go run ./cmd/service
```

### Docker with Local Storage

```bash
# Create .env file
cat > .env << EOF
CENSUS3_PROJECT=my-gcp-project
CENSUS3_MIN_BALANCES=0.25,1.0,5.0
CENSUS3_HOST=https://sequencer1.davinci.vote
CENSUS3_GITHUB_ENABLED=false
EOF

# Start service
docker-compose up -d

# Check logs
docker-compose logs -f census3-service

# Access API
curl http://localhost:8080/snapshots
```

### Docker with GitHub Storage

```bash
# Create .env file
cat > .env << EOF
CENSUS3_PROJECT=my-gcp-project
CENSUS3_MIN_BALANCES=0.25,1.0,5.0
CENSUS3_HOST=https://sequencer1.davinci.vote
CENSUS3_GITHUB_ENABLED=true
CENSUS3_GITHUB_PAT=ghp_xxxxxxxxxxxxxxxxxxxx
CENSUS3_GITHUB_REPO=https://github.com/org/eth-census-snapshots
EOF

# Start service (no API server in GitHub mode)
docker-compose up -d

# Check logs
docker-compose logs -f census3-service
```

### Production Deployment

```bash
# Create production .env
cat > .env << EOF
CENSUS3_PROJECT=production-gcp-project
CENSUS3_MIN_BALANCES=0.1,0.25,0.5,1.0,5.0,10.0
CENSUS3_PERIOD=1h
CENSUS3_QUERY_NAME=ethereum_balances_recent
CENSUS3_HOST=https://sequencer1.davinci.vote
CENSUS3_BATCH_SIZE=10000
CENSUS3_GITHUB_ENABLED=true
CENSUS3_GITHUB_PAT=ghp_xxxxxxxxxxxxxxxxxxxx
CENSUS3_GITHUB_REPO=https://github.com/org/eth-census-snapshots
RESTART=always
EOF

# Deploy
docker-compose up -d
```

## Google Cloud Authentication

### Using Default Credentials (Recommended)

If running on Google Cloud (GCE, GKE, Cloud Run), the service will automatically use the default service account.

### Using Service Account Key File

1. **Create service account key**:
   ```bash
   gcloud iam service-accounts keys create key.json \
     --iam-account=your-service-account@project.iam.gserviceaccount.com
   ```

2. **Mount in Docker**:
   ```yaml
   # In docker-compose.yaml
   volumes:
     - ./key.json:/app/key.json:ro
   environment:
     - GOOGLE_APPLICATION_CREDENTIALS=/app/key.json
   ```

### Required BigQuery Permissions

The service account needs:
- `bigquery.datasets.get`
- `bigquery.tables.get`
- `bigquery.jobs.create`
- `roles/bigquery.jobUser`

## Monitoring and Logging

### Health Checks

- **HTTP**: `GET /health` (local storage mode only)
- **Docker**: Built-in healthcheck every 30s
- **Logs**: Structured JSON logging with zerolog

### Log Examples

```json
{"level":"info","time":"2025-06-16T18:00:00Z","message":"Starting census3-bigquery service"}
{"level":"info","period":3600000,"api_port":8080,"project":"my-project","query_name":"ethereum_balances","github_enabled":true,"message":"Service configuration"}
{"level":"info","message":"Using Git storage mode - API server disabled"}
{"level":"info","message":"Running initial sync"}
{"level":"info","min_balance":0.25,"balance_index":1,"total_balances":3,"message":"Processing balance threshold"}
{"level":"info","participant_count":150,"min_balance":0.25,"message":"Fetched participants from BigQuery"}
{"level":"info","census_root":"0x832f...","processed_count":150,"min_balance":0.25,"message":"Census created successfully"}
{"level":"info","filename":"2025-06-16-180000-ethereum_balances-0.25.gz","message":"Successfully pushed snapshot to Git repository"}
```

## Troubleshooting

### Common Issues

1. **BigQuery Permission Denied**:
   ```
   Error: failed to fetch BigQuery data: permission denied
   ```
   - Verify service account has BigQuery permissions
   - Check `GOOGLE_APPLICATION_CREDENTIALS` path

2. **GitHub Authentication Failed**:
   ```
   Error: failed to clone repository: authentication required
   ```
   - Verify GitHub PAT has repository access
   - Check repository URL format

3. **Census Creation Failed**:
   ```
   Error: failed to create census: connection refused
   ```
   - Verify Vocdoni node is accessible
   - Check `CENSUS3_HOST` configuration

4. **Query Not Found**:
   ```
   Error: query 'custom_query' not found in registry
   ```
   - Use `--list-queries` to see available queries
   - Check query name spelling

### Debug Mode

Enable verbose logging:
```bash
export LOG_LEVEL=debug
go run ./cmd/service --project=my-project
```

## Development

### Building

```bash
# Build service binary
go build ./cmd/service

# Build Docker image
docker build -t census3-bigquery .

# Run tests
go test ./...

# Run linter
golangci-lint run
```

### Project Structure

```
census3-bigquery/
├── cmd/service/           # Service entry point
├── internal/
│   ├── api/              # HTTP API server
│   ├── bigquery/         # BigQuery client and query registry
│   ├── census/           # Census creation client
│   ├── config/           # Configuration management
│   ├── service/          # Main service orchestrator
│   └── storage/          # Storage implementations
├── Dockerfile            # Container definition
├── docker-compose.yaml   # Local deployment
├── .env.example         # Configuration template
└── README.md            # This file
```

## License

[Add your license information here]

## Contributing

[Add contributing guidelines here]
