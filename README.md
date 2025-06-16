# Census3 BigQuery Service

A service that automatically creates Ethereum census snapshots by querying BigQuery for ETH balances and creating census trees on Vocdoni nodes. Supports both local storage and GitHub repository storage with compressed CSV files.

## Features

- **Automated Snapshots**: Periodic creation of census snapshots from Ethereum balance data
- **Multiple Balance Thresholds**: Support for multiple minimum ETH balance filters in a single run
- **Dual Storage Modes**: 
  - Local storage with HTTP API
  - GitHub repository storage with compressed CSV files
- **BigQuery Integration**: Efficient querying of Ethereum balance data
- **Vocdoni Census Creation**: Automatic census tree creation on Vocdoni nodes
- **Docker Support**: Complete containerization with docker-compose
- **Graceful Shutdown**: Proper signal handling and resource cleanup
- **Structured Logging**: Beautiful console output with zerolog

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
| `--max-count` | Maximum addresses to fetch | `100` |
| `--host` | Vocdoni node host URL | `http://localhost:8080` |
| `--batch-size` | Batch size for census creation | `5000` |
| `--github-enabled` | Enable GitHub storage mode | `false` |
| `--github-pat` | GitHub Personal Access Token | - |
| `--github-repo` | GitHub repository URL | - |

### Environment Variables

All flags can be configured via environment variables with the `CENSUS3_` prefix:

| Environment Variable | Flag Equivalent |
|---------------------|-----------------|
| `CENSUS3_PROJECT` | `--project` |
| `CENSUS3_MIN_BALANCES` | `--min-balances` |
| `CENSUS3_PERIOD` | `--period` |
| `CENSUS3_API_PORT` | `--api-port` |
| `CENSUS3_STORAGE_PATH` | `--storage-path` |
| `CENSUS3_MAX_COUNT` | `--max-count` |
| `CENSUS3_HOST` | `--host` |
| `CENSUS3_BATCH_SIZE` | `--batch-size` |
| `CENSUS3_GITHUB_ENABLED` | `--github-enabled` |
| `CENSUS3_GITHUB_PAT` | `--github-pat` |
| `CENSUS3_GITHUB_REPO` | `--github-repo` |

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
      "minBalance": 0.25
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
    ├── 2025-06-16-180000-ethereum-0.25.gz
    ├── 2025-06-16-180000-ethereum-1.00.gz
    └── ...
```

**Enhanced snapshots.json:**
```json
{
  "snapshots": [
    {
      "snapshotDate": "2025-06-16T18:00:00Z",
      "censusRoot": "0x832f31d1490ea413864da0be8ec8e962ab0e208a0ca25178c908b5ad22c83f12",
      "participantCount": 100,
      "createdAt": "2025-06-16T18:01:23Z",
      "minBalance": 0.25,
      "filename": "2025-06-16-180000-ethereum-0.25.gz"
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
- `2025-06-16-180000-ethereum-0.25.gz` (addresses with ≥0.25 ETH)
- `2025-06-16-180000-ethereum-1.00.gz` (addresses with ≥1.0 ETH)  
- `2025-06-16-180000-ethereum-5.00.gz` (addresses with ≥5.0 ETH)

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
CENSUS3_MAX_COUNT=10000
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
{"level":"info","period":3600000,"api_port":8080,"project":"my-project","github_enabled":true,"message":"Service configuration"}
{"level":"info","message":"Using Git storage mode - API server disabled"}
{"level":"info","message":"Running initial sync"}
{"level":"info","min_balance":0.25,"balance_index":1,"total_balances":3,"message":"Processing balance threshold"}
{"level":"info","participant_count":150,"min_balance":0.25,"message":"Fetched participants from BigQuery"}
{"level":"info","census_root":"0x832f...","processed_count":150,"min_balance":0.25,"message":"Census created successfully"}
{"level":"info","filename":"2025-06-16-180000-ethereum-0.25.gz","message":"Successfully pushed snapshot to Git repository"}
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
│   ├── bigquery/         # BigQuery client
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
