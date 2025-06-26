# Census3 BigQuery Service

A service that automatically creates Ethereum census snapshots by querying BigQuery for ETH balances and creating census merkle-trees for Vocdoni. Features a unified KV storage system with HTTP API for accessing snapshots and census data.

## Features

- **YAML-Based Query Configuration**: Flexible query management with user-defined names and independent scheduling
- **Multiple Query Support**: Run multiple queries simultaneously with different parameters and periods
- **Automated Snapshots**: Periodic creation of census snapshots from Ethereum balance data
- **Modular BigQuery System**: Choose from multiple predefined queries or add custom ones
- **HTTP API**: RESTful API for accessing snapshots and census data

## Configuration

### YAML-Based Query Configuration

The service uses a YAML configuration file to define multiple queries with independent schedules and parameters. 

1. **Copy the example file**:
   ```bash
   cp queries.yaml.example queries.yaml
   ```

2. **Edit the configuration**:
   ```yaml
   # queries.yaml
   queries:
     # Ethereum balance snapshots with different weight strategies
     - name: ethereum_holders_equal_voting
       query: ethereum_balances
       period: 1h
       decimals: 18  # ETH has 18 decimals
       parameters:
         min_balance: 0.01  # 0.01 ETH minimum (human-readable)
       weight:
         strategy: "constant"
         constant_weight: 1  # Everyone gets 1 vote regardless of balance
         
     - name: ethereum_holders_quadratic
       query: ethereum_balances
       period: 1h
       decimals: 18
       parameters:
         min_balance: 0.01  # 0.01 ETH minimum
       weight:
         strategy: "proportional_auto"
         target_min_weight: 1  # 0.01 ETH = 1 point, 1 ETH = 100 points
         max_weight: 10000     # Cap at 10,000 points to prevent whales
         
     # ERC20 token holders with proper decimal handling
     - name: usdc_holders_proportional
       query: erc20_holders
       period: 30m
       decimals: 6  # USDC has 6 decimals
       parameters:
         token_address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # USDC
         min_balance: 100  # 100 USDC (human-readable)
       weight:
         strategy: "proportional_auto"
         target_min_weight: 1  # 100 USDC = 1 point
         max_weight: 1000      # Cap at 1000 points
         
     - name: dai_holders_custom
       query: erc20_holders
       period: 2h
       decimals: 18  # DAI has 18 decimals
       parameters:
         token_address: "0x6b175474e89094c44da98b954eedeac495271d0f"  # DAI
         min_balance: 50  # 50 DAI (human-readable)
       weight:
         strategy: "proportional_manual"
         multiplier: 0.1  # 50 DAI = 5 points, 500 DAI = 50 points
         
     # Recent activity without balance requirements
     - name: ethereum_active_users
       query: ethereum_recent_activity
       period: 24h
       parameters: {}  # No parameters needed - finds all active addresses
       weight:
         strategy: "constant"
         constant_weight: 1  # Equal voting for all active users
   ```

Use `--list-queries` to see all available queries:

```bash
go run ./cmd/service --list-queries
```

#### Query Configuration Fields

- **`name`**: User-defined identifier for this query instance (used in logs and API responses)
- **`query`**: BigQuery query name from the registry (must exist in `bigquery/queries.go`)
- **`period`**: How often to run this query (e.g., `1h`, `30m`, `2h`)
- **`disabled`**: Optional boolean to disable synchronization while keeping existing snapshots accessible (default: false)
- **`syncOnStart`**: Optional boolean to control startup sync behavior (default: false)
- **`decimals`**: Token decimals for conversion (18 for ETH, 6 for USDC, etc.) - optional with smart defaults
- **`parameters`**: Query-specific parameters including `min_balance` in human-readable units
- **`weight`**: Weight calculation configuration for census creation - optional, defaults to proportional_manual with multiplier 100


#### Weight Configuration Strategies

The `weight` field supports three strategies for converting token balances into integer weights suitable for voting:

**1. Constant Weight (Equal Voting)**
```yaml
weight:
  strategy: "constant"
  constant_weight: 1  # Everyone gets the same weight regardless of balance
```
- Use case: Democratic voting where each holder gets equal voting power
- Perfect for governance scenarios requiring equal representation

**2. Proportional Auto (Quadratic Voting)**
```yaml
weight:
  strategy: "proportional_auto"
  target_min_weight: 1     # Weight for addresses with min_balance
  max_weight: 10000        # Optional cap to prevent whale dominance
```
- Use case: Quadratic voting with automatic scaling
- Formula: `weight = (balance / min_balance) * target_min_weight`
- Example: min_balance=0.01 ETH, target_min_weight=1 → 0.01 ETH=1 point, 1 ETH=100 points

**3. Proportional Manual (Custom Multiplier)**
```yaml
weight:
  strategy: "proportional_manual"
  multiplier: 10.0         # Custom multiplier for balance
```
- Use case: Custom weight calculations
- Formula: `weight = balance * multiplier`
- Example: multiplier=10 → 1 ETH=10 points, 10 ETH=100 points

#### Human-Readable Balance Configuration

All `min_balance` values are specified in human-readable units:
- **ETH**: `min_balance: 0.01` means 0.01 ETH
- **USDC**: `min_balance: 100` means 100 USDC (with `decimals: 6`)
- **DAI**: `min_balance: 50` means 50 DAI (with `decimals: 18`)

The system automatically converts between human-readable and raw blockchain values using the `decimals` field.

#### Cost Control

Add simple cost protection to prevent expensive queries:

```yaml
queries:
  - name: my_query
    query: ethereum_balances
    period: 6h
    estimate_first: true      # Enable cost estimation
    cost_preset: "default"   # Use default limits
    parameters:
      min_balance: 1.0
    weight:
      strategy: "constant"
      constant_weight: 1
```

**Cost Presets:**
- `"conservative"` - 1GB/$0.10 limits
- `"default"` - 100GB/$5.00 limits   
- `"high_volume"` - 1TB/$50.00 limits 
- `"none"` - No limits

## HTTP API

The service provides a RESTful API for accessing snapshots and census data:

#### `GET /snapshots`
List all snapshots with pagination and filtering support.

**Query Parameters:**
- `page` (int): Page number (default: 1)
- `pageSize` (int): Items per page (default: 20, max: 100)
- `minBalance` (float): Filter by minimum balance
- `queryName` (string): Filter by user-defined query name

**Example:**
```bash
curl "http://localhost:8080/snapshots?page=1&pageSize=10&minBalance=1.0"
```

**Response:**
```json
{
  "snapshots": [
    {
      "snapshotDate": "2025-06-18T00:00:00Z",
      "censusRoot": "0x832f31d1490ea413864da0be8ec8e962ab0e208a0ca25178c908b5ad22c83f12",
      "participantCount": 150,
      "minBalance": 1.0,
      "queryName": "ethereum_holders_quadratic",
      "queryType": "ethereum_balances",
      "decimals": 18,
      "period": "1h",
      "parameters": {
        "min_balance": 1.0
      },
      "weightConfig": {
        "strategy": "proportional_auto",
        "targetMinWeight": 1,
        "maxWeight": 10000
      },
      "createdAt": "2025-06-18T00:01:23Z"
    }
  ],
  "total": 25,
  "page": 1,
  "pageSize": 10,
  "hasNext": true,
  "hasPrev": false
}
```

#### `GET /snapshots/latest`
Get the most recent snapshot.

**Example:**
```bash
curl "http://localhost:8080/snapshots/latest"
```

#### `GET /censuses/{root}/size`
Get the number of participants in a census by its merkle root.

**Example:**
```bash
curl "http://localhost:8080/censuses/0x832f31d1490ea413864da0be8ec8e962ab0e208a0ca25178c908b5ad22c83f12/size"
```

**Response:**
```json
{
  "size": 150
}
```

#### `GET /censuses/{root}/proof?key={hexKey}`
Generate a merkle proof for a specific key in the census.

**Example:**
```bash
curl "http://localhost:8080/censuses/0x832f.../proof?key=0x742d35Cc6634C0532925a3b8D4C9db96"
```

**Response:**
```json
{
  "root": "0x832f31d1490ea413864da0be8ec8e962ab0e208a0ca25178c908b5ad22c83f12",
  "key": "0x742d35Cc6634C0532925a3b8D4C9db96",
  "value": "0x64",
  "siblings": ["0x...", "0x..."],
  "weight": "100"
}
```

### Health Endpoint

#### `GET /health`
Service health check.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-06-18T00:00:00Z",
  "service": "census3-bigquery"
}
```

## Custom Census Creation

The service provides API endpoints for creating custom censuses with manual participant management. This allows building censuses outside of the automated BigQuery workflow.

### Working vs Published Censuses

- **Working Censuses**: Identified by UUID, mutable, can add participants up to 1M limit
- **Published Censuses**: Identified by merkle root, immutable, space-optimized for proofs

### Census Management Endpoints

#### `POST /censuses`
Create a new working census.

**Response:**
```json
{
  "census": "550e8400-e29b-41d4-a716-446655440000"
}
```

#### `POST /censuses/{censusId}/participants`
Add participants to a working census (max 1M total).

**Request:**
```json
{
  "participants": [
    {
      "key": "0x742d35Cc6634C0532925a3b8D4C9db96",
      "weight": "100"
    },
    {
      "key": "0x8ba1f109551bD432803012645Hac136c",
      "weight": "50"
    }
  ]
}
```

**Notes:**
- Keys longer than 20 bytes are automatically hashed
- Weight defaults to 1 if not provided
- Returns HTTP 400 if census size limit exceeded

#### `GET /censuses/{censusId}/root`
Get the merkle root of a working census.

**Response:**
```json
{
  "root": "0x832f31d1490ea413864da0be8ec8e962ab0e208a0ca25178c908b5ad22c83f12"
}
```

#### `POST /censuses/{censusId}/publish`
Publish working census to immutable root-based census.

**Response:**
```json
{
  "root": "0x832f31d1490ea413864da0be8ec8e962ab0e208a0ca25178c908b5ad22c83f12",
  "participantCount": 150,
  "createdAt": "2025-06-18T00:00:00Z",
  "publishedAt": "2025-06-18T00:01:23Z"
}
```

**Process:**
1. Creates space-optimized root-based census
2. Transfers data using export/import for efficiency
3. Verifies root integrity
4. Cleans up working census in background

#### `DELETE /censuses/{censusId}`
Delete a working census (only UUID-based censuses can be deleted).

#### `GET /censuses/{censusId}/participants`
List participants in a working census (placeholder - not yet implemented).

### Configuration

#### `CENSUS3_MAX_CENSUS_SIZE`
Maximum participants per census (default: 1,000,000).

**Environment variable:**
```env
CENSUS3_MAX_CENSUS_SIZE=1000000
```

**Command line:**
```bash
--max-census-size=1000000
```

### Example Workflow

```bash
# 1. Create working census
CENSUS_ID=$(curl -X POST http://localhost:8080/censuses | jq -r '.census')

# 2. Add participants
curl -X POST http://localhost:8080/censuses/$CENSUS_ID/participants \
  -H "Content-Type: application/json" \
  -d '{
    "participants": [
      {"key": "0x742d35Cc6634C0532925a3b8D4C9db96", "weight": "100"},
      {"key": "0x8ba1f109551bD432803012645Hac136c", "weight": "50"}
    ]
  }'

# 3. Get root
ROOT=$(curl http://localhost:8080/censuses/$CENSUS_ID/root | jq -r '.root')

# 4. Publish census
curl -X POST http://localhost:8080/censuses/$CENSUS_ID/publish

# 5. Generate proof using published census
curl "http://localhost:8080/censuses/$ROOT/proof?key=0x742d35Cc6634C0532925a3b8D4C9db96"
```

## Step-by-step setup with Google Cloud configuration

This service requires access to Google Cloud BigQuery to query Ethereum balance data. Follow these step-by-step instructions to set up your Google Cloud project and configure authentication.

### Prerequisites

- A Google Cloud account
- Billing enabled on your Google Cloud account (BigQuery requires billing)
- Docker and Docker Compose installed (for containerized deployment)

### Step 1: Install Google Cloud CLI

#### On macOS (using Homebrew)
```bash
brew install --cask google-cloud-sdk
```

#### On Ubuntu/Debian
```bash
# Add the Cloud SDK distribution URI as a package source
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# Import the Google Cloud public key
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

# Update and install the Cloud SDK
sudo apt-get update && sudo apt-get install google-cloud-cli
```

### Step 2: Initialize gcloud and Authenticate

```bash
# Initialize gcloud (this will open a browser for authentication)
gcloud init

# Alternatively, authenticate separately
gcloud auth login

# Set your default project (optional, can be done in step 3)
gcloud config set project YOUR_PROJECT_ID
```

### Step 3: Create a Google Cloud Project

#### Option A: Create via gcloud CLI
```bash
# Create a new project
gcloud projects create census3-bigquery-project --name="Census3 BigQuery Service"

# Set as default project
gcloud config set project census3-bigquery-project

# Enable billing (replace BILLING_ACCOUNT_ID with your billing account)
gcloud billing projects link census3-bigquery-project --billing-account=BILLING_ACCOUNT_ID
```

#### Option B: Create via Google Cloud Console
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click "Select a project" → "New Project"
3. Enter project name: `Census3 BigQuery Service`
4. Note the generated Project ID (e.g., `census3-bigquery-project-123456`)
5. Enable billing for the project

### Step 4: Enable Required APIs

```bash
# Enable BigQuery API
gcloud services enable bigquery.googleapis.com

# Verify the API is enabled
gcloud services list --enabled --filter="name:bigquery"
```

### Step 5: Create a Service Account

```bash
# Create a service account
gcloud iam service-accounts create census3-bigquery-sa \
    --display-name="Census3 BigQuery Service Account" \
    --description="Service account for Census3 BigQuery operations"

# Get your project ID
PROJECT_ID=$(gcloud config get-value project)

# Grant BigQuery permissions to the service account
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:census3-bigquery-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:census3-bigquery-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataViewer"
```

### Step 6: Generate Service Account Key

```bash
# Create and download the service account key
gcloud iam service-accounts keys create ./gcp-service-account-key.json \
    --iam-account=census3-bigquery-sa@$PROJECT_ID.iam.gserviceaccount.com

# Verify the key was created
ls -la gcp-service-account-key.json
```

**⚠️ Security Note**: Keep this key file secure and never commit it to version control!

### Step 7: Test BigQuery Access

```bash
# Set the credentials environment variable
export GOOGLE_APPLICATION_CREDENTIALS="./gcp-service-account-key.json"

# Test BigQuery access
bq ls

# Or test with gcloud
gcloud auth activate-service-account --key-file=./gcp-service-account-key.json
gcloud auth list
```

## Docker Compose Configuration

Now that you have your Google Cloud project set up, configure Docker Compose to use your credentials via environment variables.

### Using Base64 Encoded Service Account Key

This method stores the service account key as a base64-encoded environment variable, which is secure and doesn't require mounting files.

1. **Convert your service account key to base64**:
   ```bash
   # Convert the JSON key to base64 (single line, no wrapping)
   base64 -w 0 gcp-service-account-key.json > gcp-key-base64.txt
   
   # Display the base64 content to copy
   cat gcp-key-base64.txt
   ```

2. **Update your `.env` file**:
   ```bash
   # Copy the example environment file
   cp .env.example .env
   
   # Edit the .env file
   nano .env
   ```

3. **Configure the `.env` file**:
   ```env
   # Required: Your GCP Project ID
   CENSUS3_PROJECT=census3-bigquery-project-123456
   
   # Google Cloud Credentials (Base64 encoded service account key)
   GOOGLE_APPLICATION_CREDENTIALS_JSON=ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiY2Vuc3VzMy1iaWdxdWVyeS1wcm9qZWN0LTEyMzQ1NiIsCiAgInByaXZhdGVfa2V5X2lkIjogIjEyMzQ1NiIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuLi4uXG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAiY2Vuc3VzMy1iaWdxdWVyeS1zYUBjZW5zdXMzLWJpZ3F1ZXJ5LXByb2plY3QtMTIzNDU2LmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAiY2xpZW50X2lkIjogIjEyMzQ1Njc4OTAiLAogICJhdXRoX3VyaSI6ICJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20vby9vYXV0aDIvYXV0aCIsCiAgInRva2VuX3VyaSI6ICJodHRwczovL29hdXRoMi5nb29nbGVhcGlzLmNvbS90b2tlbiIsCiAgImF1dGhfcHJvdmlkZXJfeDUwOV9jZXJ0X3VybCI6ICJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9vYXV0aDIvdjEvY2VydHMiLAogICJjbGllbnRfeDUwOV9jZXJ0X3VybCI6ICJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9yb2JvdC92MS9tZXRhZGF0YS94NTA5L2NlbnN1czMtYmlncXVlcnktc2ElNDBjZW5zdXMzLWJpZ3F1ZXJ5LXByb2plY3QtMTIzNDU2LmlhbS5nc2VydmljZWFjY291bnQuY29tIgp9
   
   # Service Configuration
   CENSUS3_API_PORT=8080
   CENSUS3_BATCH_SIZE=10000
   CENSUS3_DATA_DIR=/app/.bigcensus3
   CENSUS3_QUERIES_FILE=/app/queries.yaml
   
   # Docker Configuration
   RESTART=unless-stopped
   ```

   **⚠️ Important**: Replace the example `GOOGLE_APPLICATION_CREDENTIALS_JSON` value with your actual base64-encoded service account key from step 1.

4. **Create your queries configuration**:
   ```bash
   cp queries.yaml.example queries.yaml
   # Edit queries.yaml with your desired query configurations
   ```

5. **Start the service**:
   ```bash
   docker-compose up -d
   
   # Check logs to verify authentication works
   docker-compose logs -f census3-service
   ```
