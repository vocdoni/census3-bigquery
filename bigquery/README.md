# Google BigQuery Ethereum Data – Public Datasets and Query Examples

## Introduction to Ethereum on BigQuery

Google BigQuery hosts public **Ethereum blockchain datasets** that are updated daily, enabling analytical SQL queries on on-chain data without running a node. The main Ethereum dataset (`bigquery-public-data.crypto_ethereum`) includes tables for blocks, transactions, token transfers, logs (events), smart contracts, account balances, and more. These tables capture the full history of Ethereum’s blockchain, from basic transaction data to contract bytecode and event logs. BigQuery allows OLAP-style queries over this data (using standard SQL), making it easy to answer questions about token transfers, contract activity, Ether balances, and other on-chain metrics.

**Available Ethereum-Based Datasets:** In addition to Ethereum **mainnet**, BigQuery provides datasets for other networks that share Ethereum’s technology:

* **Ethereum Mainnet** – Dataset: `crypto_ethereum` (full Ethereum blockchain).
* **Ethereum Classic** – Dataset: `crypto_ethereum_classic` (Ethereum Classic chain, using the same schema as Ethereum).
* **Ethereum Görli (Testnet)** – Dataset for the Goerli test network (public dataset added in 2023).
* **EVM-Compatible Chains** – Datasets for several popular chains that use EVM (Ethereum Virtual Machine) or similar smart contract models. For example: **Polygon** (`crypto_polygon` for Polygon mainnet, and `crypto_polygon_mumbai` for the Mumbai testnet), **Arbitrum** (`crypto_arbitrum`), **Optimism** (`crypto_optimism`), **Avalanche** (`crypto_avalanche`), **Fantom** (`crypto_fantom`), **Cronos** (`crypto_cronos`), and even **Tron** (`crypto_tron`). These datasets follow a unified schema, so queries are easily adaptable across networks. For instance, Ethereum and Ethereum Classic share the same table structure, making it simple to run the same query on different chains by changing the dataset name.

## Ethereum Blockchain Dataset Schema (Tables & Fields)

Google’s Ethereum public dataset organizes data into multiple tables. Here is a summary of each table and its key fields (columns):

* **Blocks** (`crypto_ethereum.blocks`): One row per block on the blockchain. Fields include:

  * `number` – Block number (sequence ID)
  * `hash` – Block hash (unique 256-bit identifier for the block)
  * `parent_hash` – Hash of the previous block (link in chain)
  * `timestamp` – Timestamp when block was mined (Unix epoch time)
  * `miner` – Address of the miner who produced the block
  * `difficulty` – Mining difficulty for the block
  * `total_difficulty` – Cumulative difficulty up to this block (sum of difficulties)
  * `size` – Block size in bytes
  * `gas_limit` – Maximum gas allowed in the block
  * `gas_used` – Total gas used by all transactions in the block
  * `transaction_count` – Number of transactions included in the block
  * `base_fee_per_gas` – Base fee per gas (Burn base fee introduced by EIP-1559)
  * Other fields: `nonce` (block nonce for proof-of-work), `sha3_uncles` (hash of uncle headers), `logs_bloom` (bloom filter for logs in this block), `transactions_root`, `state_root`, `receipts_root` (Merkle trie root hashes for block data structures), `extra_data` (miscellaneous data field in block), and (if applicable) fields for post-Merge data like withdrawal information (e.g., `withdrawals_root`) and blob gas fields for EIP-4844 (if present in the latest schema).

* **Transactions** (`crypto_ethereum.transactions`): All Ethereum **transactions** (external transactions) in every block. Each row is a transaction with fields:

  * `hash` – Transaction hash (unique ID of the transaction)
  * `nonce` – Transaction nonce (sender’s transaction count at this tx)
  * `transaction_index` – Index of this transaction within its block
  * `from_address` – Sender’s address (the wallet or contract that sent the tx)
  * `to_address` – Recipient’s address (the wallet or contract that received the tx; `NULL` if the tx created a contract)
  * `value` – Amount of Ether transferred in this transaction (in **wei**, the smallest unit)
  * `gas` – Gas limit provided by the sender for this transaction
  * `gas_price` – Gas price offered by the sender (in wei per gas unit)
  * `input` – Hex-encoded input data payload of the transaction (e.g. contract call data)
  * **Receipt fields** (transaction execution results, merged from the Ethereum receipt):

    * `receipt_cumulative_gas_used` – Total gas used in the block up to and including this tx.
    * `receipt_gas_used` – Gas used by this transaction alone (actual gas consumed).
    * `receipt_contract_address` – If this was a contract creation tx, this is the address of the newly created contract; otherwise `NULL`.
    * `receipt_root` – Post-transaction state root (pre-Byzantium hardfork; deprecated field).
    * `receipt_status` – Transaction execution status: `1` if successful, `0` if it **reverted/failed** (available for transactions after the Byzantium fork).
  * `block_timestamp` – Timestamp of the block containing this transaction.
  * `block_number` – Block number containing this transaction.
  * `block_hash` – Block hash containing this transaction (for completeness).
  * **EIP-1559 fields** (for transactions after the London upgrade):

    * `max_fee_per_gas` – Max total fee (in wei) the sender is willing to pay (sum of base fee + priority tip).
    * `max_priority_fee_per_gas` – Max priority fee (tip) the sender will pay the miner.
    * `transaction_type` – Transaction type (e.g. `0` for legacy, `2` for EIP-1559 types, etc.).
    * `receipt_effective_gas_price` – The actual gas price paid (post EIP-1559), i.e. base fee + tip, per gas unit.

* **Logs** (`crypto_ethereum.logs`): All **event logs** emitted by smart contracts, from every transaction’s receipt. Each log corresponds to an event (e.g. `Transfer` events from ERC-20 tokens). Important fields:

  * `log_index` – Index of the log within its block (each log has a unique index per block).
  * `transaction_hash` – Hash of the transaction that generated this log.
  * `transaction_index` – Index of the transaction within the block (that produced the log).
  * `address` – The contract address that emitted the log/event.
  * `data` – The event data payload (unindexed event parameters, in hex).
  * `topic0`, `topic1`, `topic2`, `topic3` – Topics of the log. `topic0` is typically the **event signature** (hash of the event name and types) for non-anonymous events, and `topic1-3` are additional indexed parameters of the event (topics may be `NULL` if not used). These fields let you filter logs by event type or indexed values.
  * `block_timestamp`, `block_number`, `block_hash` – Redundant references to the block context of this log (same timestamp, number, hash as the containing block).

* **Token Transfers** (`crypto_ethereum.token_transfers`): A specialized table that captures **ERC-20 and ERC-721 token transfer events** for convenience. Each row corresponds to a `Transfer` event from a token contract (denormalized from the logs table). Fields include:

  * `token_address` – The address of the token contract (ERC-20 or ERC-721 contract).
  * `from_address` – Sender address of the tokens (indexed `from` topic of the event).
  * `to_address` – Recipient address of the tokens (indexed `to` topic).
  * `value` – Amount of tokens transferred. For ERC-20 this is the token amount (in the token’s smallest unit), and for ERC-721 this may be the token ID of the NFT transferred.
  * `transaction_hash` – The transaction hash that triggered this token transfer event.
  * `log_index` – Index of the log within that transaction’s receipt (to uniquely identify the event).
  * `block_timestamp`, `block_number`, `block_hash` – The block context for the transfer (time and location in chain).

* **Contracts** (`crypto_ethereum.contracts`): Metadata on **smart contracts** that have been created on Ethereum. Each row represents a contract creation. Fields:

  * `address` – The contract’s address. (Note: This table may not enforce uniqueness on address alone because a contract address might appear multiple times if re-created via certain mechanisms, so consider composite key with block.)
  * `bytecode` – The runtime bytecode of the contract (hex string).
  * `function_sighashes` – A concatenated list of 4-byte function signature hashes found in the contract’s code (if available), which hints at what functions the contract implements.
  * `is_erc20` – Boolean, `TRUE` if the contract appears to implement an ERC-20 token interface.
  * `is_erc721` – Boolean, `TRUE` if the contract appears to be an ERC-721 (NFT) contract.
  * `block_number`, `block_timestamp`, `block_hash` – The block in which this contract was created (deployed). You can join this with the transactions table (via `transactions.receipt_contract_address`) to find the creation transaction.

* **Tokens** (`crypto_ethereum.tokens`): This is a list of known **ERC-20 tokens (and possibly other token standards)**, with metadata. Each row corresponds to a token contract (often overlapping with the Contracts table entries that are tokens). Fields:

  * `address` – Token contract address (same as contract address).
  * `symbol` – Token’s symbol (ticker), if known.
  * `name` – Token’s name, if known.
  * `decimals` – Number of decimal places the token uses (token precision).
  * `total_supply` – The total token supply (as of the last time data was updated).
  * `block_number`, `block_timestamp`, `block_hash` – Block context for when this token entry was recorded/updated. *(The dataset maintainers periodically update token info; an “amended\_tokens” view is available to combine official data with community-sourced token metadata.)*

* **Traces** (`crypto_ethereum.traces`): This table contains **internal transaction traces**, which are low-level actions (calls, value transfers, suicides, creates, rewards, etc.) that occur *within* transactions as a result of contract execution. These are obtained via Ethereum client tracing and include “internal transactions” that are not in the regular transactions list. Key fields include:

  * `transaction_hash` – The hash of the transaction that this trace belongs to (external transaction that initiated the internal calls).
  * `transaction_index` – Index of the external transaction in the block (same as in transactions table).
  * `from_address` – The caller or source address for this trace. This may be `NULL` for certain trace types (e.g. genesis, rewards).
  * `to_address` – The target address of the trace. Meaning varies by `trace_type`: for `call` it’s the called contract, for `create` it could be the new contract address (or null until created), for `suicide` it’s the beneficiary, etc..
  * `value` – Amount of Ether (wei) transferred in this trace (for call/create/selfdestruct value transfers).
  * `input` – Call input data (for call/create traces; hex data sent to a contract).
  * `output` – Call output or return data (for completed calls; for `create` it’s the created contract bytecode).
  * `trace_type` – Type of trace: e.g. `"call"`, `"create"`, `"suicide"` (self-destruct), `"reward"`, `"genesis"`, `"daofork"`.
  * `call_type` – Subtype of call, if applicable (e.g. `call`, `callcode`, `delegatecall`, `staticcall` for `trace_type="call"`).
  * `reward_type` – Type of miner reward if `trace_type="reward"` (e.g. "block" or "uncle").
  * `gas` – Gas provided for this internal call/operation.
  * `gas_used` – Gas used by this trace (internal operation).
  * `subtraces` – Number of sub-traces (calls made within this call, indicating depth of call tree).
  * `trace_address` – Sequence of integers (as a comma-separated list) showing the call depth path to this trace. Example: `"[0,2,1]"` denotes this is trace index 1 inside trace index 2 inside trace index 0 of the transaction.
  * `error` – Error message if this call failed (e.g. out of gas or revert reason).
  * `status` – 1/0 success or failure (similar to receipt\_status but for internal call; e.g. 0 if this trace threw an error).
  * `block_timestamp`, `block_number`, `block_hash` – Block context for the trace (the block in which the parent transaction was mined).
  * `trace_id` – Unique ID string for the trace (concatenating type and tx or block info, not commonly used in queries).

* **Balances** (`crypto_ethereum.balances`): This table contains the **current Ether balance** of every address, updated daily. Fields:

  * `address` – Ethereum address (EOA or contract) in hex format.
  * `eth_balance` – The ETH balance for that address (in wei, as a BigQuery NUMERIC).
    *Note:* This table is essentially a snapshot of all account balances and is refreshed regularly. It does not contain historical balance changes or token balances (only native Ether). To get token balances or historical balances, you would need to aggregate transactions or token transfers over time.

* **Other tables**: The dataset may include some internal tables such as `sessions` and `load_metadata` (used for ETL bookkeeping). These are not generally queried for blockchain analysis. There is also an **“amended\_tokens”** view that merges the `tokens` table with external token metadata fixes (ensuring unique tokens by address and adding any missing names/symbols). Most analytics focus on the main tables described above.

Each table is either partitioned by date or block (for performance). For example, the transactions table is partitioned by `block_timestamp` (date), so adding a date filter in the `WHERE` clause (e.g. `WHERE DATE(block_timestamp) = '2025-06-17'`) will make queries more efficient. Similarly, logs, token\_transfers, traces, etc., are partitioned by block timestamp or block number. Using these partition fields in queries is recommended to reduce scan costs on the huge tables.

## Example SQL Queries for Ethereum Data

Below is a collection of **SQL query examples** demonstrating how to retrieve various insights from the Ethereum blockchain data on BigQuery. These queries are written for Ethereum mainnet (dataset `crypto_ethereum`), but you can run them on other networks by substituting the dataset name (for instance, use `crypto_polygon` for Polygon data, etc.). Each query is accompanied by a brief description of what it does. You can run these in the BigQuery console’s query editor.

### 1. Top Accounts by Ether Balance (Rich List)

Find the addresses with the largest Ether balances. This query selects all addresses with a balance above a given threshold and orders them by balance descending. You can set a parameter `@min_balance` (in wei) to filter addresses by minimum balance:

```sql
SELECT address, eth_balance
FROM `bigquery-public-data.crypto_ethereum.balances`
WHERE eth_balance >= @min_balance
ORDER BY eth_balance DESC;
```

*Example:* To get the top 10 richest addresses, you could set `@min_balance` to a very high value or simply remove the `WHERE` clause and use `LIMIT 10`. This yields the addresses holding the most ETH (excluding the null address if present). The `balances` table is updated daily, so it always reflects the latest state.

### 2. Latest Ether Balance of Specific Addresses

Retrieve the ETH balance for specific addresses of interest. For example, to get the balance of two particular addresses (replace with actual addresses):

```sql
SELECT address, eth_balance
FROM `bigquery-public-data.crypto_ethereum.balances`
WHERE address IN (
    "0x742d35Cc6634C0532925a3b844Bc454e4438f44e",  -- Example: a known address
    "0x00000000219ab540356cBB839Cbe05303d7705Fa"   -- Example: another address
);
```

This will return the current ETH balances for the addresses listed (common use-case: checking exchange cold wallet balances, DeFi contract holdings, etc.).

### 3. Daily Transaction Count on Ethereum

Calculate how many transactions are executed per day on Ethereum. This query groups the transactions by date and counts them:

```sql
SELECT DATE(block_timestamp) AS date, COUNT(*) AS tx_count
FROM `bigquery-public-data.crypto_ethereum.transactions`
GROUP BY date
ORDER BY date DESC
LIMIT 30;
```

This will list the transaction counts for the last 30 days (you can adjust the date range or remove the LIMIT to see all days). It uses the `block_timestamp` partition to efficiently aggregate by day. You can also filter by date range, e.g. `WHERE DATE(block_timestamp) BETWEEN '2025-06-01' AND '2025-06-30'` for a specific month.

### 4. Daily Transaction Count – Ethereum vs. Ethereum Classic (Comparison)

Using similar schema across networks, compare metrics between chains. For example, to compare the number of transactions per day on Ethereum mainnet vs Ethereum Classic over the past week:

```sql
SELECT 
  "Ethereum Mainnet" AS network, 
  DATE(block_timestamp) AS date, 
  COUNT(*) AS tx_count
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY network, date

UNION ALL

SELECT 
  "Ethereum Classic" AS network, 
  DATE(block_timestamp) AS date, 
  COUNT(*) AS tx_count
FROM `bigquery-public-data.crypto_ethereum_classic.transactions`
WHERE DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY network, date

ORDER BY date DESC, network;
```

This query uses two SELECTs (one for each chain’s dataset) and combines them. The result shows, for each of the last 7 days, how many transactions occurred on Ethereum vs Ethereum Classic. Such comparative analysis is possible because both datasets share an identical structure.

### 5. Most Active ERC-20 Token Contracts (by Transaction Count)

Find which ERC-20 token contracts have the highest number of transactions involving them. “Transactions involving a token contract” typically means either transferring the token or any call to the token’s contract methods. We can identify ERC-20 contracts via the `is_erc20` flag in the contracts table. Then count how many transactions either called these contracts or created them:

```sql
SELECT c.address AS token_contract, COUNT(DISTINCT t.hash) AS tx_count
FROM `bigquery-public-data.crypto_ethereum.contracts` AS c
JOIN `bigquery-public-data.crypto_ethereum.transactions` AS t
  ON c.address = t.to_address OR c.address = t.receipt_contract_address
WHERE c.is_erc20 = TRUE
GROUP BY token_contract
ORDER BY tx_count DESC
LIMIT 10;
```

This joins the contracts and transactions tables, counting any transaction where the contract’s address appears as the `to_address` (a call to the contract) or as the `receipt_contract_address` (the contract was created in that transaction). We filter for contracts known to be ERC-20 tokens. The result is the **top 10 ERC-20 token contracts by total transaction count**. (Often, high on this list are popular tokens like USDT, USDC, DAI, etc., as well as utility tokens that are frequently transacted.)

### 6. Most Active ERC-721 (NFT) Contracts (by Transaction Count)

Similarly, find the top ERC-721 collectible contracts by number of transactions. This query is analogous to the above but filters `is_erc721`:

```sql
SELECT c.address AS nft_contract, COUNT(DISTINCT t.hash) AS tx_count
FROM `bigquery-public-data.crypto_ethereum.contracts` AS c
JOIN `bigquery-public-data.crypto_ethereum.transactions` AS t
  ON c.address = t.to_address OR c.address = t.receipt_contract_address
WHERE c.is_erc721 = TRUE
GROUP BY nft_contract
ORDER BY tx_count DESC
LIMIT 10;
```

This yields the **10 most popular NFT contracts** by transaction count. (For example, CryptoKitties’ contract is known to be among the top by transaction volume.) Such a query helps identify which NFT projects have seen the most on-chain activity.

### 7. Daily Token Transfer Counts for a Specific Token

Measure usage of a specific token over time. For example, consider the ERC-20 token **OMG (OmiseGO)** with contract address `0xd26114cd6ee289accf82350c8d8487fedb8a0c07`. We can count daily transfer events for OMG:

```sql
SELECT DATE(block_timestamp) AS date, COUNT(*) AS transfers
FROM `bigquery-public-data.crypto_ethereum.token_transfers`
WHERE token_address = "0xd26114cd6ee289accf82350c8d8487fedb8a0c07"
GROUP BY date
ORDER BY date;
```

This will give the number of OMG token transfers per day from inception to present. We could further filter by date range or visualize this as a time series. In fact, a similar query was used to observe the OMG airdrop on September 13, 2017 (a spike in receivers with no spike in senders).

### 8. Latest 10 Transactions Involving a Given Address

To fetch recent activity for a particular address (either as sender or receiver), you can query the transactions table by `from_address` or `to_address`. For example, for address `0xcda7559bcef42e68f16233b5b8c99c757a5f4697` (replace with any address of interest):

```sql
SELECT *
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE from_address = "0xcda7559bcef42e68f16233b5b8c99c757a5f4697"
   OR to_address   = "0xcda7559bcef42e68f16233b5b8c99c757a5f4697"
ORDER BY block_timestamp DESC
LIMIT 10;
```

This returns the 10 most recent transactions **from or to** the given address (could be a user wallet or contract). It’s useful for checking an address’s activity (e.g., monitoring a whale wallet or contract interactions).

### 9. Latest 10 Token Transfers Involving a Given Address

Likewise, to get recent ERC-20 token transfers for a specific address (either sending or receiving tokens):

```sql
SELECT *
FROM `bigquery-public-data.crypto_ethereum.token_transfers`
WHERE from_address = "0xcda7559bcef42e68f16233b5b8c99c757a5f4697"
   OR to_address   = "0xcda7559bcef42e68f16233b5b8c99c757a5f4697"
ORDER BY block_timestamp DESC
LIMIT 10;
```

This will list the latest token transfer events involving that address (could be outgoing transfers or incoming tokens). Each result includes which token (`token_address`) and the amount (`value`) as well as the time. It’s a quick way to see what tokens a given address is transacting with.

### 10. Latest Transfers of a Specific Token (e.g. USDT)

To focus on a single token, filter by its contract address. For example, Tether USD (USDT) has token contract `0xdAC17F958D2ee523a2206206994597C13D831ec7`. The query for recent USDT transfers:

```sql
SELECT *
FROM `bigquery-public-data.crypto_ethereum.token_transfers`
WHERE token_address = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
  AND block_timestamp > TIMESTAMP "2023-09-04 00:00:00"
ORDER BY block_timestamp DESC
LIMIT 10;
```

This finds the latest 10 USDT transfer events after September 4, 2023. You can adjust the date or remove that condition to get the absolute latest transfers. The results show USDT moving between addresses, which can be useful for tracking large stablecoin transactions.

### 11. Top Gas-Spenders: Transactions with the Highest Gas Used

Identify the transactions that consumed the most gas. We can use the `receipt_gas_used` field for each transaction:

```sql
SELECT hash, block_number, from_address, to_address, receipt_gas_used
FROM `bigquery-public-data.crypto_ethereum.transactions`
ORDER BY receipt_gas_used DESC
LIMIT 10;
```

This returns the top 10 transactions by gas used. These might include contract execution transactions that ran very complex operations or hit the block gas limit. (For context, the maximum gas per block is fixed, so very high gas\_used transactions are usually near the block limit.)

### 12. Average Gas Price Per Day

Calculate the average gas price (in Gwei) of transactions each day:

```sql
SELECT 
  DATE(block_timestamp) AS date, 
  AVG(gas_price) / 1e9 AS avg_gas_price_gwei
FROM `bigquery-public-data.crypto_ethereum.transactions`
GROUP BY date
ORDER BY date DESC
LIMIT 30;
```

This computes the average gas price per day over the last 30 days (adjust or remove LIMIT as needed). We divide by 1e9 to convert wei to Gwei for readability (1 Gwei = 1e9 wei). This query helps track how network gas prices fluctuate over time (e.g., identifying days of high congestion).

### 13. Internal Ether Transfers (Value Transfers from Smart Contracts)

Using the **traces** table, we can find internal transactions where Ether was transferred within contract execution. For example, list the internal transfers (calls) within a specific transaction that carried value:

```sql
SELECT 
  t.transaction_hash, 
  t.from_address, 
  t.to_address, 
  t.value AS wei_value, 
  t.status
FROM `bigquery-public-data.crypto_ethereum.traces` AS t
WHERE t.transaction_hash = "<TX_HASH_OF_INTEREST>"
  AND t.trace_type = "call"        -- focusing on call traces
  AND t.value > 0                 -- only include value-transferring calls
ORDER BY t.trace_address;
```

Replace `<TX_HASH_OF_INTEREST>` with an actual transaction hash. This will list the internal calls in that transaction that moved Ether (value > 0). It includes the from/to addresses of each call, the value in wei, and the status (1 for success, 0 if that internal call failed). This replicates what block explorers call **“internal transactions”** (which are value transfers from smart contracts). If you omit the `transaction_hash` filter, you could retrieve *all* internal transfers in the chain, but that would be an extremely large result – instead, you might aggregate or filter by block or address.

*Example use:* You could change the query to find all internal transfers involving a certain address by filtering `from_address` or `to_address` in the traces table (similar to how we did for external transactions). Keep in mind not all internal calls carry value; many are just function calls with zero Ether transferred.

### 14. Contracts Created per Day and Average Bytecode Size

Using the contracts and transactions tables together, one can analyze contract deployments. For example, to see how many contracts are created each day and the average size of their bytecode:

```sql
SELECT 
  DATE(t.block_timestamp) AS date,
  COUNT(*) AS contracts_created,
  AVG(LENGTH(c.bytecode)) AS avg_bytecode_length
FROM `bigquery-public-data.crypto_ethereum.contracts` AS c
JOIN `bigquery-public-data.crypto_ethereum.transactions` AS t
  ON c.address = t.receipt_contract_address
WHERE t.receipt_contract_address IS NOT NULL
GROUP BY date
ORDER BY date DESC
LIMIT 30;
```

This query finds contracts created (i.e., transactions where `receipt_contract_address` is not null) per day and calculates the average bytecode length of those contracts. It can reveal trends such as spikes in contract deployments or changes in smart contract code size over time.

### 15. Top Miners by Blocks Mined

Determine which mining addresses mined the most blocks in a given period. For example, overall top 5 miners:

```sql
SELECT miner, COUNT(*) AS blocks_mined
FROM `bigquery-public-data.crypto_ethereum.blocks`
GROUP BY miner
ORDER BY blocks_mined DESC
LIMIT 5;
```

This yields the addresses (mining accounts) that have mined the most blocks in the dataset. We could add a `WHERE` clause on the block timestamp to focus on a specific timeframe (e.g., `WHERE timestamp >= '2021-01-01'`). Note that after Ethereum’s merge to Proof-of-Stake (Sep 2022), mining stopped and “miner” field reflects validators (but the field name remains `miner` in the dataset).

### 16. Cross-Chain Query Example – Total Daily Transactions on Multiple Chains

Thanks to BigQuery’s ability to query across datasets, you can combine data from multiple blockchains. For instance, compare daily transaction counts across three EVM chains (Ethereum, Polygon, and Avalanche) in one query:

```sql
SELECT date, network, tx_count
FROM (
  SELECT DATE(block_timestamp) AS date, "Ethereum" AS network, COUNT(*) AS tx_count
  FROM `bigquery-public-data.crypto_ethereum.transactions`
  WHERE DATE(block_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY date
  
  UNION ALL
  
  SELECT DATE(block_timestamp) AS date, "Polygon" AS network, COUNT(*) AS tx_count
  FROM `bigquery-public-data.crypto_polygon.transactions`
  WHERE DATE(block_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY date
  
  UNION ALL
  
  SELECT DATE(block_timestamp) AS date, "Avalanche" AS network, COUNT(*) AS tx_count
  FROM `bigquery-public-data.crypto_avalanche.transactions`
  WHERE DATE(block_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY date
)
ORDER BY network;
```

This example compares yesterday’s transaction counts on Ethereum vs. Polygon vs. Avalanche. You can extend it to other networks or different metrics. It highlights the capability to do **cross-chain analytics** using BigQuery, since many chains are available in the public dataset program. The unified schema means the queries are structurally similar for each chain, differing only by the project/dataset name.

