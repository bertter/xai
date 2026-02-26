# Fetcher + Tracker System (Fetch & Track) — Overview

This document explains the architecture of a system that **fetches on-chain transaction data** from blockchain nodes/RPC (or APIs such as Etherscan) and **tracks fund flows** starting from specific target addresses (e.g., suspect/interest addresses).

---

## 1) High-level Architecture

### A. Fetcher Layer (Data Collection Pipeline)
- Purpose: Pull transactions from coin-specific nodes (RPC), parse them, and buffer them in memory (e.g., `store.data['buffer'][coin]`) in **chunk units**.
- Characteristics: Uses `asyncio` for **parallel/batched RPC calls**, and includes controls to limit memory growth.

### B. Tracker Layer (Tracing / Analysis Engine)
- Purpose: Starting from one or more target addresses:
  1) inspect balances / tx counts / exchange labels,  
  2) expand through transactions (address discovery),  
  3) produce **alerts / reports / graphs (i2 ANX)** for suspicious flows (e.g., exchange deposits).
- Characteristics: Supports both **DB-backed tracking** (with clustering/labels) and **API-backed tracking**.

---

## 2) Fetcher Design

### 2.1 Core responsibilities of `FetcherBase`
`FetcherBase` (the shared base class) provides the common processing loop, buffering, chunk orchestration, and partial/batched fetching.

Key parameters (defaults observed in code):
- `sleepSec = 10`: wait time when no new blocks are available
- `prepareChunkCount = 3`: maximum number of completed chunks retained in the done buffer
- `processBlockCount = 50`: number of blocks per “chunk window”
- `processBLockPartialCount = processBlockCount / 5`: split a chunk into smaller partial ranges for parallel fetching

### 2.2 `Process()` loop: “blocks → chunks → buffers”
The fetch cycle repeats as follows:

1. **Read the latest chain tip**
   - `currentBlockNumber = getLastBlockNumber() - 5`
   - The `-5` offset is a simple buffer against reorgs/forks (code comments indicate room for improvement).

2. **Split the range from `lastBlockNumber` to `currentBlockNumber` into chunks**
   - Example: with `processBlockCount=50`, create ranges of 50 blocks.
   - Each chunk is further split into `processBLockPartialCount`-sized partial ranges (e.g., 10 blocks) and fetched in parallel.

3. **Partial worker: `Fetching(workerSequence, startBlockNumber, blockNumber, requestCount)`**
   - For each partial range:
     - `getTransactionIds()` obtains txid lists (+ metadata like timestamp/block height)
     - `getTransactionData()` fetches full tx details and standardizes sender/receiver/value fields
     - Results accumulate into `insertBuffer['fetching'][startBlockNumber]`

4. **Finalize the chunk**
   - After all partial tasks finish, data are sorted by `blocknum`,
   - then moved from `fetching` to `done`:
     - `done[startBlockNumber]['data']`: list of parsed tx records
     - includes metadata such as `transCount`, `blockCount`
   - If done-buffer size exceeds `prepareChunkCount`, the loop waits until a consumer drains it (memory backpressure).

### 2.3 Coin-specific fetchers
Fetcher subclasses implement coin-specific RPC calls while keeping the same interface shape (e.g., `getLastBlockNumber`, `getTransactionIds`, `getTransactionData`).

#### (1) BTC Fetcher
- Uses Bitcoin JSON-RPC with multi-port load balancing.
- Typical flow:
  - `getblockcount` → height  
  - `getblockhash` + `getblock` → txids (+ block time)  
  - `getrawtransaction` (raw) → `decoderawtransaction` (decoded)  
  - Includes extra handling for witness transactions
- Address extraction:
  - Based on `scriptPubKey['type']`
  - For multisig/unknown scripts, it uses hashed placeholder addresses with prefixes like `m_`, `u_`

#### (2) ETH Fetcher
- Uses Ethereum JSON-RPC (`eth_blockNumber`, `eth_getBlockByNumber`, `eth_getTransactionByHash`).
- Converts `value` from wei to ETH and stores `gas`/`gasPrice` as memo fields.
- For contract-creation tx, it corrects `contractAddress` via receipt lookup.

#### (3) QTUM Fetcher
- Uses QTUM JSON-RPC (`getblockcount`, `getblockhash`, `getblock`, `getrawtransaction`, `decoderawtransaction`).
- Walks UTXO inputs (`vin`) and outputs (`vout`) to build sender/receiver/value.
- For some script types (create/call), it uses receipt-based address correction logic.

---

## 3) Tracker Design

### 3.1 Core responsibilities of `TrackerBase`
`TrackerBase` expands a graph of fund flows centered on target addresses, producing alerts and artifacts (CSV, i2 ANX).

Key state containers (conceptual):
- `targetList[address] = { balance, exchange, lastblock }`
- `txList[address][txid] = timestamp`
- `txData[txid] = { timestamp, blocknumber, sender, receiver, ... }`
- `alertList`: suspicious/important addresses derived from the target’s flow
- `txAlert`: txids considered alert candidates since program start

Modes:
- `datasrc == 'DB'` → DB-backed analysis (clustering / owner labels)
- otherwise → API-backed tracking (e.g., Etherscan-based)

### 3.2 `Process()` loop: continuous tracking
While `store.data['running']` is True:

1. Select tracking scope
   - If `onlySuspect=True`, track only `alertList`
   - Else, track all entries in `targetList`

2. For each address, run `Tracking(target)` asynchronously
   - fetch exchange label / balance / tx count / tx list
   - discover new recipient addresses and expand `targetList`
   - if the address looks “too big” (e.g., very high tx count), mark as suspect and stop expanding

3. Produce outputs
   - `saveResultCSV()` → `result.csv`, `target.csv`
   - `saveChart()` → i2 Analyst Notebook `.anx`
   - `saveAlertList()` → suspicious-address list and balance aggregation

4. Notifications (optional)
   - Slack message + file upload (`.anx`)
   - optionally generate official request documents (e.g., exchange deposit letters) and send emails

### 3.3 `Tracking(target)`: per-address expansion logic
The tracking step decides whether to expand through an address or stop there.

Per-round updates:
- `exchange = getExchangeInfo(target)`
- `balance = getBalance(target)`
- `transactionCount = getTransactionCount(target)` (coin-specific)
- `lastBlock = targetList[target]['lastblock']`

Typical stop/suspect condition:
- If `transactionCount >= transactionLimit`:
  - treat it as likely exchange / large service address → call `Suspect()` and stop expanding

Graph expansion:
- Parse transactions and collect newly discovered receiver addresses as `newTargetList`
- Add flow-relevant addresses to `alertList` for focused follow-up (when `onlySuspect` is enabled)

### 3.4 `Suspect(address)`: alert/report trigger
When an address is classified as suspicious (exchange/unknown/high-tx, etc.):
- persist suspect state in a manager component
- send Slack alerts + upload i2 chart (optional)
- optionally generate formal documents / emails for exchange deposit requests

---

## 4) DB-backed Tracking (Example: BTC Tracker)

The BTC tracker variant extends the base tracker to read both transaction data and clustering results from a DB, improving trace quality.

Key ideas:
- cluster tables (e.g., `cluster_address`, `cluster_master`) group addresses and map them to a representative “master” address
- transaction/address linkage tables (e.g., `address_tx`, `transaction`) provide tx lists and tx detail records
- the tracker can replace sender/receiver with master addresses to simplify the graph and reduce noise

---

## 5) ETH Tracking Variants: `ETHTracker` vs `ETHAPITracker`

### ETHTracker (DB + Web3)
- Computes tx count and balance via Web3 (`web3.eth.getTransactionCount`, `web3.eth.getBalance`)
- Fetches transactions from DB (sharded tables such as `ETH_transaction_0x`)

### ETHAPITracker (Etherscan API)
- Queries tx history and balances via Etherscan API
- Infers exchange labels by crawling Etherscan address pages (BeautifulSoup)

---

## 6) Coin-to-Tracker Routing

A coin identifier string (e.g., `ETH`, `BTC`, …) maps to the corresponding tracker implementation class in the router/config layer.

---

## 7) Operational Checklist

1. RPC/API reliability**
   - Fetchers split requests into smaller batches (commonly guided by a ~500KB request-size heuristic).
2. Fork/reorg handling**
   - The `-5 blocks` head offset provides a basic safety margin (improvable).
3. Memory/backpressure**
   - Done-buffer size is capped by `prepareChunkCount`; when full, fetch pauses.
4. Runaway expansion control**
   - Trackers stop expanding addresses beyond `transactionLimit` and mark them as suspect.
5. Reproducibility / restart
   - CSV outputs, i2 `.anx`, alert snapshots, and checkpoint files (e.g., `timestamp.last`) support repeatable runs.

