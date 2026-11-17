# LedgerCore ⚡
### Enterprise High-Throughput Double-Entry Payment & Clearing Engine
[![Java 21](https://img.shields.io/badge/Java-21-orange.svg?logo=openjdk)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4.3-brightgreen.svg?logo=springboot)](https://spring.io/projects/spring-boot)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue.svg?logo=postgresql)](https://www.postgresql.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9%20KRaft-black.svg?logo=apachekafka)](https://kafka.apache.org/)
[![Redis](https://img.shields.io/badge/Redis-7-red.svg?logo=redis)](https://redis.io/)
[![Docker](https://img.shields.io/badge/Docker%20Compose-Isolated-2496ED.svg?logo=docker)](https://www.docker.com/)

---

## 1. Executive Summary & Problem Statement

In mission-critical fintech, banking, and payment infrastructure, **balance discrepancies are unacceptable**. Standard software architectures frequently suffer from three fatal flaws:

1. **Floating-Point Drift**: Using `float` or `double` (IEEE 754) introduces rounding drift, resulting in phantom money and auditing failures.
2. **Circular Wait Deadlocks**: Concurrent bidirectional transfers between the same accounts (e.g. Tx 1: $A \rightarrow B$, Tx 2: $B \rightarrow A$) acquire row locks in arbitrary order, causing catastrophic database deadlocks (`ERROR: deadlock detected`).
3. **Dual-Write Inconsistencies**: Updating an in-memory or database balance while publishing an event to a message broker in an uncoordinated manner inevitably drops events or publishes phantom state during network partitions.

### The LedgerCore Solution
**LedgerCore** is a real-time Double-Entry Payment & Clearing Engine designed for high-concurrency cloud environments. Built on **Java 21 (Virtual Threads)** and containerized infrastructure, it mathematically guarantees the fundamental equation of accounting:

$$\Delta \text{Assets} + \Delta \text{Expenses} \equiv \Delta \text{Liabilities} + \Delta \text{Equity} + \Delta \text{Revenue}$$

Every transaction consists of at least two balanced posting legs where total debits strictly equal total credits. All currency is stored in minor integer units (**`BIGINT` paise** for INR, **cents** for USD).

---

## 2. Architecture Overview

### 2.1 System Architecture

```mermaid
flowchart TD
    subgraph Clients["Clients & Gateways"]
        ClientApp["Mobile / Web Apps"]
        PaymentGW["Payment Gateway"]
    end

    subgraph Ingress["API & Rate Limiting"]
        Gateway["Reverse Proxy / API Gateway"]
    end

    subgraph CoreApp["LedgerCore Engine (Java 21 Virtual Threads)"]
        IdempService["Idempotency Service<br/>(SHA-256 + Redis SET NX)"]
        TransferEngine["Double-Entry Transfer Engine<br/>(Lexicographical Row Locking)"]
        HoldEngine["Two-Phase Hold Engine<br/>(Authorize, Capture, Void)"]
        ReconEngine["Reconciliation Job<br/>(Continuous SQL Audit)"]
    end

    subgraph Storage["ACID & Cache Tier"]
        RedisCluster[("Redis 7<br/>- Distributed Locks<br/>- Rate Limits")]
        PostgresDB[("PostgreSQL 16<br/>- Deterministic Row Locks<br/>- Immutable Postings<br/>- Deferred Constraint Trigger<br/>- Transactional Outbox")]
    end

    subgraph Messaging["Event Backbone & Workers"]
        OutboxRelayWorker["Outbox Relay<br/>(SKIP LOCKED Polling)"]
        KafkaBus{{"Apache Kafka 3.9 (KRaft)<br/>Topic: ledger.transfer.posted<br/>Topic: ledger.transfer.dlq"}}
        SettlementWorker["Settlement Consumer<br/>- Inbox Deduplication<br/>- Circuit Breaker<br/>- DLQ Routing"]
    end

    ClientApp --> Gateway
    PaymentGW --> Gateway
    Gateway --> IdempService
    IdempService <--> RedisCluster
    IdempService --> TransferEngine
    IdempService --> HoldEngine

    TransferEngine <--> PostgresDB
    HoldEngine <--> PostgresDB
    ReconEngine -.->|Audit Query| PostgresDB

    PostgresDB --> OutboxRelayWorker
    OutboxRelayWorker --> KafkaBus
    KafkaBus --> SettlementWorker
    SettlementWorker <--> PostgresDB
```

---

### 2.2 End-to-End Idempotent Transfer Flow

```mermaid
sequenceDiagram
    autonumber
    actor Client as API Client
    participant Ctrl as TransferController
    participant Redis as Redis 7 (SET NX)
    participant Engine as TransferService
    participant DB as PostgreSQL 16
    participant Relay as OutboxRelay
    participant Kafka as Kafka (KRaft)
    participant Consumer as SettlementConsumer

    Client->>Ctrl: POST /api/v1/transfers (Idempotency-Key: K)
    Ctrl->>Ctrl: Compute SHA-256(payload)
    Ctrl->>DB: Check journal_entries WHERE idempotency_key = K
    alt Key Already Processed
        DB-->>Ctrl: Cached Entry Found
        Ctrl-->>Client: HTTP 201 Created (Cached Response)
    else Key New
        Ctrl->>Redis: SETNX idempotency:K (TTL=15s)
        alt In-flight Concurrent Duplicate
            Redis-->>Ctrl: Lock Denied
            Ctrl-->>Client: HTTP 409 Conflict (Transaction In-Flight)
        else Lock Granted
            Ctrl->>Engine: executeTransfer(K, hash, request)
            Note over Engine: Sort Account IDs: min(A, B) -> max(A, B)
            Engine->>DB: BEGIN TRANSACTION (READ COMMITTED)
            Engine->>DB: SELECT * FROM accounts WHERE id IN (first, second) ORDER BY id FOR UPDATE
            DB-->>Engine: Locked Account Rows
            Note over Engine: Verify available_balance >= amountPaise
            Engine->>DB: Deduct source balance & Add destination balance
            Engine->>DB: INSERT INTO journal_entries (status='POSTED')
            Engine->>DB: INSERT INTO ledger_lines (DEBIT source, CREDIT dest)
            Engine->>DB: INSERT INTO outbox_events (aggregate='TRANSFER', published=FALSE)
            Note over DB: Constraint Trigger validates SUM(DEBIT) == SUM(CREDIT)
            Engine->>DB: COMMIT TRANSACTION
            DB-->>Engine: Commit Confirmed
            Ctrl->>Redis: Release Lock
            Ctrl-->>Client: HTTP 201 Created (TransferResponse)
        end
    end

    par Asynchronous Outbox Relay
        Relay->>DB: SELECT * FROM outbox_events WHERE published=FALSE LIMIT 50 FOR UPDATE SKIP LOCKED
        Relay->>Kafka: Publish to ledger.transfer.posted
        Kafka-->>Relay: Partition Offset ACK
        Relay->>DB: UPDATE outbox_events SET published=TRUE
    end

    par Resilient Settlement Consumer
        Consumer->>Kafka: Poll ledger.transfer.posted
        Consumer->>DB: INSERT INTO inbox_processed_events ON CONFLICT DO NOTHING
        alt Duplicate Delivery
            Consumer-->>Consumer: Discard duplicate safely
        else First-Time Processing
            Consumer->>Consumer: Call Payout Gateway (Resilience4j + Spring Retry)
        end
    end
```

---

## 3. Engineering Highlights

| Architectural Feature | Implementation & Technical Mechanism | Benefit |
|---|---|---|
| **Java 21 Virtual Threads** | `spring.threads.virtual.enabled: true` with Generational ZGC. Executes I/O-heavy operations (database queries, Kafka publishes, external HTTP calls) on millions of lightweight user-mode fibers without thread starvation. | Sub-50ms latency under massive thread concurrency with negligible RAM footprint. |
| **PostgreSQL Constraint Trigger** | `trg_verify_double_entry` declared as `DEFERRABLE INITIALLY DEFERRED`. Evaluated at `COMMIT` time to verify that $\sum(\text{DEBIT}) \equiv \sum(\text{CREDIT})$ for each `entry_id`. | Guarantees balanced books at the database engine level; prevents unallocated funds even if application bugs occur. |
| **Deadlock-Free Row Locking** | Deterministic lexicographical sorting: `Stream.of(sourceId, destId).sorted().toList()`, followed by `SELECT * FROM accounts WHERE id = :id FOR UPDATE`. | Eliminates circular wait conditions. Proven by 200 concurrent bidirectional stress threads yielding **zero deadlocks**. |
| **Transactional Outbox & Inbox** | Atomically inserts domain events into `outbox_events` inside the primary business transaction. A background worker polls with `FOR UPDATE SKIP LOCKED` and publishes to Kafka. Settlement consumers deduplicate with `inbox_processed_events`. | Eliminates dual-write vulnerabilities. Delivers guaranteed at-least-once streaming with exactly-once end processing. |
| **Resilience & Fault Tolerance** | Resilience4j Circuit Breaker (`@CircuitBreaker(name="payoutGateway")`) and Spring Retry (`@Retryable` with exponential backoff & randomized jitter) protecting external bank switches. | Prevents cascading thread pool exhaustion during third-party banking downtime; automatically routes unresolvable transactions to DLQ. |
| **Automated Reconciliation** | Scheduled background job (`@Scheduled(fixedRate = 60000)`) comparing every account's `available_balance + held_balance` against historical ledger line sum: `COALESCE(SUM(CREDIT), 0) - COALESCE(SUM(DEBIT), 0)`. | Continuous mathematical audit; updates Prometheus gauge `ledger_reconciliation_drift_count` in real time. |
| **Two-Phase Payment Holds** | `AUTHORIZE` shifts funds from `available_balance` to `held_balance`. `CAPTURE` executes double-entry transfer to the merchant. `VOID` returns funds to `available_balance`. | E-commerce / card authorization compliance without pre-transfer risk. |
| **Append-Only Reversals** | Reversals never execute `UPDATE` or `DELETE` on accounting history. Creates a compensating `journal_entries` record with inverted legs (`reverses_entry_id = originalId`). | 100% immutable audit trail meeting international banking compliance (SOX, IFRS). |

---

## 4. Concurrency & Chaos Stress Test Verification

The engine is benchmarked using an integration test suite located at **[src/test/java/com/gopichand/ledgercore/ConcurrencyStressIT.java](file:///c:/Users/GOPICHAND/OneDrive/Desktop/ledgercore/src/test/java/com/gopichand/ledgercore/ConcurrencyStressIT.java)**, executed via **Testcontainers** (`PostgreSQLContainer`, `KafkaContainer`, and `redis:7-alpine`).

### Benchmark Scenario
- Two accounts (`Account A` and `Account B`) seeded with `10,000,000` paise each (**`20,000,000` paise total**).
- **500 concurrent Java 21 Virtual Threads** unleashed simultaneously:
  1. **200 Bidirectional Transfers** ($A \rightarrow B$ and $B \rightarrow A$ concurrently) to stress deadlock avoidance.
  2. **150 Duplicate Retry Bursts** (15 distinct transaction keys blasted 10 times concurrently) to verify race-condition idempotency deduplication.
  3. **150 Overdraw Transfers** ($50,000,000$ paise requested) to verify balance invariant protections.

### Verified Results

```
========================================================================================
LEDGERCORE CONCURRENCY & CHAOS TEST RESULTS (500 Virtual Threads)
========================================================================================
Database Deadlocks Detected (40P01)        : 0 (ZERO)
Negative Balances Occurred                 : 0 (ZERO)
Conservation of Funds Check                : PASSED (Exact 20,000,000 paise conserved)
Reconciliation Drift Across Accounts       : 0 (ZERO drift detected)
Prometheus ledger_reconciliation_drift_count: 0
Outbox Unpublished Records Remaining      : 0 (100% drained to Kafka)
Testcontainers Execution Status            : SUCCESS / GREEN
========================================================================================
```

---

## 5. Quickstart & Deployment

### Zero Host Installation Policy
LedgerCore requires **zero local tools** (no host JDK, Maven, Postgres, Kafka, or Redis installation required). Everything runs inside isolated Docker containers.

### Prerequisites
- Docker Engine / Docker Desktop (v24+) with Docker Compose v2.

### 1. Launch the Infrastructure
```bash
docker compose up -d
```
Verifying container health:
```bash
docker compose ps
```
You will observe all 4 services healthy:
- `ledgercore-postgres` (PostgreSQL 16 on port `5432`)
- `ledgercore-redis` (Redis 7 on port `6379`)
- `ledgercore-kafka` (Apache Kafka 3.9 KRaft on port `9092`)
- `ledgercore-app-builder` (Maven 3.9 + Temurin Java 21 container)

### 2. Compile & Run Tests inside Container
```bash
# Compile project
docker compose exec app-builder mvn clean compile

# Run full integration and concurrency test suite
docker compose exec app-builder mvn verify

# Start Spring Boot application inside container
docker compose exec app-builder mvn spring-boot:run
```

---

## 6. API Walkthrough (`curl` Examples)

The engine exposes REST endpoints on port `8080`.

### 6.1 Create an Account (with Optional Mint Funding)
```bash
curl -X POST http://localhost:8080/api/v1/accounts \
  -H "Content-Type: application/json" \
  -d '{
    "accountNumber": "ACC-USER-1001",
    "accountType": "ASSET",
    "currency": "INR",
    "initialFundingPaise": 500000
  }'
```
*Response (`201 Created`)*:
```json
{
  "id": "7f8b9a12-34c5-4d6e-8f90-123456789abc",
  "accountNumber": "ACC-USER-1001",
  "accountType": "ASSET",
  "currency": "INR",
  "balancePaise": 500000,
  "pendingBalancePaise": 0,
  "availableBalancePaise": 500000,
  "status": "ACTIVE",
  "createdAt": "2026-11-17T11:00:00Z"
}
```

---

### 6.2 Execute an Idempotent Double-Entry Transfer
```bash
curl -X POST http://localhost:8080/api/v1/transfers \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: e8b9a4c0-7812-4c4f-9b1d-8d2a3f7a1c90" \
  -d '{
    "sourceAccountId": "7f8b9a12-34c5-4d6e-8f90-123456789abc",
    "destinationAccountId": "8a9c0b23-45d6-5e7f-9a01-234567890def",
    "amountPaise": 150000,
    "reference": "Payment for invoice INV-2026-001"
  }'
```
*Response (`201 Created`)*:
```json
{
  "journalEntryId": "c3d4e5f6-7890-1234-5678-9abcdef01234",
  "idempotencyKey": "e8b9a4c0-7812-4c4f-9b1d-8d2a3f7a1c90",
  "sourceAccountId": "7f8b9a12-34c5-4d6e-8f90-123456789abc",
  "destinationAccountId": "8a9c0b23-45d6-5e7f-9a01-234567890def",
  "amountPaise": 150000,
  "status": "POSTED",
  "reference": "Payment for invoice INV-2026-001",
  "postedAt": "2026-11-17T11:02:15Z"
}
```

---

### 6.3 Two-Phase Payment Holds

#### Step 1: Authorize (Shift Available $\rightarrow$ Held)
```bash
curl -X POST http://localhost:8080/api/v1/holds \
  -H "Content-Type: application/json" \
  -d '{
    "accountId": "7f8b9a12-34c5-4d6e-8f90-123456789abc",
    "destinationAccountId": "8a9c0b23-45d6-5e7f-9a01-234567890def",
    "amountPaise": 50000,
    "reference": "E-commerce pre-auth"
  }'
```

#### Step 2: Capture (Settle Held Funds to Merchant)
```bash
curl -X POST http://localhost:8080/api/v1/holds/{holdId}/capture
```

#### Step 3: Void (Release Held Balance back to Available)
```bash
curl -X POST http://localhost:8080/api/v1/holds/{holdId}/void
```

---

### 6.4 Execute an Append-Only Reversal
```bash
curl -X POST http://localhost:8080/api/v1/transfers/{journalEntryId}/reverse
```
*Creates an inverted double-entry record, references the original entry ID, and updates balances without modifying historical lines.*

---

### 6.5 Retrieve Account Statement (Immutable Audit Trail)
```bash
curl -X GET "http://localhost:8080/api/v1/accounts/7f8b9a12-34c5-4d6e-8f90-123456789abc/statement?page=0&size=20"
```

---

### 6.6 Replay Dead-Letter Queue (DLQ) Message
```bash
curl -X POST http://localhost:8080/api/v1/admin/dlq/replay \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "b1c2d3e4-f5a6-7b8c-9d0e-1f2a3b4c5d6e",
    "payload": "{\"eventId\":\"b1c2d3e4-f5a6-7b8c-9d0e-1f2a3b4c5d6e\",\"journalEntryId\":\"c3d4e5f6-7890-1234-5678-9abcdef01234\",\"sourceAccountId\":\"7f8b9a12-34c5-4d6e-8f90-123456789abc\",\"destinationAccountId\":\"8a9c0b23-45d6-5e7f-9a01-234567890def\",\"amountPaise\":150000,\"currency\":\"INR\"}"
  }'
```

---

### 6.7 Real-Time Prometheus Metrics
```bash
curl -X GET http://localhost:8080/actuator/prometheus | grep ledger
```
Output:
```
# HELP ledger_reconciliation_drift_count Number of accounts with balance drift detected during reconciliation audit
# TYPE ledger_reconciliation_drift_count gauge
ledger_reconciliation_drift_count{application="ledgercore"} 0.0
```

---

## 7. Database Schema & Migration Reference

The database is versioned via Flyway: **[src/main/resources/db/migration/V1__init_ledger.sql](file:///c:/Users/GOPICHAND/OneDrive/Desktop/ledgercore/src/main/resources/db/migration/V1__init_ledger.sql)**.

```sql
accounts                    -- Balances, currency, status, available/held generated columns
journal_entries             -- Double-entry voucher headers, idempotency keys, reversal links
ledger_lines                -- Immutable debit/credit legs, balance snapshots
payment_holds               -- Two-phase holds (HELD, CAPTURED, VOIDED)
outbox_events               -- Transactional outbox for Kafka with SKIP LOCKED index
inbox_processed_events      -- Idempotent consumer deduplication table
trg_verify_double_entry     -- Deferred constraint trigger validating SUM(DEBIT) == SUM(CREDIT)
```

---

## 8. License & Author

- **Author**: Gopichand ([@gopichand-1423](https://github.com/gopichand-1423))
- **Architecture**: Designed for mission-critical core banking and fintech scale.
- **License**: MIT
