# LedgerCore — Engineering & Agent Operational Rules

Welcome to **LedgerCore**, a high-throughput, mission-critical Double-Entry Payment & Clearing Engine designed for Java 24 and containerized cloud-native infrastructure.

This document establishes the mandatory architectural invariants, coding standards, and execution constraints that every AI agent and developer **MUST STRICTLY ENFORCE** when interacting with, modifying, or extending this repository.

---

## 1. Zero Host Installation Policy (Container Execution Invariant)

### Invariant
> **NEVER execute Java, Maven, or database tooling directly on the host OS.**
> The Windows 11 host OS must remain completely clean. All compilation, execution, testing, dependency management, and database migrations must take place strictly inside the isolated container environment.

### Command Execution Standard
All Maven, Java, and CLI commands must be executed via `docker compose exec app-builder ...`:

```bash
# Compile and build the project
docker compose exec app-builder mvn clean compile

# Run unit and integration tests
docker compose exec app-builder mvn test

# Execute package build
docker compose exec app-builder mvn package -DskipTests

# Start the Spring Boot application inside container
docker compose exec app-builder mvn spring-boot:run

# Run database migration (Flyway / Liquibase)
docker compose exec app-builder mvn flyway:migrate

# Open an interactive shell inside the Java 24 build environment
docker compose exec app-builder bash
```

### Prohibited Actions:
- ❌ Do NOT run `mvn`, `javac`, `java`, `psql`, `redis-cli`, or `kafka-topics.sh` directly on the host PowerShell/Command Prompt.
- ❌ Do NOT install JDKs, Maven packages, or native build tools onto the host machine.
- ❌ Do NOT configure local Windows PATH variables for project-specific binaries.

---

## 2. Monetary Precision & Java 24 Record Discipline

### Minor Currency Units Invariant (`paise`)
> **NEVER use `double`, `float`, or arbitrary decimal types for financial calculations.**
> IEEE 754 floating-point arithmetic introduces non-deterministic precision degradation and rounding drift, which is unacceptable in an accounting ledger.

- All monetary quantities must be represented as **`BIGINT`** (Java primitive `long` or boxed `Long`) representing the minor currency unit (e.g., **`paise`** for INR, **`cents`** for USD, **`pence`** for GBP).
- 1 INR = 100 paise. An amount of ₹250.75 is stored and manipulated strictly as `25075L`.
- Database columns representing money must always use the PostgreSQL type `BIGINT` (e.g., `amount_paise BIGINT NOT NULL CHECK (amount_paise > 0)`).

### Java 24 `record` Invariant
All Data Transfer Objects (DTOs), API Request/Response bodies, Value Objects, and Domain Events must be implemented as **Java 24 `record` types**:

- Records provide shallow immutability, canonical constructor validation, value-based `equals`/`hashCode`, and native pattern matching.
- Enforce validation invariants inside compact constructors.

#### Example DTO Pattern:
```java
package com.ledgercore.domain.dto;

import java.util.Objects;
import java.util.UUID;

public record PostTransactionRequest(
    UUID idempotencyKey,
    UUID sourceAccountId,
    UUID destinationAccountId,
    long amountPaise,
    String currency,
    String description
) {
    public PostTransactionRequest {
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        Objects.requireNonNull(sourceAccountId, "sourceAccountId must not be null");
        Objects.requireNonNull(destinationAccountId, "destinationAccountId must not be null");
        if (sourceAccountId.equals(destinationAccountId)) {
            throw new IllegalArgumentException("Source and destination accounts must be distinct");
        }
        if (amountPaise <= 0) {
            throw new IllegalArgumentException("Transaction amount in paise must be strictly positive (> 0)");
        }
        if (currency == null || currency.trim().length() != 3) {
            throw new IllegalArgumentException("Currency must be a valid 3-letter ISO-4217 code");
        }
    }
}
```

---

## 3. Concurrency Discipline & Deadlock-Free Locking

### The Problem
When two concurrent transactions transfer funds between the same pair of accounts in opposite directions (e.g., Tx 1: Account A $\rightarrow$ Account B, Tx 2: Account B $\rightarrow$ Account A), acquiring locks in arbitrary execution order causes a **circular wait condition**, triggering catastrophic database deadlocks (`ERROR: deadlock detected`).

### Invariant
> **Always acquire `SELECT ... FOR UPDATE` row locks in deterministic lexicographical order of `account_id`.**

### Deterministic Ordering Rule
1. Before acquiring row locks in any transaction involving multiple accounts, extract and sort all participating `account_id`s in ascending natural order.
2. Query the database using `SELECT ... FROM accounts WHERE id IN (:sortedIds) ORDER BY id ASC FOR UPDATE`.
3. Process balance checks and journal postings within the same transactional boundary.

#### Example Repository & Service Implementation:
```java
package com.ledgercore.service;

import com.ledgercore.domain.model.Account;
import com.ledgercore.repository.AccountRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

@Service
public class ClearingService {

    private final AccountRepository accountRepository;

    public ClearingService(AccountRepository accountRepository) {
        this.accountRepository = accountRepository;
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public void executeTransfer(UUID sourceId, UUID destinationId, long amountPaise) {
        // 1. Sort account IDs in deterministic lexicographical order
        List<UUID> lockedIds = Stream.of(sourceId, destinationId)
            .sorted()
            .toList();

        // 2. Acquire row locks strictly in sorted order:
        //    SELECT * FROM accounts WHERE id IN (:ids) ORDER BY id ASC FOR UPDATE
        List<Account> lockedAccounts = accountRepository.findAllByIdInOrderByIdAscForUpdate(lockedIds);

        Account source = lockedAccounts.stream()
            .filter(a -> a.getId().equals(sourceId))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Source account not found: " + sourceId));

        Account destination = lockedAccounts.stream()
            .filter(a -> a.getId().equals(destinationId))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Destination account not found: " + destinationId));

        // 3. Verify balance invariant
        if (source.getBalancePaise() < amountPaise) {
            throw new InsufficientFundsException("Insufficient balance in account: " + sourceId);
        }

        // 4. Update balances and create postings...
    }
}
```

---

## 4. Double-Entry Accounting Invariants

1. **Balanced Books Principle**:
   Every journal entry consists of at least two posting legs. For every journal entry:
   $$\sum \text{Debit Postings} \equiv \sum \text{Credit Postings}$$
2. **Immutable Ledger**:
   Committed records in `journal_entries` and `postings` tables are **strictly append-only**.
   - `UPDATE` and `DELETE` queries on these tables are categorically forbidden.
   - Any financial correction or rollback must be performed via a **compensating transaction** (Reversal Journal Entry).
3. **Audit Trail Guarantee**:
   Every posting leg records the snapshot of `balance_after_paise` for that account at that exact chronological point.

---

## 5. Distributed Idempotency & Outbox Pattern

1. **Client Idempotency Keys**:
   Every mutation endpoint requires an `Idempotency-Key` HTTP header. The key is deduplicated using Redis (distributed lock with 10s TTL) and stored permanently in PostgreSQL with a unique constraint.
2. **Transactional Outbox for Kafka**:
   - Never perform a dual-write (Postgres + Kafka) in a non-transactional manner.
   - Domain events (e.g. `PaymentClearedEvent`, `TransferSettledEvent`) are written to the `outbox_events` table inside the primary database transaction.
   - An Outbox publisher worker reads pending outbox records and publishes them to the Kafka topic (`ledgercore.transactions`) with at-least-once delivery semantics.

---

## 6. Container Architecture Reference

| Service | Container Name | Port | Description |
|---|---|---|---|
| PostgreSQL 16 | `ledgercore-postgres` | `5432` | Source of truth (ACID, row-level locks, ledger tables) |
| Redis 7 | `ledgercore-redis` | `6379` | Fast distributed locking, token bucket rate limits, idempotency cache |
| Apache Kafka 3.9 | `ledgercore-kafka` | `9092` / `29092` | KRaft-mode single-node event streaming (Zero ZooKeeper) |
| App Builder | `ledgercore-app-builder` | N/A | Maven 3.9 + Eclipse Temurin Java 24 build & test execution container |
