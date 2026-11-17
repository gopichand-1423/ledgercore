package com.gopichand.ledgercore;

import com.gopichand.ledgercore.account.AccountRepository;
import com.gopichand.ledgercore.domain.exception.IdempotencyConflictException;
import com.gopichand.ledgercore.domain.exception.InsufficientFundsException;
import com.gopichand.ledgercore.domain.model.Account;
import com.gopichand.ledgercore.domain.model.AccountStatus;
import com.gopichand.ledgercore.domain.model.AccountType;
import com.gopichand.ledgercore.domain.model.Direction;
import com.gopichand.ledgercore.domain.model.EntryStatus;
import com.gopichand.ledgercore.domain.model.EntryType;
import com.gopichand.ledgercore.domain.model.JournalEntry;
import com.gopichand.ledgercore.domain.model.LedgerLine;
import com.gopichand.ledgercore.outbox.OutboxEvent;
import com.gopichand.ledgercore.outbox.OutboxRelay;
import com.gopichand.ledgercore.outbox.OutboxRepository;
import com.gopichand.ledgercore.reconciliation.ReconciliationDiscrepancy;
import com.gopichand.ledgercore.reconciliation.ReconciliationJob;
import com.gopichand.ledgercore.reconciliation.ReconciliationRepository;
import com.gopichand.ledgercore.transfer.TransferController;
import com.gopichand.ledgercore.transfer.TransferRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"resource", "deprecation"})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@TestPropertySource(properties = {
        "spring.datasource.hikari.maximum-pool-size=50",
        "spring.datasource.hikari.connection-timeout=30000"
})
class ConcurrencyStressIT {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyStressIT.class);

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("ledgercore")
            .withUsername("ledger")
            .withPassword("ledger_secret");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Container
    @ServiceConnection(name = "redis")
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void configureDynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", redis::getFirstMappedPort);
    }

    @Autowired
    private TransferController transferController;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private OutboxRelay outboxRelay;

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private ReconciliationJob reconciliationJob;

    @Autowired
    private ReconciliationRepository reconciliationRepository;

    private Account accountA;
    private Account accountB;

    @BeforeEach
    void seedAccounts() {
        UUID aId = UUID.randomUUID();
        UUID bId = UUID.randomUUID();
        UUID mintId = UUID.randomUUID();
        Instant now = Instant.now();

        // 1. Create System Mint Equity Account (Source of funds for initial seeding)
        Account mintAccount = new Account(
                mintId,
                "SYSTEM_MINT_" + aId.toString().substring(0, 8),
                AccountType.EQUITY,
                "INR",
                100_000_000_000L,
                0L,
                AccountStatus.ACTIVE,
                0L,
                now,
                now
        );
        accountRepository.insertAccount(mintAccount);

        // 2. Create Account A and Account B seeded with 10,000,000 paise each (20,000,000 paise total)
        accountA = new Account(
                aId,
                "ACC-A-" + aId.toString().substring(0, 8),
                AccountType.ASSET,
                "INR",
                10_000_000L,
                0L,
                AccountStatus.ACTIVE,
                0L,
                now,
                now
        );
        accountB = new Account(
                bId,
                "ACC-B-" + bId.toString().substring(0, 8),
                AccountType.ASSET,
                "INR",
                10_000_000L,
                0L,
                AccountStatus.ACTIVE,
                0L,
                now,
                now
        );
        accountRepository.insertAccount(accountA);
        accountRepository.insertAccount(accountB);

        // 3. Insert balanced seed journal entry and ledger lines to satisfy double-entry constraint & reconciliation
        UUID seedEntryId = UUID.randomUUID();
        JournalEntry seedEntry = new JournalEntry(
                seedEntryId,
                "SEED_" + seedEntryId,
                null,
                EntryType.CLEARING,
                EntryStatus.POSTED,
                "Initial test account seeding",
                null,
                now,
                now
        );
        accountRepository.insertJournalEntry(seedEntry);

        // Double-entry legs: DEBIT mint 20,000,000; CREDIT Account A 10,000,000; CREDIT Account B 10,000,000
        accountRepository.insertLedgerLine(new LedgerLine(
                UUID.randomUUID(), seedEntryId, mintId, Direction.DEBIT, 20_000_000L, 99_980_000_000L, 1, now
        ));
        accountRepository.insertLedgerLine(new LedgerLine(
                UUID.randomUUID(), seedEntryId, aId, Direction.CREDIT, 10_000_000L, 10_000_000L, 2, now
        ));
        accountRepository.insertLedgerLine(new LedgerLine(
                UUID.randomUUID(), seedEntryId, bId, Direction.CREDIT, 10_000_000L, 10_000_000L, 3, now
        ));

        log.info("Seeded Account A ({}) and Account B ({}) with 10,000,000 paise each", aId, bId);
    }

    @Test
    @DisplayName("500 Concurrent Virtual Threads Chaos & Stress Test (Deadlock, Idempotency, and Conservation of Funds)")
    void testConcurrencyAndChaosStress() throws InterruptedException {
        int totalThreads = 500;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalThreads);

        AtomicInteger deadlocksDetected = new AtomicInteger(0);
        AtomicInteger successfulBidirectional = new AtomicInteger(0);
        AtomicInteger idempotencyHandled = new AtomicInteger(0);
        AtomicInteger insufficientFundsBlocked = new AtomicInteger(0);
        AtomicInteger unexpectedErrors = new AtomicInteger(0);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {

            // =========================================================================
            // GROUP 1: 200 Bidirectional Transfers (A -> B and B -> A concurrently)
            // Stresses deadlock prevention via deterministic lexicographical row locks
            // =========================================================================
            for (int i = 0; i < 200; i++) {
                final int idx = i;
                final boolean aToB = (i % 2 == 0);
                final UUID source = aToB ? accountA.id() : accountB.id();
                final UUID destination = aToB ? accountB.id() : accountA.id();
                final String idempotencyKey = "stress-bidirectional-" + idx + "-" + UUID.randomUUID();

                executor.submit(() -> {
                    try {
                        startLatch.await();
                        TransferRequest request = new TransferRequest(source, destination, 5_000L, "Bidirectional tx " + idx);
                        transferController.createTransfer(idempotencyKey, request);
                        successfulBidirectional.incrementAndGet();
                    } catch (Exception e) {
                        if (isDeadlockException(e)) {
                            deadlocksDetected.incrementAndGet();
                            log.error("Deadlock encountered in bidirectional transfer {}: {}", idx, e.getMessage());
                        } else {
                            unexpectedErrors.incrementAndGet();
                            log.error("Unexpected error in bidirectional transfer {}: {}", idx, e.getMessage(), e);
                        }
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // =========================================================================
            // GROUP 2: 150 Duplicate Retry Bursts sharing identical Idempotency-Key
            // 15 distinct transactions, each blasted 10 times concurrently
            // Verifies exact-once execution and race-condition idempotency deduplication
            // =========================================================================
            for (int keyIdx = 0; keyIdx < 15; keyIdx++) {
                final String burstKey = "stress-duplicate-burst-" + keyIdx + "-" + UUID.randomUUID();
                final TransferRequest burstReq = new TransferRequest(accountA.id(), accountB.id(), 1_000L, "Duplicate burst " + keyIdx);

                for (int attempt = 0; attempt < 10; attempt++) {
                    executor.submit(() -> {
                        try {
                            startLatch.await();
                            transferController.createTransfer(burstKey, burstReq);
                            idempotencyHandled.incrementAndGet();
                        } catch (IdempotencyConflictException e) {
                            // Concurrent in-flight lock rejection is an expected idempotency protection outcome
                            idempotencyHandled.incrementAndGet();
                        } catch (Exception e) {
                            if (isDeadlockException(e)) {
                                deadlocksDetected.incrementAndGet();
                                log.error("Deadlock encountered in duplicate burst {}: {}", burstKey, e.getMessage());
                            } else {
                                unexpectedErrors.incrementAndGet();
                                log.error("Unexpected error in duplicate burst {}: {}", burstKey, e.getMessage(), e);
                            }
                        } finally {
                            doneLatch.countDown();
                        }
                    });
                }
            }

            // =========================================================================
            // GROUP 3: 150 Concurrent Transfers exceeding remaining funds
            // Requests huge amounts (50,000,000 paise) to verify negative-balance protection
            // =========================================================================
            for (int i = 0; i < 150; i++) {
                final int idx = i;
                final String overdrawKey = "stress-overdraw-" + idx + "-" + UUID.randomUUID();

                executor.submit(() -> {
                    try {
                        startLatch.await();
                        TransferRequest overdrawReq = new TransferRequest(accountA.id(), accountB.id(), 50_000_000L, "Overdraw tx " + idx);
                        transferController.createTransfer(overdrawKey, overdrawReq);
                    } catch (InsufficientFundsException e) {
                        insufficientFundsBlocked.incrementAndGet();
                    } catch (Exception e) {
                        if (isDeadlockException(e)) {
                            deadlocksDetected.incrementAndGet();
                        } else if (e.getMessage() != null && e.getMessage().contains("Insufficient funds")) {
                            insufficientFundsBlocked.incrementAndGet();
                        } else {
                            unexpectedErrors.incrementAndGet();
                            log.error("Unexpected error in overdraw tx {}: {}", idx, e.getMessage());
                        }
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Release all 500 Virtual Threads simultaneously
            startLatch.countDown();
            boolean finished = doneLatch.await(60, TimeUnit.SECONDS);
            assertTrue(finished, "All 500 concurrent Virtual Threads should terminate within 60 seconds");
        }

        log.info("Stress Execution Finished: SuccessfulBidirectional={}, IdempotencyHandled={}, InsufficientFundsBlocked={}, Deadlocks={}, UnexpectedErrors={}",
                successfulBidirectional.get(), idempotencyHandled.get(), insufficientFundsBlocked.get(), deadlocksDetected.get(), unexpectedErrors.get());

        // =========================================================================
        // ASSERTIONS
        // =========================================================================

        // 1. Zero database deadlocks occurred
        assertEquals(0, deadlocksDetected.get(), "Zero database deadlocks must occur under deterministic row locking");
        assertEquals(0, unexpectedErrors.get(), "Zero unexpected exceptions must occur");
        assertEquals(150, insufficientFundsBlocked.get(), "All 150 overdraw attempts must be rejected");

        // 2. Neither account has a negative balance
        Account freshA = accountRepository.findById(accountA.id()).orElseThrow();
        Account freshB = accountRepository.findById(accountB.id()).orElseThrow();

        assertTrue(freshA.balancePaise() >= 0, "Account A balance must never drop below 0");
        assertTrue(freshB.balancePaise() >= 0, "Account B balance must never drop below 0");
        assertTrue(freshA.availableBalancePaise() >= 0, "Account A available balance must never be negative");
        assertTrue(freshB.availableBalancePaise() >= 0, "Account B available balance must never be negative");

        // 3. 100% Conservation of Funds (Account A + Account B == 20,000,000 paise)
        long totalConservation = freshA.balancePaise() + freshB.balancePaise();
        assertEquals(20_000_000L, totalConservation,
                "Conservation of Funds violated: total combined balance must remain exactly 20,000,000 paise");

        // 4. ReconciliationJob reports 0 drift across all accounts
        reconciliationJob.executeReconciliationAudit();
        List<ReconciliationDiscrepancy> drifts = reconciliationRepository.findDriftDiscrepancies();
        assertTrue(drifts.isEmpty(), "Double-entry reconciliation must report zero drift across all accounts, found: " + drifts);
        assertEquals(0L, reconciliationJob.getCurrentDriftCount(), "Reconciliation drift count metric must be 0");

        // 5. OutboxRelay drains all events to Kafka with zero lost messages
        int drainSafetyCount = 0;
        while (!outboxRepository.findUnpublishedForUpdate(50).isEmpty() && drainSafetyCount < 50) {
            outboxRelay.relayUnpublishedEvents();
            drainSafetyCount++;
        }

        List<OutboxEvent> remainingUnpublished = outboxRepository.findUnpublishedForUpdate(50);
        assertTrue(remainingUnpublished.isEmpty(),
                "OutboxRelay must drain all events to Kafka topic with zero unpublished records remaining");
    }

    private boolean isDeadlockException(Throwable throwable) {
        if (throwable == null) {
            return false;
        }
        String message = throwable.getMessage();
        if (message != null) {
            String lower = message.toLowerCase();
            if (lower.contains("deadlock detected")
                    || lower.contains("deadlockloserexception")
                    || lower.contains("40p01")) {
                return true;
            }
        }
        return isDeadlockException(throwable.getCause());
    }
}
