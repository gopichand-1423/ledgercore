package com.gopichand.ledgercore.transfer;

import com.gopichand.ledgercore.account.AccountRepository;
import com.gopichand.ledgercore.domain.exception.IdempotencyConflictException;
import com.gopichand.ledgercore.domain.model.Direction;
import com.gopichand.ledgercore.domain.model.JournalEntry;
import com.gopichand.ledgercore.domain.model.LedgerLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class IdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyService.class);
    private static final Duration LOCK_TTL = Duration.ofSeconds(15);

    private final StringRedisTemplate redisTemplate;
    private final AccountRepository accountRepository;

    public IdempotencyService(StringRedisTemplate redisTemplate, AccountRepository accountRepository) {
        this.redisTemplate = redisTemplate;
        this.accountRepository = accountRepository;
    }

    public String hashPayload(TransferRequest request) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String raw = request.sourceAccountId() + "|"
                    + request.destinationAccountId() + "|"
                    + request.amountPaise() + "|"
                    + (request.reference() != null ? request.reference() : "");
            byte[] hash = digest.digest(raw.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }

    /**
     * Checks if the transaction has already been posted to PostgreSQL.
     * Returns cached response if hash matches; throws 409 Conflict if key is reused with a different payload.
     */
    public Optional<TransferResponse> checkExistingEntry(String idempotencyKey, String payloadHash) {
        Optional<JournalEntry> existing = accountRepository.findJournalEntryByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            JournalEntry entry = existing.get();
            if (entry.payloadHash() != null && !entry.payloadHash().equals(payloadHash)) {
                log.warn("Idempotency conflict for key {}: existing hash {} != new hash {}",
                        idempotencyKey, entry.payloadHash(), payloadHash);
                throw new IdempotencyConflictException(idempotencyKey,
                        "Idempotency key '" + idempotencyKey + "' was previously used with a different request payload");
            }

            log.info("Returning cached response for idempotency key: {}", idempotencyKey);
            List<LedgerLine> lines = accountRepository.findLedgerLinesByEntryId(entry.id());
            UUID sourceId = lines.stream()
                    .filter(l -> l.direction() == Direction.DEBIT)
                    .findFirst()
                    .map(LedgerLine::accountId)
                    .orElse(null);
            UUID destId = lines.stream()
                    .filter(l -> l.direction() == Direction.CREDIT)
                    .findFirst()
                    .map(LedgerLine::accountId)
                    .orElse(null);
            long amountPaise = lines.stream()
                    .filter(l -> l.direction() == Direction.DEBIT)
                    .findFirst()
                    .map(LedgerLine::amountPaise)
                    .orElse(0L);

            return Optional.of(new TransferResponse(
                    entry.id(),
                    entry.idempotencyKey(),
                    sourceId,
                    destId,
                    amountPaise,
                    entry.status().name(),
                    entry.description(),
                    entry.postedAt()
            ));
        }
        return Optional.empty();
    }

    /**
     * Acquires a short-lived Redis SET NX processing lock on idempotency:{key}.
     */
    public void acquireProcessingLock(String idempotencyKey, String payloadHash) {
        String lockKey = "idempotency:" + idempotencyKey;
        try {
            Boolean acquired = redisTemplate.opsForValue().setIfAbsent(lockKey, payloadHash, LOCK_TTL);
            if (Boolean.FALSE.equals(acquired)) {
                log.warn("Concurrent in-flight duplicate detected for idempotency key: {}", idempotencyKey);
                throw new IdempotencyConflictException(idempotencyKey,
                        "Transaction with idempotency key '" + idempotencyKey + "' is currently being processed");
            }
        } catch (IdempotencyConflictException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to acquire Redis idempotency lock for key {}: {}", idempotencyKey, e.getMessage());
            // In degraded cache scenarios, we can proceed to relying on PostgreSQL UNIQUE constraint
        }
    }

    /**
     * Releases the Redis in-flight lock after processing.
     */
    public void releaseProcessingLock(String idempotencyKey) {
        String lockKey = "idempotency:" + idempotencyKey;
        try {
            redisTemplate.delete(lockKey);
        } catch (Exception e) {
            log.warn("Failed to release Redis lock {}: {}", lockKey, e.getMessage());
        }
    }
}
