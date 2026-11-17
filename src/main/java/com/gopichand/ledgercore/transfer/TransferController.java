package com.gopichand.ledgercore.transfer;

import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/transfers")
public class TransferController {

    private static final Logger log = LoggerFactory.getLogger(TransferController.class);

    private final IdempotencyService idempotencyService;
    private final TransferService transferService;

    public TransferController(IdempotencyService idempotencyService, TransferService transferService) {
        this.idempotencyService = idempotencyService;
        this.transferService = transferService;
    }

    @PostMapping
    public ResponseEntity<TransferResponse> createTransfer(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody TransferRequest request) {

        String trimmedKey = idempotencyKey != null ? idempotencyKey.trim() : "";
        if (trimmedKey.isBlank()) {
            throw new IllegalArgumentException("Idempotency-Key header must not be blank");
        }

        // 1. Hash request payload with SHA-256
        String payloadHash = idempotencyService.hashPayload(request);

        // 2. Check PostgreSQL journal_entries: return cached 201 Created if match, or throw 409 Conflict if mismatch
        Optional<TransferResponse> cachedResponse = idempotencyService.checkExistingEntry(trimmedKey, payloadHash);
        if (cachedResponse.isPresent()) {
            log.info("Returning cached idempotent response for key: {}", trimmedKey);
            return ResponseEntity.status(HttpStatus.CREATED).body(cachedResponse.get());
        }

        // 3. Acquire short-lived Redis SET NX processing lock
        idempotencyService.acquireProcessingLock(trimmedKey, payloadHash);

        try {
            // 4. Execute atomic transfer within ACID transaction
            TransferResponse response = transferService.executeTransfer(trimmedKey, payloadHash, request);
            log.info("Transfer completed successfully for entryId: {}, key: {}", response.entryId(), trimmedKey);
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        } finally {
            // 5. Release processing lock upon completion
            idempotencyService.releaseProcessingLock(trimmedKey);
        }
    }

    @PostMapping("/{entryId}/reverse")
    public ResponseEntity<TransferResponse> reverseTransfer(@PathVariable UUID entryId) {
        log.info("Received reversal request for entry: {}", entryId);
        TransferResponse response = transferService.executeReversal(entryId);
        return ResponseEntity.ok(response);
    }
}
