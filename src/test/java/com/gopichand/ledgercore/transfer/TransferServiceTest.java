package com.gopichand.ledgercore.transfer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gopichand.ledgercore.account.AccountRepository;
import com.gopichand.ledgercore.domain.exception.InsufficientFundsException;
import com.gopichand.ledgercore.domain.model.Account;
import com.gopichand.ledgercore.domain.model.AccountStatus;
import com.gopichand.ledgercore.domain.model.AccountType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransferServiceTest {

    @Mock
    private AccountRepository accountRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private TransferService transferService;

    @BeforeEach
    void setUp() {
        transferService = new TransferService(accountRepository, objectMapper);
    }

    @Test
    @DisplayName("Should successfully transfer funds between two active accounts in deterministic order")
    void testSuccessfulTransfer() {
        UUID sourceId = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID destId = UUID.fromString("00000000-0000-0000-0000-000000000002");
        Instant now = Instant.now();

        Account source = new Account(sourceId, "ACC1", AccountType.ASSET, "INR", 50000L, 0L, AccountStatus.ACTIVE, 0L, now, now);
        Account dest = new Account(destId, "ACC2", AccountType.ASSET, "INR", 10000L, 0L, AccountStatus.ACTIVE, 0L, now, now);

        when(accountRepository.findByIdForUpdate(sourceId)).thenReturn(Optional.of(source));
        when(accountRepository.findByIdForUpdate(destId)).thenReturn(Optional.of(dest));

        TransferRequest request = new TransferRequest(sourceId, destId, 15000L, "Payment for invoice 42");
        TransferResponse response = transferService.executeTransfer("idemp-key-1", "hash-1", request);

        assertNotNull(response);
        assertEquals("POSTED", response.status());
        assertEquals(15000L, response.amountPaise());
        assertEquals(sourceId, response.sourceAccountId());
        assertEquals(destId, response.destinationAccountId());

        // Verify balance updates
        verify(accountRepository).updateBalance(sourceId, 35000L);
        verify(accountRepository).updateBalance(destId, 25000L);
    }

    @Test
    @DisplayName("Should throw InsufficientFundsException when source available balance is inadequate")
    void testInsufficientFunds() {
        UUID sourceId = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID destId = UUID.fromString("00000000-0000-0000-0000-000000000002");
        Instant now = Instant.now();

        Account source = new Account(sourceId, "ACC1", AccountType.ASSET, "INR", 5000L, 0L, AccountStatus.ACTIVE, 0L, now, now);
        Account dest = new Account(destId, "ACC2", AccountType.ASSET, "INR", 10000L, 0L, AccountStatus.ACTIVE, 0L, now, now);

        when(accountRepository.findByIdForUpdate(sourceId)).thenReturn(Optional.of(source));
        when(accountRepository.findByIdForUpdate(destId)).thenReturn(Optional.of(dest));

        TransferRequest request = new TransferRequest(sourceId, destId, 15000L, "Overdraft attempt");

        assertThrows(InsufficientFundsException.class, () ->
                transferService.executeTransfer("idemp-key-2", "hash-2", request));
    }

    @Test
    @DisplayName("Should reject self-transfer attempt")
    void testSelfTransferRejection() {
        UUID accountId = UUID.randomUUID();
        assertThrows(IllegalArgumentException.class, () ->
                new TransferRequest(accountId, accountId, 1000L, "Self transfer"));
    }

    @Test
    @DisplayName("Should reject non-positive amount")
    void testNonPositiveAmountRejection() {
        UUID sourceId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();
        assertThrows(IllegalArgumentException.class, () ->
                new TransferRequest(sourceId, destId, -500L, "Negative transfer"));
        assertThrows(IllegalArgumentException.class, () ->
                new TransferRequest(sourceId, destId, 0L, "Zero transfer"));
    }

    @Test
    @DisplayName("Should execute append-only reversal with inverted debit and credit legs")
    void testExecuteReversal() {
        UUID originalEntryId = UUID.randomUUID();
        UUID sourceId = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID destId = UUID.fromString("00000000-0000-0000-0000-000000000002");
        Instant now = Instant.now();

        com.gopichand.ledgercore.domain.model.JournalEntry original = new com.gopichand.ledgercore.domain.model.JournalEntry(
                originalEntryId, "idemp-orig", "hash-orig",
                com.gopichand.ledgercore.domain.model.EntryType.TRANSFER,
                com.gopichand.ledgercore.domain.model.EntryStatus.POSTED,
                "Original transfer", null, now, now
        );

        com.gopichand.ledgercore.domain.model.LedgerLine debitLeg = new com.gopichand.ledgercore.domain.model.LedgerLine(
                UUID.randomUUID(), originalEntryId, sourceId,
                com.gopichand.ledgercore.domain.model.Direction.DEBIT, 10000L, 40000L, 1, now
        );
        com.gopichand.ledgercore.domain.model.LedgerLine creditLeg = new com.gopichand.ledgercore.domain.model.LedgerLine(
                UUID.randomUUID(), originalEntryId, destId,
                com.gopichand.ledgercore.domain.model.Direction.CREDIT, 10000L, 20000L, 2, now
        );

        Account currentDest = new Account(destId, "ACC2", AccountType.ASSET, "INR", 20000L, 0L, AccountStatus.ACTIVE, 0L, now, now);
        Account currentSource = new Account(sourceId, "ACC1", AccountType.ASSET, "INR", 40000L, 0L, AccountStatus.ACTIVE, 0L, now, now);

        when(accountRepository.findJournalEntryById(originalEntryId)).thenReturn(Optional.of(original));
        when(accountRepository.findLedgerLinesByEntryId(originalEntryId)).thenReturn(java.util.List.of(debitLeg, creditLeg));
        when(accountRepository.findByIdForUpdate(sourceId)).thenReturn(Optional.of(currentSource));
        when(accountRepository.findByIdForUpdate(destId)).thenReturn(Optional.of(currentDest));

        TransferResponse reversal = transferService.executeReversal(originalEntryId);

        assertNotNull(reversal);
        assertEquals("POSTED", reversal.status());
        assertEquals(10000L, reversal.amountPaise());

        // In reversal: destId is debited (decreases from 20000 to 10000), sourceId is credited (increases from 40000 to 50000)
        verify(accountRepository).updateBalance(destId, 10000L);
        verify(accountRepository).updateBalance(sourceId, 50000L);
        verify(accountRepository).updateJournalEntryStatus(originalEntryId, com.gopichand.ledgercore.domain.model.EntryStatus.REVERSED);
    }
}
