package com.gopichand.ledgercore.hold;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gopichand.ledgercore.account.AccountRepository;
import com.gopichand.ledgercore.domain.exception.InsufficientFundsException;
import com.gopichand.ledgercore.domain.model.Account;
import com.gopichand.ledgercore.domain.model.AccountStatus;
import com.gopichand.ledgercore.domain.model.AccountType;
import com.gopichand.ledgercore.domain.model.HoldStatus;
import com.gopichand.ledgercore.domain.model.PaymentHold;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HoldServiceTest {

    @Mock
    private HoldRepository holdRepository;

    @Mock
    private AccountRepository accountRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private HoldService holdService;

    @BeforeEach
    void setUp() {
        holdService = new HoldService(holdRepository, accountRepository, objectMapper);
    }

    @Test
    @DisplayName("Should successfully authorize hold by shifting available balance to held balance")
    void testAuthorizeHold() {
        UUID accountId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();
        Instant now = Instant.now();

        Account account = new Account(accountId, "ACC-101", AccountType.ASSET, "INR", 50000L, 0L, AccountStatus.ACTIVE, 0L, now, now);
        when(accountRepository.findByIdForUpdate(accountId)).thenReturn(Optional.of(account));

        CreateHoldRequest request = new CreateHoldRequest(accountId, destId, 10000L, "Auth hold test");
        HoldResponse response = holdService.createHold(request);

        assertNotNull(response);
        assertEquals(HoldStatus.HELD, response.status());
        assertEquals(10000L, response.amountPaise());

        verify(holdRepository).updateAccountHeldBalance(accountId, 10000L);
        verify(holdRepository).insertHold(any(PaymentHold.class));
    }

    @Test
    @DisplayName("Should reject hold authorization when available balance is insufficient")
    void testAuthorizeHoldInsufficientFunds() {
        UUID accountId = UUID.randomUUID();
        Instant now = Instant.now();

        Account account = new Account(accountId, "ACC-101", AccountType.ASSET, "INR", 5000L, 0L, AccountStatus.ACTIVE, 0L, now, now);
        when(accountRepository.findByIdForUpdate(accountId)).thenReturn(Optional.of(account));

        CreateHoldRequest request = new CreateHoldRequest(accountId, null, 10000L, "Overdraft hold");
        assertThrows(InsufficientFundsException.class, () -> holdService.createHold(request));
    }

    @Test
    @DisplayName("Should successfully void hold and release held balance back to available balance")
    void testVoidHold() {
        UUID holdId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        Instant now = Instant.now();

        PaymentHold hold = new PaymentHold(holdId, accountId, null, 10000L, HoldStatus.HELD, "Test ref", null, now, now);
        Account account = new Account(accountId, "ACC-101", AccountType.ASSET, "INR", 50000L, 10000L, AccountStatus.ACTIVE, 0L, now, now);

        when(holdRepository.findByIdForUpdate(holdId)).thenReturn(Optional.of(hold));
        when(accountRepository.findByIdForUpdate(accountId)).thenReturn(Optional.of(account));

        HoldResponse response = holdService.voidHold(holdId);

        assertNotNull(response);
        assertEquals(HoldStatus.VOIDED, response.status());
        verify(holdRepository).updateAccountHeldBalance(accountId, 0L);
        verify(holdRepository).updateHoldStatus(holdId, HoldStatus.VOIDED, null);
    }
}
