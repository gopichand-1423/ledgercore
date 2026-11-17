package com.gopichand.ledgercore.common.web;

import com.gopichand.ledgercore.domain.exception.AccountNotFoundException;
import com.gopichand.ledgercore.domain.exception.IdempotencyConflictException;
import com.gopichand.ledgercore.domain.exception.InsufficientFundsException;
import com.gopichand.ledgercore.domain.exception.InvalidTransferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(InsufficientFundsException.class)
    public ProblemDetail handleInsufficientFunds(InsufficientFundsException ex) {
        log.warn("Insufficient funds error: {}", ex.getMessage());
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, ex.getMessage());
        problem.setTitle("Insufficient Funds");
        problem.setType(URI.create("https://ledgercore.com/errors/insufficient-funds"));
        problem.setProperty("errorCode", "INSUFFICIENT_FUNDS");
        problem.setProperty("accountId", ex.getAccountId());
        problem.setProperty("availableBalancePaise", ex.getAvailableBalancePaise());
        problem.setProperty("requestedAmountPaise", ex.getRequestedAmountPaise());
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    public ProblemDetail handleIdempotencyConflict(IdempotencyConflictException ex) {
        log.warn("Idempotency conflict error: {}", ex.getMessage());
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
        problem.setTitle("Idempotency Conflict");
        problem.setType(URI.create("https://ledgercore.com/errors/idempotency-conflict"));
        problem.setProperty("errorCode", "IDEMPOTENCY_CONFLICT");
        problem.setProperty("idempotencyKey", ex.getIdempotencyKey());
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }

    @ExceptionHandler(AccountNotFoundException.class)
    public ProblemDetail handleAccountNotFound(AccountNotFoundException ex) {
        log.warn("Account not found error: {}", ex.getMessage());
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setTitle("Account Not Found");
        problem.setType(URI.create("https://ledgercore.com/errors/account-not-found"));
        problem.setProperty("errorCode", "ACCOUNT_NOT_FOUND");
        if (ex.getAccountId() != null) {
            problem.setProperty("accountId", ex.getAccountId());
        }
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }

    @ExceptionHandler(InvalidTransferException.class)
    public ProblemDetail handleInvalidTransfer(InvalidTransferException ex) {
        log.warn("Invalid transfer error: {}", ex.getMessage());
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        problem.setTitle("Invalid Transfer");
        problem.setType(URI.create("https://ledgercore.com/errors/invalid-transfer"));
        problem.setProperty("errorCode", "INVALID_TRANSFER");
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleIllegalArgument(IllegalArgumentException ex) {
        log.warn("Illegal argument error: {}", ex.getMessage());
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        problem.setTitle("Bad Request");
        problem.setType(URI.create("https://ledgercore.com/errors/bad-request"));
        problem.setProperty("errorCode", "BAD_REQUEST");
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    public ProblemDetail handleMissingHeader(MissingRequestHeaderException ex) {
        log.warn("Missing header error: {}", ex.getMessage());
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                "Required request header '" + ex.getHeaderName() + "' is not present");
        problem.setTitle("Missing Request Header");
        problem.setType(URI.create("https://ledgercore.com/errors/missing-header"));
        problem.setProperty("errorCode", "MISSING_HEADER");
        problem.setProperty("headerName", ex.getHeaderName());
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidationExceptions(MethodArgumentNotValidException ex) {
        Map<String, String> errors = new HashMap<>();
        for (FieldError error : ex.getBindingResult().getFieldErrors()) {
            errors.put(error.getField(), error.getDefaultMessage());
        }
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Validation failed for request parameters");
        problem.setTitle("Validation Failed");
        problem.setType(URI.create("https://ledgercore.com/errors/validation-failed"));
        problem.setProperty("errorCode", "VALIDATION_FAILED");
        problem.setProperty("fieldErrors", errors);
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleGeneralException(Exception ex) {
        log.error("Unhandled internal server exception", ex);
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR,
                "An unexpected internal error occurred. Please contact system support.");
        problem.setTitle("Internal Server Error");
        problem.setType(URI.create("https://ledgercore.com/errors/internal-error"));
        problem.setProperty("errorCode", "INTERNAL_SERVER_ERROR");
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }
}
