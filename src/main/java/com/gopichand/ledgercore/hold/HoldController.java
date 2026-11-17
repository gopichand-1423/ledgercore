package com.gopichand.ledgercore.hold;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/holds")
public class HoldController {

    private final HoldService holdService;

    public HoldController(HoldService holdService) {
        this.holdService = holdService;
    }

    @PostMapping
    public ResponseEntity<HoldResponse> createHold(@Valid @RequestBody CreateHoldRequest request) {
        HoldResponse response = holdService.createHold(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @PostMapping("/{holdId}/capture")
    public ResponseEntity<HoldResponse> captureHold(@PathVariable UUID holdId) {
        HoldResponse response = holdService.captureHold(holdId);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/{holdId}/void")
    public ResponseEntity<HoldResponse> voidHold(@PathVariable UUID holdId) {
        HoldResponse response = holdService.voidHold(holdId);
        return ResponseEntity.ok(response);
    }
}
