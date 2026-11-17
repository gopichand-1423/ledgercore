package com.gopichand.ledgercore.settlement;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/admin/dlq")
public class DLQReplayController {

    private final DLQAdminService dlqAdminService;

    public DLQReplayController(DLQAdminService dlqAdminService) {
        this.dlqAdminService = dlqAdminService;
    }

    @PostMapping("/replay")
    public ResponseEntity<DLQReplayResponse> replayDlqMessage(@Valid @RequestBody DLQReplayRequest request) {
        DLQReplayResponse response = dlqAdminService.replayMessage(request);
        return ResponseEntity.ok(response);
    }
}
