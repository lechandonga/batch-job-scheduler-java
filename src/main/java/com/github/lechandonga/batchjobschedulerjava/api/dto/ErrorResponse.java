package com.github.lechandonga.batchjobschedulerjava.api.dto;

import com.github.lechandonga.batchjobschedulerjava.core.exception.RejectReason;

public record ErrorResponse(RejectReason reason, String message) {
}
