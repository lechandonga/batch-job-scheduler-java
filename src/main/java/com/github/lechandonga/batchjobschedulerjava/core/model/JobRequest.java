package com.github.lechandonga.batchjobschedulerjava.core.model;

/**
 * Immutable submission request created after parameter validation.
 */
public record JobRequest(String jobId,
                         String idempotencyKey,
                         String tenantId,
                         String jobType,
                         JobPriority priority,
                         long timeoutMillis,
                         int maxAttempts,
                         String payload) {
}
