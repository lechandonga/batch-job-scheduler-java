package com.github.lechandonga.batchjobschedulerjava.api.dto;

import com.github.lechandonga.batchjobschedulerjava.core.model.Job;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobPriority;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobStatus;

import java.time.Instant;

public record JobView(String jobId,
                      String idempotencyKey,
                      String tenantId,
                      String jobType,
                      JobPriority priority,
                      JobStatus status,
                      int attemptCount,
                      int maxAttempts,
                      long timeoutMillis,
                      Instant createdAt,
                      Instant startedAt,
                      Instant finishedAt,
                      Instant notBefore,
                      String result,
                      String errorMessage,
                      boolean reused) {

    public static JobView of(Job job, boolean reused) {
        return new JobView(job.getJobId(), job.getIdempotencyKey(), job.getTenantId(),
                job.getJobType(), job.getPriority(), job.getStatus(), job.getAttemptCount(),
                job.getMaxAttempts(), job.getTimeoutMillis(), job.getCreatedAt(),
                job.getStartedAt(), job.getFinishedAt(), job.getNotBefore(), job.getResult(),
                job.getErrorMessage(), reused);
    }
}
