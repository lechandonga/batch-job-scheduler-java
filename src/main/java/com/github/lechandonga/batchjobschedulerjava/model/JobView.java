package com.github.lechandonga.batchjobschedulerjava.model;

import java.time.Instant;

/**
 * 作业只读快照，供查询接口返回。每次查询时复制生成，避免外部修改内部状态。
 */
public class JobView {

    private final String jobId;
    private final String idempotencyKey;
    private final String tenantId;
    private final int priority;
    private final int effectivePriority;
    private final String handlerType;
    private final String payload;
    private final JobStatus status;
    private final TerminalReason terminalReason;
    private final int attempt;
    private final int maxAttempts;
    private final long timeoutMillis;
    private final Instant submittedAt;
    private final Instant nextRunAt;
    private final Instant startedAt;
    private final Instant finishedAt;
    private final String resultMessage;
    private final String lastError;

    public JobView(Job job, Instant now) {
        this.jobId = job.getJobId();
        this.idempotencyKey = job.getIdempotencyKey();
        this.tenantId = job.getTenantId();
        this.priority = job.getPriority();
        this.effectivePriority = job.effectivePriority(now.toEpochMilli());
        this.handlerType = job.getHandlerType();
        this.payload = job.getPayload();
        this.status = job.getStatus();
        this.terminalReason = job.getTerminalReason();
        this.attempt = job.getAttempt();
        this.maxAttempts = job.getMaxAttempts();
        this.timeoutMillis = job.getTimeoutMillis();
        this.submittedAt = Instant.ofEpochMilli(job.getSubmittedAtMillis());
        this.nextRunAt = job.getNextRunAtMillis() == Long.MAX_VALUE
                ? null : Instant.ofEpochMilli(job.getNextRunAtMillis());
        this.startedAt = job.getStartedAtMillis() == 0 ? null : Instant.ofEpochMilli(job.getStartedAtMillis());
        this.finishedAt = job.getFinishedAtMillis() == 0 ? null : Instant.ofEpochMilli(job.getFinishedAtMillis());
        this.resultMessage = job.getResultMessage();
        this.lastError = job.getLastError();
    }

    public String getJobId() {
        return jobId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getTenantId() {
        return tenantId;
    }

    public int getPriority() {
        return priority;
    }

    public int getEffectivePriority() {
        return effectivePriority;
    }

    public String getHandlerType() {
        return handlerType;
    }

    public String getPayload() {
        return payload;
    }

    public JobStatus getStatus() {
        return status;
    }

    public TerminalReason getTerminalReason() {
        return terminalReason;
    }

    public int getAttempt() {
        return attempt;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public Instant getSubmittedAt() {
        return submittedAt;
    }

    public Instant getNextRunAt() {
        return nextRunAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public String getResultMessage() {
        return resultMessage;
    }

    public String getLastError() {
        return lastError;
    }
}
