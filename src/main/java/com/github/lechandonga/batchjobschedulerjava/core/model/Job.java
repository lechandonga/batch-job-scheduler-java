package com.github.lechandonga.batchjobschedulerjava.core.model;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Mutable job aggregate. All state transitions happen under {@link #stateLock}
 * via {@link #casTransit}; the lock also guards {@link #runningPermitHeld} and
 * {@link #attemptCount}. A job can only ever reach one terminal state and can
 * only be executed by one worker at a time.
 */
public class Job {

    private final String jobId;
    private final String idempotencyKey;
    private final String tenantId;
    private final String jobType;
    private final JobPriority priority;
    private final long timeoutMillis;
    private final int maxAttempts;
    private final String payload;
    private final long sequence;
    private final Instant createdAt;

    private final AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.QUEUED);
    private final AtomicInteger attemptCount = new AtomicInteger(0);
    private final AtomicInteger outcomeClaim = new AtomicInteger(-1);
    private final AtomicBoolean workerSlotReleased = new AtomicBoolean(false);
    private final Object stateLock = new Object();

    private volatile Instant queuedAt;
    private volatile Instant startedAt;
    private volatile Instant finishedAt;
    private volatile Instant notBefore;
    private volatile String result;
    private volatile String errorMessage;
    private volatile boolean runningPermitHeld;
    private volatile boolean queuedPermitHeld = true;
    private volatile Thread runnerThread;

    public Job(JobRequest request, long sequence) {
        this.jobId = request.jobId();
        this.idempotencyKey = request.idempotencyKey();
        this.tenantId = request.tenantId();
        this.jobType = request.jobType();
        this.priority = request.priority();
        this.timeoutMillis = request.timeoutMillis();
        this.maxAttempts = request.maxAttempts();
        this.payload = request.payload();
        this.sequence = sequence;
        Instant now = Instant.now();
        this.createdAt = now;
        this.queuedAt = now;
    }

    /** Conditional state transition; returns false (and changes nothing) when illegal. */
    public boolean casTransit(JobStatus from, JobStatus to) {
        synchronized (stateLock) {
            if (status.get() != from || !from.canTransitTo(to)) {
                return false;
            }
            status.set(to);
            applyTimestamps(to);
            return true;
        }
    }

    private void applyTimestamps(JobStatus to) {
        Instant now = Instant.now();
        switch (to) {
            case RUNNING -> {
                if (startedAt == null) {
                    startedAt = now;
                }
            }
            case QUEUED -> queuedAt = now;
            case SUCCEEDED, FAILED, CANCELLED, TIMED_OUT -> finishedAt = now;
        }
    }

    /**
     * Exactly one party (worker / timeout task / cancel) may settle an attempt.
     * Reset by the dispatcher when a new attempt starts.
     */
    public boolean claimAttemptOutcome(int attempt) {
        return outcomeClaim.compareAndSet(-1, attempt);
    }

    /** One-shot release of the global worker slot occupied by the current attempt. */
    public boolean markWorkerSlotReleased() {
        return workerSlotReleased.compareAndSet(false, true);
    }

    public void resetAttemptSlots() {
        synchronized (stateLock) {
            outcomeClaim.set(-1);
            workerSlotReleased.set(false);
        }
    }

    /** Parks the job after a failed attempt; the running permit is released while it waits. */
    public boolean parkForRetry(long backoffMillis) {
        synchronized (stateLock) {
            if (status.get() != JobStatus.RUNNING) {
                return false;
            }
            status.set(JobStatus.QUEUED);
            applyTimestamps(JobStatus.QUEUED);
            notBefore = Instant.now().plusMillis(backoffMillis);
            runningPermitHeld = false;
            runnerThread = null;
            return true;
        }
    }

    public boolean markPermitAcquired() {
        synchronized (stateLock) {
            if (status.get() != JobStatus.RUNNING || runningPermitHeld) {
                return false;
            }
            runningPermitHeld = true;
            return true;
        }
    }

    /** Exactly-once release of the tenant running permit, performed by whoever finishes the job. */
    public boolean consumeRunningPermit() {
        synchronized (stateLock) {
            boolean held = runningPermitHeld;
            runningPermitHeld = false;
            return held;
        }
    }

    /** Exactly-once release of the tenant queued (submission) permit. */
    public boolean consumeQueuedPermit() {
        synchronized (stateLock) {
            boolean held = queuedPermitHeld;
            queuedPermitHeld = false;
            return held;
        }
    }

    public void markQueuedPermitHeld() {
        synchronized (stateLock) {
            queuedPermitHeld = true;
        }
    }

    public int nextAttempt() {
        return attemptCount.incrementAndGet();
    }

    public Object stateLock() {
        return stateLock;
    }

    public String getJobId() { return jobId; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getTenantId() { return tenantId; }
    public String getJobType() { return jobType; }
    public JobPriority getPriority() { return priority; }
    public long getTimeoutMillis() { return timeoutMillis; }
    public int getMaxAttempts() { return maxAttempts; }
    public String getPayload() { return payload; }
    public long getSequence() { return sequence; }
    public JobStatus getStatus() { return status.get(); }
    public int getAttemptCount() { return attemptCount.get(); }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getStartedAt() { return startedAt; }
    public Instant getFinishedAt() { return finishedAt; }
    public Instant getNotBefore() { return notBefore; }
    public String getResult() { return result; }
    public String getErrorMessage() { return errorMessage; }
    public boolean isRunningPermitHeld() { return runningPermitHeld; }
    public Thread getRunnerThread() { return runnerThread; }

    public void setResult(String result) { this.result = result; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public void setRunnerThread(Thread runnerThread) { this.runnerThread = runnerThread; }
}
