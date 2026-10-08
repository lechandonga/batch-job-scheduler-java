package com.github.lechandonga.batchjobschedulerjava.core.store;

import com.github.lechandonga.batchjobschedulerjava.core.exception.JobNotFoundException;
import com.github.lechandonga.batchjobschedulerjava.core.exception.JobRejectedException;
import com.github.lechandonga.batchjobschedulerjava.core.exception.RejectReason;
import com.github.lechandonga.batchjobschedulerjava.core.model.Job;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobRequest;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobStatus;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory job storage plus idempotency bookkeeping.
 * <p>
 * Submission is serialized on {@link #submitLock} so that concurrent submissions with the
 * same idempotency key are folded onto exactly one job. Finished jobs are retained for
 * queries; only the active (QUEUED + RUNNING) count feeds capacity checks.
 */
public class JobStore {

    public record Submission(Job job, boolean created) {
    }

    private final ConcurrentMap<String, Job> byJobId = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> idempotencyIndex = new ConcurrentHashMap<>();
    private final AtomicLong sequenceGenerator = new AtomicLong();
    private final AtomicInteger activeCount = new AtomicInteger();
    private final Object submitLock = new Object();

    /**
     * Atomically deduplicates by idempotency key and enforces global capacity. The caller is
     * responsible for tenant quota; this method only mutates state when {@code capacityOk}
     * suppliers succeed and the job is new, so a rejected submission leaves no trace.
     */
    public Submission submit(JobRequest request, java.util.function.BooleanSupplier tenantQuotaAcquirer,
                             int globalCapacity) {
        synchronized (submitLock) {
            String existingId = idempotencyIndex.get(request.idempotencyKey());
            if (existingId != null) {
                Job existing = byJobId.get(existingId);
                if (existing != null) {
                    return new Submission(existing, false);
                }
            }
            if (activeCount.get() >= globalCapacity) {
                throw new JobRejectedException(RejectReason.QUEUE_FULL,
                        "global queue is full, capacity=" + globalCapacity);
            }
            // Tenant quota acquisition inside the same critical section: no quota is held
            // unless the job is actually created.
            if (!tenantQuotaAcquirer.getAsBoolean()) {
                throw new JobRejectedException(RejectReason.TENANT_QUOTA_EXCEEDED,
                        "tenant queued quota exhausted, tenantId=" + request.tenantId());
            }
            Job job = new Job(request, sequenceGenerator.incrementAndGet());
            job.markQueuedPermitHeld();
            byJobId.put(job.getJobId(), job);
            idempotencyIndex.put(job.getIdempotencyKey(), job.getJobId());
            activeCount.incrementAndGet();
            return new Submission(job, true);
        }
    }

    /** Invoked exactly once when a job reaches a terminal state. */
    public void onTerminal(Job job) {
        activeCount.decrementAndGet();
    }

    public int activeCount() {
        return activeCount.get();
    }

    public Optional<Job> find(String jobId) {
        return Optional.ofNullable(byJobId.get(jobId));
    }

    public Job require(String jobId) {
        return find(jobId).orElseThrow(() -> new JobNotFoundException(jobId));
    }
}
