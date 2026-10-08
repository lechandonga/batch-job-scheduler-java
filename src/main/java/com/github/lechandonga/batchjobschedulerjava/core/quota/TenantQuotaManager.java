package com.github.lechandonga.batchjobschedulerjava.core.quota;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-tenant two-level quota model, fully in-memory:
 * <ul>
 *   <li>queued permits: capacity of jobs a tenant may have in QUEUED + RUNNING state
 *       (acquired at submission, released once at any terminal outcome);</li>
 *   <li>running permits: max concurrently RUNNING jobs (acquired at dispatch,
 *       released when an attempt ends — including backoff parking).</li>
 * </ul>
 * All counters are exact: rejected submissions acquire nothing, and every terminal
 * outcome / attempt end releases exactly what it acquired.
 */
public class TenantQuotaManager {

    private final Map<String, AtomicInteger> queuedUsed = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> runningUsed = new ConcurrentHashMap<>();
    private volatile Map<String, Integer> queuedQuotaConfig = Map.of();
    private volatile Map<String, Integer> runningQuotaConfig = Map.of();
    private volatile int defaultQueuedQuota;
    private volatile int defaultRunningQuota;

    public void applyConfig(Map<String, Integer> queuedQuota, Map<String, Integer> runningQuota,
                            int defaultQueued, int defaultRunning) {
        this.queuedQuotaConfig = Map.copyOf(queuedQuota);
        this.runningQuotaConfig = Map.copyOf(runningQuota);
        this.defaultQueuedQuota = defaultQueued;
        this.defaultRunningQuota = defaultRunning;
    }

    public int queuedQuotaOf(String tenantId) {
        return queuedQuotaConfig.getOrDefault(tenantId, defaultQueuedQuota);
    }

    public int runningQuotaOf(String tenantId) {
        return runningQuotaConfig.getOrDefault(tenantId, defaultRunningQuota);
    }

    /** Attempts to reserve a submission slot; returns false without mutating anything when full. */
    public boolean tryAcquireQueued(String tenantId) {
        AtomicInteger used = queuedUsed.computeIfAbsent(tenantId, k -> new AtomicInteger());
        int limit = queuedQuotaOf(tenantId);
        while (true) {
            int current = used.get();
            if (current >= limit) {
                return false;
            }
            if (used.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /** Attempts to reserve a running slot at dispatch time. */
    public boolean tryAcquireRunning(String tenantId) {
        AtomicInteger used = runningUsed.computeIfAbsent(tenantId, k -> new AtomicInteger());
        int limit = runningQuotaOf(tenantId);
        while (true) {
            int current = used.get();
            if (current >= limit) {
                return false;
            }
            if (used.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    public void releaseQueued(String tenantId) {
        AtomicInteger used = queuedUsed.computeIfAbsent(tenantId, k -> new AtomicInteger());
        int after = used.decrementAndGet();
        if (after < 0) {
            used.set(0);
            throw new IllegalStateException("queued quota release mismatch for tenant=" + tenantId);
        }
    }

    public void releaseRunning(String tenantId) {
        AtomicInteger used = runningUsed.computeIfAbsent(tenantId, k -> new AtomicInteger());
        int after = used.decrementAndGet();
        if (after < 0) {
            used.set(0);
            throw new IllegalStateException("running quota release mismatch for tenant=" + tenantId);
        }
    }

    public int queuedUsedOf(String tenantId) {
        return queuedUsed.getOrDefault(tenantId, new AtomicInteger()).get();
    }

    public int runningUsedOf(String tenantId) {
        return runningUsed.getOrDefault(tenantId, new AtomicInteger()).get();
    }
}
