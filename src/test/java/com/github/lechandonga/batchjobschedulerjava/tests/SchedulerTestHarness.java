package com.github.lechandonga.batchjobschedulerjava.tests;

import com.github.lechandonga.batchjobschedulerjava.config.SchedulerProperties;
import com.github.lechandonga.batchjobschedulerjava.api.dto.SubmitJobRequest;
import com.github.lechandonga.batchjobschedulerjava.core.handler.JobHandlerRegistry;
import com.github.lechandonga.batchjobschedulerjava.core.model.Job;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobPriority;
import com.github.lechandonga.batchjobschedulerjava.core.quota.TenantQuotaManager;
import com.github.lechandonga.batchjobschedulerjava.core.scheduler.JobScheduler;
import com.github.lechandonga.batchjobschedulerjava.core.service.JobService;
import com.github.lechandonga.batchjobschedulerjava.core.store.JobStore;

import java.util.List;
import java.util.concurrent.TimeUnit;

/** Small hand-built (no Spring) scheduler assembly for fast, deterministic unit tests. */
final class SchedulerTestHarness implements AutoCloseable {

    final SchedulerProperties props;
    final JobStore store;
    final TenantQuotaManager quotas;
    final JobHandlerRegistry registry;
    final JobScheduler scheduler;
    final JobService service;

    private SchedulerTestHarness(SchedulerProperties props, List<com.github.lechandonga.batchjobschedulerjava.core.handler.JobHandler> extraHandlers) {
        this.props = props;
        this.store = new JobStore();
        this.quotas = new TenantQuotaManager();
        this.registry = new JobHandlerRegistry(List.of());
        extraHandlers.forEach(registry::register);
        this.scheduler = new JobScheduler(store, quotas, registry, props);
        this.service = new JobService(store, scheduler, quotas, props, registry);
        this.quotas.applyConfig(props.getTenantQueuedQuota(), props.getTenantRunningQuota(),
                props.getDefaultTenantQueuedQuota(), props.getDefaultTenantRunningQuota());
    }

    static Builder builder() {
        return new Builder();
    }

    void start() {
        scheduler.start();
    }

    @Override
    public void close() {
        scheduler.stop();
    }

    void awaitIdle(long timeoutSeconds) throws InterruptedException {
        scheduler.awaitQuiet(timeoutSeconds, TimeUnit.SECONDS);
    }

    Job require(String jobId) {
        return store.require(jobId);
    }

    static SubmitJobRequest request(String idem, String tenant, String type, JobPriority priority) {
        SubmitJobRequest r = new SubmitJobRequest();
        r.setIdempotencyKey(idem);
        r.setTenantId(tenant);
        r.setJobType(type);
        r.setPriority(priority);
        return r;
    }

    static final class Builder {
        private final SchedulerProperties p = new SchedulerProperties();
        private final java.util.List<com.github.lechandonga.batchjobschedulerjava.core.handler.JobHandler> handlers = new java.util.ArrayList<>();

        Builder() {
            p.setTickMillis(5);
            p.setWorkerThreads(4);
            p.setDefaultTimeoutMillis(2_000);
            p.setDefaultMaxAttempts(1);
            p.setRetryBackoffMillis(20);
            p.setRetryMaxBackoffMillis(500);
            p.setFairnessQuantumMillis(100);
            p.setDefaultTenantQueuedQuota(100);
            p.setDefaultTenantRunningQuota(100);
        }

        Builder workers(int n) { p.setWorkerThreads(n); return this; }
        Builder capacity(int n) { p.setGlobalQueueCapacity(n); return this; }
        Builder queuedQuota(String tenant, int q) { p.getTenantQueuedQuota().put(tenant, q); return this; }
        Builder runningQuota(String tenant, int q) { p.getTenantRunningQuota().put(tenant, q); return this; }
        Builder fairnessQuantum(long ms) { p.setFairnessQuantumMillis(ms); return this; }
        Builder backoff(long ms) { p.setRetryBackoffMillis(ms); return this; }
        Builder tick(long ms) { p.setTickMillis(ms); return this; }
        Builder handler(com.github.lechandonga.batchjobschedulerjava.core.handler.JobHandler h) { handlers.add(h); return this; }

        SchedulerTestHarness build() {
            return new SchedulerTestHarness(p, handlers);
        }
    }
}
