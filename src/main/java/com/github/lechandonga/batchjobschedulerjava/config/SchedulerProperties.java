package com.github.lechandonga.batchjobschedulerjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "scheduler")
public class SchedulerProperties {

    /** Global bound on number of QUEUED (including retrying) + RUNNING jobs across all tenants. */
    private int globalQueueCapacity = 1024;
    /** Size of the worker pool, i.e. global concurrency cap. */
    private int workerThreads = 8;
    /** Dispatcher wake-up interval in millis. */
    private long tickMillis = 20;
    /** Default per-tenant submission (QUEUED+RUNNING) quota. */
    private int defaultTenantQueuedQuota = 64;
    /** Default per-tenant concurrent RUNNING quota. */
    private int defaultTenantRunningQuota = 2;
    /** Default attempt timeout in millis. */
    private long defaultTimeoutMillis = 5_000;
    /** Default max attempts (1 = no retry). */
    private int defaultMaxAttempts = 3;
    /** Base backoff between attempts; delay = baseBackoffMillis * 2^(attempt-1). */
    private long retryBackoffMillis = 200;
    /** Max backoff cap in millis. */
    private long retryMaxBackoffMillis = 5_000;
    /** Priority boost per queue-wait quantum: each quantum of waiting moves a job up one level. */
    private long fairnessQuantumMillis = 200;

    private Map<String, Integer> tenantQueuedQuota = new LinkedHashMap<>();
    private Map<String, Integer> tenantRunningQuota = new LinkedHashMap<>();

    public int getGlobalQueueCapacity() { return globalQueueCapacity; }
    public void setGlobalQueueCapacity(int globalQueueCapacity) { this.globalQueueCapacity = globalQueueCapacity; }
    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    public long getTickMillis() { return tickMillis; }
    public void setTickMillis(long tickMillis) { this.tickMillis = tickMillis; }
    public int getDefaultTenantQueuedQuota() { return defaultTenantQueuedQuota; }
    public void setDefaultTenantQueuedQuota(int v) { this.defaultTenantQueuedQuota = v; }
    public int getDefaultTenantRunningQuota() { return defaultTenantRunningQuota; }
    public void setDefaultTenantRunningQuota(int v) { this.defaultTenantRunningQuota = v; }
    public long getDefaultTimeoutMillis() { return defaultTimeoutMillis; }
    public void setDefaultTimeoutMillis(long v) { this.defaultTimeoutMillis = v; }
    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public void setDefaultMaxAttempts(int v) { this.defaultMaxAttempts = v; }
    public long getRetryBackoffMillis() { return retryBackoffMillis; }
    public void setRetryBackoffMillis(long v) { this.retryBackoffMillis = v; }
    public long getRetryMaxBackoffMillis() { return retryMaxBackoffMillis; }
    public void setRetryMaxBackoffMillis(long v) { this.retryMaxBackoffMillis = v; }
    public long getFairnessQuantumMillis() { return fairnessQuantumMillis; }
    public void setFairnessQuantumMillis(long v) { this.fairnessQuantumMillis = v; }
    public Map<String, Integer> getTenantQueuedQuota() { return tenantQueuedQuota; }
    public void setTenantQueuedQuota(Map<String, Integer> v) { this.tenantQueuedQuota = v; }
    public Map<String, Integer> getTenantRunningQuota() { return tenantRunningQuota; }
    public void setTenantRunningQuota(Map<String, Integer> v) { this.tenantRunningQuota = v; }
}
