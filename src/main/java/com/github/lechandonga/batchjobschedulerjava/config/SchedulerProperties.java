package com.github.lechandonga.batchjobschedulerjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 调度器配置项（前缀 scheduler）。
 */
@ConfigurationProperties(prefix = "scheduler")
public class SchedulerProperties {

    /** 全局待调度队列容量（QUEUED 作业总数上限，不包含正在运行的作业）。 */
    private int queueCapacity = 1000;

    /** 工作线程上限：全局同时处于 RUNNING 的作业数上限。 */
    private int workerThreads = 8;

    /** 默认租户并发配额：某租户同时处于活跃（QUEUED+RUNNING）的作业数上限。 */
    private int defaultTenantQuota = 4;

    /** 单独覆盖指定租户的配额，key=租户ID。 */
    private Map<String, Integer> tenantQuotas = new LinkedHashMap<>();

    /** 单次执行（每次 attempt）的默认超时毫秒。 */
    private long defaultTimeoutMillis = 5_000L;

    /** 每个作业的默认最大尝试次数（1 表示不重试）。 */
    private int defaultMaxAttempts = 3;

    /** 失败重试退避基准毫秒，第 n 次重试等待 baseBackoffMillis * 2^(n-1)。 */
    private long baseBackoffMillis = 100L;

    /** 退避上限毫秒。 */
    private long maxBackoffMillis = 5_000L;

    /** 优先级老化间隔：作业每等待该时长，有效优先级提升 1 档，用于防止低优先级饿死。 */
    private long agingIntervalMillis = 200L;

    /** 调度循环空转等待毫秒。 */
    private long dispatchTickMillis = 10L;

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public int getDefaultTenantQuota() {
        return defaultTenantQuota;
    }

    public void setDefaultTenantQuota(int defaultTenantQuota) {
        this.defaultTenantQuota = defaultTenantQuota;
    }

    public Map<String, Integer> getTenantQuotas() {
        return tenantQuotas;
    }

    public void setTenantQuotas(Map<String, Integer> tenantQuotas) {
        this.tenantQuotas = tenantQuotas;
    }

    public long getDefaultTimeoutMillis() {
        return defaultTimeoutMillis;
    }

    public void setDefaultTimeoutMillis(long defaultTimeoutMillis) {
        this.defaultTimeoutMillis = defaultTimeoutMillis;
    }

    public int getDefaultMaxAttempts() {
        return defaultMaxAttempts;
    }

    public void setDefaultMaxAttempts(int defaultMaxAttempts) {
        this.defaultMaxAttempts = defaultMaxAttempts;
    }

    public long getBaseBackoffMillis() {
        return baseBackoffMillis;
    }

    public void setBaseBackoffMillis(long baseBackoffMillis) {
        this.baseBackoffMillis = baseBackoffMillis;
    }

    public long getMaxBackoffMillis() {
        return maxBackoffMillis;
    }

    public void setMaxBackoffMillis(long maxBackoffMillis) {
        this.maxBackoffMillis = maxBackoffMillis;
    }

    public long getAgingIntervalMillis() {
        return agingIntervalMillis;
    }

    public void setAgingIntervalMillis(long agingIntervalMillis) {
        this.agingIntervalMillis = agingIntervalMillis;
    }

    public long getDispatchTickMillis() {
        return dispatchTickMillis;
    }

    public void setDispatchTickMillis(long dispatchTickMillis) {
        this.dispatchTickMillis = dispatchTickMillis;
    }
}
