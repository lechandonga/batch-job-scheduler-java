package com.github.lechandonga.batchjobschedulerjava.model;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 作业实体。所有状态判断与迁移都在对象内置锁（synchronized 方法）内完成，
 * 保证同一作业在任何并发场景下都只发生一次合法迁移，不会出现重复执行或互相矛盾的最终结论。
 */
public class Job {

    private static final AtomicLong SEQUENCE = new AtomicLong();

    /** 每等待 agingIntervalMillis，有效优先级提升 1 档。 */
    public static final long DEFAULT_AGING_INTERVAL_MILLIS = 200L;

    private final String jobId;
    private final String idempotencyKey;
    private final String tenantId;
    private final int priority;
    private final String handlerType;
    private final String payload;
    private final long timeoutMillis;
    private final int maxAttempts;
    private final long submittedAtMillis;
    private final long sequence;
    private final long agingIntervalMillis;

    private JobStatus status = JobStatus.QUEUED;
    private TerminalReason terminalReason;
    private int attempt;
    private long nextRunAtMillis;
    private long startedAtMillis;
    private long finishedAtMillis;
    private String resultMessage;
    private String lastError;

    /** 执行线程持有的中断标记，取消运行中作业时由调度器置位。 */
    private volatile Thread runnerThread;

    public Job(String idempotencyKey, String tenantId, int priority, String handlerType, String payload,
               long timeoutMillis, int maxAttempts, long nowMillis, long agingIntervalMillis) {
        this.jobId = UUID.randomUUID().toString();
        this.idempotencyKey = idempotencyKey;
        this.tenantId = tenantId;
        this.priority = priority;
        this.handlerType = handlerType;
        this.payload = payload;
        this.timeoutMillis = timeoutMillis;
        this.maxAttempts = maxAttempts;
        this.submittedAtMillis = nowMillis;
        this.nextRunAtMillis = nowMillis;
        this.sequence = SEQUENCE.incrementAndGet();
        this.agingIntervalMillis = agingIntervalMillis;
    }

    /**
     * 有效优先级 = 静态优先级 + 老化提升。等待越久提升越多，从而保证低优先级不会被持续到来的
     * 高优先级作业无限饿死（调度时按有效优先级从大到小选取）。
     */
    public int effectivePriority(long nowMillis) {
        long waited = Math.max(0, nowMillis - submittedAtMillis);
        return priority + (int) (waited / agingIntervalMillis);
    }

    /**
     * 尝试从期望状态迁移到目标状态。迁移必须由当前状态唯一匹配才能成功，
     * 这使得“执行完成”和“取消/超时”之间天然互斥，先到者得到唯一最终结论。
     *
     * @return true 表示本次调用赢得了迁移；false 表示作业已处于其他状态（通常是终态），调用方必须放弃
     */
    public synchronized boolean tryTransition(JobStatus expected, JobStatus target, TerminalReason reason) {
        if (status != expected) {
            return false;
        }
        status = target;
        if (target.isTerminal()) {
            terminalReason = reason;
            finishedAtMillis = System.currentTimeMillis();
            notifyAll();
        }
        return true;
    }

    /**
     * 要求作业必须处于期望状态，否则抛出带可区分原因的非法迁移异常。
     */
    public synchronized void requireStatus(JobStatus expected) {
        if (status != expected) {
            throw new com.github.lechandonga.batchjobschedulerjava.exception.IllegalJobTransitionException(
                    jobId, expected, status);
        }
    }

    public synchronized boolean claimForExecution(long nowMillis) {
        if (status != JobStatus.QUEUED || nowMillis < nextRunAtMillis) {
            return false;
        }
        status = JobStatus.RUNNING;
        attempt++;
        startedAtMillis = nowMillis;
        return true;
    }

    /**
     * 运行失败后准备重试：回到 QUEUED 并安排退避后的下次可运行时刻。
     */
    public synchronized boolean requeueForRetry(long nowMillis, long backoffMillis, String error) {
        if (status != JobStatus.RUNNING) {
            return false;
        }
        status = JobStatus.QUEUED;
        nextRunAtMillis = nowMillis + backoffMillis;
        lastError = error;
        return true;
    }

    public synchronized void markFinished(JobStatus terminal, TerminalReason reason,
                                          String message, long nowMillis) {
        status = terminal;
        terminalReason = reason;
        finishedAtMillis = nowMillis;
        if (message != null) {
            if (terminal == JobStatus.SUCCEEDED) {
                resultMessage = message;
            } else {
                lastError = message;
            }
        }
        notifyAll();
    }

    public synchronized void awaitTerminal(long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!status.isTerminal()) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                return;
            }
            wait(remaining);
        }
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

    public String getHandlerType() {
        return handlerType;
    }

    public String getPayload() {
        return payload;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public long getSubmittedAtMillis() {
        return submittedAtMillis;
    }

    public long getSequence() {
        return sequence;
    }

    public synchronized JobStatus getStatus() {
        return status;
    }

    public synchronized TerminalReason getTerminalReason() {
        return terminalReason;
    }

    public synchronized int getAttempt() {
        return attempt;
    }

    public synchronized long getNextRunAtMillis() {
        return nextRunAtMillis;
    }

    public synchronized long getStartedAtMillis() {
        return startedAtMillis;
    }

    public synchronized long getFinishedAtMillis() {
        return finishedAtMillis;
    }

    public synchronized String getResultMessage() {
        return resultMessage;
    }

    public synchronized String getLastError() {
        return lastError;
    }

    public Thread getRunnerThread() {
        return runnerThread;
    }

    public void setRunnerThread(Thread runnerThread) {
        this.runnerThread = runnerThread;
    }
}
