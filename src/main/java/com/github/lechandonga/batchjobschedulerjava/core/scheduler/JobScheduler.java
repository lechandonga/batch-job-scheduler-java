package com.github.lechandonga.batchjobschedulerjava.core.scheduler;

import com.github.lechandonga.batchjobschedulerjava.config.SchedulerProperties;
import com.github.lechandonga.batchjobschedulerjava.core.handler.JobHandler;
import com.github.lechandonga.batchjobschedulerjava.core.handler.JobHandlerRegistry;
import com.github.lechandonga.batchjobschedulerjava.core.model.Job;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobPriority;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobStatus;
import com.github.lechandonga.batchjobschedulerjava.core.quota.TenantQuotaManager;
import com.github.lechandonga.batchjobschedulerjava.core.store.JobStore;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Single-dispatcher / bounded-worker in-memory scheduler.
 *
 * Scheduling & fairness
 * ---------------------
 * Every tick the dispatcher snapshots all QUEUED jobs whose backoff has elapsed, and ranks
 * them by <em>effective priority</em>: the declared priority aged (boosted) by one level per
 * {@code scheduler.fairness-quantum-millis} of queue waiting. This guarantees that a
 * continuously arriving stream of HIGH jobs can never starve NORMAL/LOW jobs: after bounded
 * waiting their effective priority catches up. Ties are broken FIFO via the global sequence.
 *
 * A job is started only while (a) a global worker permit is free and (b) the tenant has a
 * free RUNNING permit. Permits are released exactly once per attempt/terminal outcome.
 */
public class JobScheduler {

    private static final Logger log = LoggerFactory.getLogger(JobScheduler.class);

    private final JobStore store;
    private final TenantQuotaManager quotaManager;
    private final JobHandlerRegistry handlerRegistry;
    private final SchedulerProperties props;

    private final List<Job> queue = new CopyOnWriteArrayList<>();
    private final Semaphore workerPermits;

    private volatile ExecutorService workers;
    private volatile ScheduledExecutorService dispatcher;
    private volatile ScheduledExecutorService timeouts;
    private volatile boolean started;

    public JobScheduler(JobStore store, TenantQuotaManager quotaManager,
                        JobHandlerRegistry handlerRegistry, SchedulerProperties props) {
        this.store = store;
        this.quotaManager = quotaManager;
        this.handlerRegistry = handlerRegistry;
        this.props = props;
        this.workerPermits = new Semaphore(props.getWorkerThreads());
    }

    @PostConstruct
    public synchronized void start() {
        if (started) {
            return;
        }
        quotaManager.applyConfig(props.getTenantQueuedQuota(), props.getTenantRunningQuota(),
                props.getDefaultTenantQueuedQuota(), props.getDefaultTenantRunningQuota());
        workers = Executors.newFixedThreadPool(props.getWorkerThreads(), r -> {
            Thread t = new Thread(r, "job-worker");
            t.setDaemon(true);
            return t;
        });
        dispatcher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "job-dispatcher");
            t.setDaemon(true);
            return t;
        });
        timeouts = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "job-timeout");
            t.setDaemon(true);
            return t;
        });
        dispatcher.scheduleAtFixedRate(this::safeDispatch, props.getTickMillis(),
                props.getTickMillis(), TimeUnit.MILLISECONDS);
        started = true;
        log.info("scheduler started: workerThreads={}, globalQueueCapacity={}, defaultRunningQuota={}, "
                        + "fairnessQuantum={}ms, retryBackoff={}ms", props.getWorkerThreads(),
                props.getGlobalQueueCapacity(), props.getDefaultTenantRunningQuota(),
                props.getFairnessQuantumMillis(), props.getRetryBackoffMillis());
    }

    @PreDestroy
    public synchronized void stop() {
        if (!started) {
            return;
        }
      started = false;
        shutdown(dispatcher);
        shutdown(timeouts);
        shutdown(workers);
        log.info("scheduler stopped");
    }

    private static void shutdown(ExecutorService es) {
        if (es == null) {
            return;
        }
        es.shutdownNow();
        try {
            es.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void enqueue(Job job) {
        queue.add(job);
    }

    /** Snapshot of pending jobs for tests / inspection. */
    public List<Job> queuedJobs() {
        return List.copyOf(queue);
    }

    // ------------------------------------------------------------------ dispatch

    private void safeDispatch() {
        try {
            dispatch();
        } catch (Throwable t) {
            // A dispatch failure must never kill the scheduler thread or other jobs.
            log.error("[dispatch] unexpected error, scheduler keeps running", t);
        }
    }

    void dispatch() {
        Instant now = Instant.now();
        List<Job> eligible = new ArrayList<>();
        for (Job job : queue) {
            if (job.getStatus() == JobStatus.QUEUED
                    && (job.getNotBefore() == null || !job.getNotBefore().isAfter(now))) {
                eligible.add(job);
            }
        }
        eligible.sort(Comparator
                .comparingInt((Job j) -> effectivePriority(j, now).ordinal())
                .thenComparingLong(Job::getSequence));

        for (Job job : eligible) {
            if (workerPermits.availablePermits() == 0) {
                return;
            }
            if (quotaManager.runningUsedOf(job.getTenantId()) >= quotaManager.runningQuotaOf(job.getTenantId())) {
                continue;
            }
            synchronized (job.stateLock()) {
                if (job.getStatus() != JobStatus.QUEUED) {
                    queue.remove(job);
                    continue;
                }
                if (!workerPermits.tryAcquire()) {
                    return;
                }
                if (!quotaManager.tryAcquireRunning(job.getTenantId())) {
                    workerPermits.release();
                    continue;
                }
                if (!job.casTransit(JobStatus.QUEUED, JobStatus.RUNNING)) {
                    quotaManager.releaseRunning(job.getTenantId());
                    workerPermits.release();
                    if (job.getStatus().isTerminal()) {
                        queue.remove(job);
                    }
                    continue;
                }
                if (!job.markPermitAcquired()) {
                    quotaManager.releaseRunning(job.getTenantId());
                    workerPermits.release();
                    continue;
                }
                job.resetAttemptSlots();
            }
            queue.remove(job);
            int attempt = job.nextAttempt();
            log.info("[dispatch] jobId={}, tenant={}, type={}, priority={}->effective={}, attempt={}/{}",
                    job.getJobId(), job.getTenantId(), job.getJobType(), job.getPriority(),
                    effectivePriority(job, now), attempt, job.getMaxAttempts());
            workers.submit(() -> runAttempt(job, attempt));
        }
    }

    /** Declared priority boosted by one level per quantum spent waiting (aging -> no starvation). */
    private JobPriority effectivePriority(Job job, Instant now) {
        JobPriority base = job.getPriority();
        long waitedMs = Duration.between(job.getCreatedAt(), now).toMillis();
        long boosts = props.getFairnessQuantumMillis() <= 0
                ? 0 : waitedMs / props.getFairnessQuantumMillis();
        int ordinal = Math.max(0, base.ordinal() - (int) Math.min(boosts, base.ordinal()));
        return JobPriority.values()[ordinal];
    }

    // ------------------------------------------------------------------ execution

    private void runAttempt(Job job, int attempt) {
        JobHandler handler;
        try {
            handler = handlerRegistry.require(job.getJobType());
        } catch (RuntimeException e) {
            log.error("[execute] no handler for jobId={}, type={}", job.getJobId(), job.getJobType(), e);
            settleFailure(job, attempt, e, false);
            return;
        }

        Thread runner = Thread.currentThread();
        job.setRunnerThread(runner);
        final boolean[] timedOut = {false};
        java.util.concurrent.ScheduledFuture<?> timeoutTask = timeouts.schedule(() -> {
            if (!job.claimAttemptOutcome(attempt)) {
                return;
            }
            timedOut[0] = true;
            onAttemptTimeout(job, attempt, runner);
        }, job.getTimeoutMillis(), TimeUnit.MILLISECONDS);

        Throwable failure = null;
        String output = null;
        try {
            output = handler.execute(job.getPayload());
        } catch (Throwable t) {
            failure = t;
        } finally {
            timeoutTask.cancel(false);
        }
        if (timedOut[0]) {
            // Timeout path owns this attempt (may be retrying or terminal). Do nothing further.
            return;
        }
        if (failure != null) {
            settleFailure(job, attempt, failure, false);
        } else {
            settleSuccess(job, output, attempt);
        }
    }

    private void onAttemptTimeout(Job job, int attempt, Thread runner) {
        String message = "attempt " + attempt + " timed out after " + job.getTimeoutMillis() + "ms";
        log.warn("[timeout] jobId={}, tenant={}, attempt={}/{}, interrupting worker",
                job.getJobId(), job.getTenantId(), attempt, job.getMaxAttempts());
        job.setErrorMessage(message);
        runner.interrupt();
        // TIMED_OUT is only a terminal state when no retries remain; otherwise the job goes
        // back to QUEUED for backoff (RUNNING -> QUEUED is a legal transition).
        settleFailure(job, attempt, new RuntimeException(message), true);
    }

    private void settleSuccess(Job job, String output, int attempt) {
        if (!job.claimAttemptOutcome(attempt)) {
            return;
        }
        if (job.casTransit(JobStatus.RUNNING, JobStatus.SUCCEEDED)) {
            job.setResult(output);
            log.info("[succeeded] jobId={}, tenant={}, attempts={}", job.getJobId(),
                    job.getTenantId(), job.getAttemptCount());
            finalizeTerminal(job);
        }
    }

    /**
     * Common settlement for exceptions and timeouts. Exactly one caller wins the attempt;
     * the loser (e.g. handler returning just after timeout) must not emit any conclusion.
     */
    private void settleFailure(Job job, int attempt, Throwable cause, boolean fromTimeout) {
        if (!fromTimeout && !job.claimAttemptOutcome(attempt)) {
            return;
        }
        if (fromTimeout) {
            // Allow retry from the timeout path: park directly from RUNNING.
        }
        log.warn("[attempt-failed] jobId={}, tenant={}, attempt={}/{}, timeout={}, error={}",
                job.getJobId(), job.getTenantId(), attempt, job.getMaxAttempts(),
                fromTimeout, cause.toString());
        if (attempt < job.getMaxAttempts()) {
            long backoff = backoffMillis(attempt);
            if (job.parkForRetry(backoff)) {
                releaseRunningPermit(job);
                job.setErrorMessage(cause.getMessage() + " | retry scheduled after " + backoff + "ms");
                log.info("[retry] jobId={}, tenant={}, nextAttempt={}/{}, backoff={}ms",
                        job.getJobId(), job.getTenantId(), attempt + 1, job.getMaxAttempts(), backoff);
                enqueue(job);
                releaseWorkerSlot(job);
                return;
            }
            // Job was concurrently cancelled into terminal state while the attempt ran.
            releaseRunningPermit(job);
            releaseWorkerSlot(job);
            return;
        }
        JobStatus terminal = fromTimeout ? JobStatus.TIMED_OUT : JobStatus.FAILED;
        if (job.casTransit(JobStatus.RUNNING, terminal)) {
            job.setErrorMessage(cause.getMessage());
            log.warn("[terminal-{}] jobId={}, tenant={}, attempts={}",
                    terminal.name().toLowerCase(), job.getJobId(), job.getTenantId(),
                    job.getAttemptCount());
            finalizeTerminal(job);
        } else {
            releaseRunningPermit(job);
            releaseWorkerSlot(job);
        }
    }

    private long backoffMillis(int attempt) {
        long base = props.getRetryBackoffMillis();
        long capped = Math.min(props.getRetryMaxBackoffMillis(),
                base * (1L << Math.min(attempt - 1, 16)));
        return Math.max(0, capped);
    }

    private void finalizeTerminal(Job job) {
        releaseRunningPermit(job);
        if (job.consumeQueuedPermit()) {
            quotaManager.releaseQueued(job.getTenantId());
        }
        store.onTerminal(job);
        releaseWorkerSlot(job);
    }

    private void releaseRunningPermit(Job job) {
        if (job.consumeRunningPermit()) {
            quotaManager.releaseRunning(job.getTenantId());
        }
    }

    private void releaseWorkerSlot(Job job) {
        if (job.markWorkerSlotReleased()) {
            workerPermits.release();
        }
    }

    // ------------------------------------------------------------------ cancel

    /**
     * Cancels a job. QUEUED jobs end CANCELLED immediately (no attempt ever runs); a RUNNING
     * job is interrupted and ends CANCELLED unless its attempt was already settled
     * (success/failure/timeout). Only the winning canceller releases permits.
     */
    public boolean cancel(String jobId) {
        Job job = store.require(jobId);
        if (job.getStatus() == JobStatus.CANCELLED || job.getStatus().isTerminal()) {
            return false;
        }
        if (job.casTransit(JobStatus.QUEUED, JobStatus.CANCELLED)) {
            log.info("[cancel] queued jobId={}, tenant={}", jobId, job.getTenantId());
            queue.remove(job);
            finalizeTerminal(job);
            return true;
        }
        if (job.casTransit(JobStatus.RUNNING, JobStatus.CANCELLED)) {
            job.setErrorMessage("cancelled while running");
            log.info("[cancel] running jobId={}, tenant={}, interrupting worker", jobId,
                    job.getTenantId());
            Thread runner = job.getRunnerThread();
            if (runner != null) {
                runner.interrupt();
            }
            finalizeTerminal(job);
            return true;
        }
        return false;
    }

    /** Test hook: block until no active jobs remain (queued, retrying or running). */
    public void awaitQuiet(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            if (store.activeCount() == 0) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("scheduler did not become quiet, active=" + store.activeCount());
    }

    public Optional<Job> findJob(String jobId) {
        return store.find(jobId);
    }
}
