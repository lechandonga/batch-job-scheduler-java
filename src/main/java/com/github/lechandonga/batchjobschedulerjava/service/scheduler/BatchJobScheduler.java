package com.github.lechandonga.batchjobschedulerjava.service.scheduler;

import com.github.lechandonga.batchjobschedulerjava.config.SchedulerProperties;
import com.github.lechandonga.batchjobschedulerjava.exception.IllegalJobTransitionException;
import com.github.lechandonga.batchjobschedulerjava.exception.JobNotFoundException;
import com.github.lechandonga.batchjobschedulerjava.exception.JobRejectedException;
import com.github.lechandonga.batchjobschedulerjava.exception.RejectReason;
import com.github.lechandonga.batchjobschedulerjava.model.Job;
import com.github.lechandonga.batchjobschedulerjava.model.JobStatus;
import com.github.lechandonga.batchjobschedulerjava.model.JobView;
import com.github.lechandonga.batchjobschedulerjava.model.SubmitJobRequest;
import com.github.lechandonga.batchjobschedulerjava.model.TerminalReason;
import com.github.lechandonga.batchjobschedulerjava.service.handler.JobExecutionException;
import com.github.lechandonga.batchjobschedulerjava.service.handler.JobHandler;
import com.github.lechandonga.batchjobschedulerjava.service.handler.JobHandlerRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 进程内批量作业调度器：内存队列 + 租户配额 + 真实执行，无数据库 / 外部中间件依赖。
 *
 * <p>并发模型：
 * <ul>
 *     <li>{@code queueLock} 保护“待调度队列 + 配额计数”，提交 / 取消 / 调度循环都经过它；</li>
 *     <li>每个 {@link Job} 自身内置锁保护状态迁移，迁移必须用 {@code tryTransition(expected,...)}
 *         比较并交换，因此执行完成与取消/超时之间天然互斥，最终结论唯一；</li>
 *     <li>作业在虚拟线程中执行，单作业异常 / 超时被完整隔离。</li>
 * </ul>
 */
@Service
public class BatchJobScheduler {

    private static final Logger log = LoggerFactory.getLogger(BatchJobScheduler.class);

    private final SchedulerProperties props;
    private final JobHandlerRegistry registry;

    private final Map<String, Job> jobsById = new ConcurrentHashMap<>();
    private final Map<String, Job> jobsByIdempotencyKey = new ConcurrentHashMap<>();

    private final Object queueLock = new Object();
    private final Set<Job> queuedJobs = new HashSet<>();
    private final Map<String, Integer> activeByTenant = new HashMap<>();
    private final Map<String, Integer> runningByTenant = new HashMap<>();
    private int globalRunning;

    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private volatile boolean running;
    private Thread dispatcher;

    public BatchJobScheduler(SchedulerProperties props, JobHandlerRegistry registry) {
        this.props = props;
        this.registry = registry;
    }

    @PostConstruct
    void start() {
        running = true;
        dispatcher = Thread.ofPlatform().name("job-dispatcher").daemon(true).start(this::dispatchLoop);
        log.info("批量作业调度器已启动: queueCapacity={}, workerThreads={}, defaultTenantQuota={}, tenantQuotas={}",
                props.getQueueCapacity(), props.getWorkerThreads(),
                props.getDefaultTenantQuota(), props.getTenantQuotas());
    }

    @PreDestroy
    void stop() {
        running = false;
        workers.shutdownNow();
        if (dispatcher != null) {
            dispatcher.interrupt();
        }
    }

    /* ============================== 提交 / 查询 / 取消 ============================== */

    /**
     * 提交作业。同一幂等键（含并发重复提交）只产生一个作业，重复提交返回既有作业。
     * 被拒绝的提交在任何计数之前抛出异常，不占用队列容量或租户配额。
     */
    public JobView submit(SubmitJobRequest request) {
        validate(request);
        registry.find(request.getHandlerType())
                .orElseThrow(() -> new JobRejectedException(RejectReason.UNKNOWN_HANDLER,
                        "未知的作业处理器类型: " + request.getHandlerType()
                                + "，已注册类型: " + registry.registeredTypes()));

        long timeout = request.getTimeoutMillis() == null
                ? props.getDefaultTimeoutMillis() : request.getTimeoutMillis();
        int maxAttempts = request.getMaxAttempts() == null
                ? props.getDefaultMaxAttempts() : request.getMaxAttempts();

        synchronized (queueLock) {
            Job existing = jobsByIdempotencyKey.get(request.getIdempotencyKey());
            if (existing != null) {
                log.info("幂等键重复提交，返回既有作业: key={}, jobId={}, status={}",
                        request.getIdempotencyKey(), existing.getJobId(), existing.getStatus());
                return snapshot(existing);
            }
            if (queuedJobs.size() >= props.getQueueCapacity()) {
                throw new JobRejectedException(RejectReason.QUEUE_FULL,
                        "全局待调度队列已满，容量=" + props.getQueueCapacity());
            }
            int quota = quotaOf(request.getTenantId());
            int active = activeByTenant.getOrDefault(request.getTenantId(), 0);
            if (active >= quota) {
                throw new JobRejectedException(RejectReason.TENANT_QUOTA_EXCEEDED,
                        "租户 " + request.getTenantId() + " 配额不足: 配额=" + quota + ", 已用=" + active);
            }

            Job job = new Job(request.getIdempotencyKey(), request.getTenantId(), request.getPriority(),
                    request.getHandlerType(), request.getPayload(), timeout, maxAttempts,
                    System.currentTimeMillis(), props.getAgingIntervalMillis());
            jobsById.put(job.getJobId(), job);
            jobsByIdempotencyKey.put(job.getIdempotencyKey(), job);
            queuedJobs.add(job);
            activeByTenant.merge(job.getTenantId(), 1, Integer::sum);
            log.info("作业已接受: jobId={}, tenant={}, priority={}, handler={}, active={}/{}",
                    job.getJobId(), job.getTenantId(), job.getPriority(), job.getHandlerType(),
                    activeByTenant.get(job.getTenantId()), quota);
            return snapshot(job);
        }
    }

    private void validate(SubmitJobRequest request) {
        if (request == null) {
            throw new JobRejectedException(RejectReason.INVALID_PARAMETER, "请求体不能为空");
        }
        if (isBlank(request.getIdempotencyKey())) {
            throw new JobRejectedException(RejectReason.INVALID_PARAMETER, "idempotencyKey 不能为空");
        }
        if (isBlank(request.getTenantId())) {
            throw new JobRejectedException(RejectReason.INVALID_PARAMETER, "tenantId 不能为空");
        }
        if (request.getPriority() == null) {
            throw new JobRejectedException(RejectReason.INVALID_PARAMETER, "priority 不能为空");
        }
        if (request.getPriority() < 0 || request.getPriority() > 100) {
            throw new JobRejectedException(RejectReason.INVALID_PARAMETER,
                    "priority 必须在 [0,100] 之间，实际=" + request.getPriority());
        }
        if (isBlank(request.getHandlerType())) {
            throw new JobRejectedException(RejectReason.INVALID_PARAMETER, "handlerType 不能为空");
        }
        if (request.getTimeoutMillis() != null && request.getTimeoutMillis() <= 0) {
            throw new JobRejectedException(RejectReason.INVALID_PARAMETER,
                    "timeoutMillis 必须为正数，实际=" + request.getTimeoutMillis());
        }
        if (request.getMaxAttempts() != null && request.getMaxAttempts() < 1) {
            throw new JobRejectedException(RejectReason.INVALID_PARAMETER,
                    "maxAttempts 必须 >= 1，实际=" + request.getMaxAttempts());
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    public JobView getJob(String jobId) {
        return snapshot(requireJob(jobId));
    }

    public JobView getByIdempotencyKey(String key) {
        Job job = jobsByIdempotencyKey.get(key);
        if (job == null) {
            throw new JobNotFoundException(key);
        }
        return snapshot(job);
    }

    /**
     * 取消作业。
     * <ul>
     *     <li>QUEUED：直接迁移到 CANCELED（CANCELED_WHILE_QUEUED），移出队列并归还活跃配额；</li>
     *     <li>RUNNING：迁移到 CANCELED（CANCELED_WHILE_RUNNING）并中断执行线程，
     *         正在执行的尝试无论后续正常返回还是抛异常都不可能再改写最终结论；</li>
     *     <li>终态：非法迁移，抛出 {@link IllegalJobTransitionException}。</li>
     * </ul>
     */
    public JobView cancel(String jobId) {
        Job job = requireJob(jobId);
        synchronized (job) {
            JobStatus current = job.getStatus();
            if (current.isTerminal()) {
                throw new IllegalJobTransitionException(jobId, JobStatus.QUEUED, current);
            }
            if (current == JobStatus.RUNNING) {
                if (job.tryTransition(JobStatus.RUNNING, JobStatus.CANCELED,
                        TerminalReason.CANCELED_WHILE_RUNNING)) {
                    Thread runner = job.getRunnerThread();
                    if (runner != null) {
                        runner.interrupt();
                    }
                    log.info("运行中作业已取消并中断执行线程: jobId={}", jobId);
                }
            } else if (job.tryTransition(JobStatus.QUEUED, JobStatus.CANCELED,
                    TerminalReason.CANCELED_WHILE_QUEUED)) {
                releaseQueuedCancel(job);
                log.info("排队中作业已取消: jobId={}", jobId);
            }
        }
        return snapshot(job);
    }

    private Job requireJob(String jobId) {
        Job job = jobsById.get(jobId);
        if (job == null) {
            throw new JobNotFoundException(jobId);
        }
        return job;
    }

    /* ============================== 租户配额视图 ============================== */

    public Map<String, Object> tenantQuotaView(String tenantId) {
        synchronized (queueLock) {
            int queued = 0;
            for (Job job : queuedJobs) {
                if (job.getTenantId().equals(tenantId)) {
                    queued++;
                }
            }
            int active = activeByTenant.getOrDefault(tenantId, 0);
            int run = runningByTenant.getOrDefault(tenantId, 0);
            Map<String, Object> view = new HashMap<>();
            view.put("tenantId", tenantId);
            view.put("quota", quotaOf(tenantId));
            view.put("active", active);
            view.put("running", run);
            view.put("queued", active - run);
            return view;
        }
    }

    public int quotaOf(String tenantId) {
        return props.getTenantQuotas().getOrDefault(tenantId, props.getDefaultTenantQuota());
    }

    /** 测试辅助：阻塞等待所有已提交作业进入终态。 */
    public void awaitQuiet(long timeout, TimeUnit unit) {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (queueLock) {
            while (globalRunning > 0 || !queuedJobs.isEmpty()) {
                long remainingMillis = (deadline - System.nanoTime()) / 1_000_000L;
                if (remainingMillis <= 0) {
                    throw new AssertionError("等待作业静默超时: running=" + globalRunning
                            + ", queued=" + queuedJobs.size());
                }
                try {
                    queueLock.wait(remainingMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("等待被中断", e);
                }
            }
        }
    }

    private JobView snapshot(Job job) {
        return new JobView(job, Instant.now());
    }

    /* ============================== 调度循环 ============================== */

    private void dispatchLoop() {
        while (running) {
            try {
                int dispatched = 0;
                while (dispatchOne()) {
                    dispatched++;
                }
                if (dispatched > 0) {
                    continue;
                }
                Thread.sleep(props.getDispatchTickMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                // 调度循环本身永不退出：任何意外都记录后继续，保证调度器不被单个问题击穿
                log.error("调度循环发生意外，已忽略并继续", t);
            }
        }
    }

    /**
     * 选出并派发一个作业。选择规则（在 queueLock 内快照比较，不在锁内触碰作业状态）：
     * 仅考虑已到可运行时刻（退避结束）、租户运行数未达配额、全局工作线程未满的作业，
     * 按有效优先级（含老化提升）降序、同优先级按提交顺序升序选取，保证公平与不饿死。
     */
    private boolean dispatchOne() {
        Job candidate;
        synchronized (queueLock) {
            if (globalRunning >= props.getWorkerThreads()) {
                return false;
            }
            long now = System.currentTimeMillis();
            candidate = queuedJobs.stream()
                    .filter(job -> now >= job.getNextRunAtMillis())
                    .filter(job -> runningByTenant.getOrDefault(job.getTenantId(), 0)
                            < quotaOf(job.getTenantId()))
                    .min(Comparator
                            .comparingLong((Job j) -> -j.effectivePriority(now))
                            .thenComparingLong(Job::getSequence))
                    .orElse(null);
            if (candidate == null) {
                return false;
            }
            queuedJobs.remove(candidate);
            globalRunning++;
            runningByTenant.merge(candidate.getTenantId(), 1, Integer::sum);
        }

        // 状态迁移在作业锁上进行，避免 调度锁 -> 作业锁 的嵌套加锁
        if (!candidate.claimForExecution(System.currentTimeMillis())) {
            // 极小窗口内被取消（QUEUED->CANCELED）：回补运行计数，活跃配额幂等释放
            reconcileClaimLost(candidate);
            return true;
        }
        log.info("调度执行: jobId={}, tenant={}, attempt={}, effectivePriority={}",
                candidate.getJobId(), candidate.getTenantId(), candidate.getAttempt(),
                candidate.effectivePriority(System.currentTimeMillis()));
        workers.execute(() -> runJob(candidate));
        return true;
    }

    private void reconcileClaimLost(Job job) {
        synchronized (queueLock) {
            globalRunning--;
            runningByTenant.merge(job.getTenantId(), -1, Integer::sum);
            queueLock.notifyAll();
        }
        synchronized (job) {
            if (job.getStatus() == JobStatus.CANCELED) {
                releaseActiveQuota(job);
            }
        }
    }

    /* ============================== 作业执行 ============================== */

    private void runJob(Job job) {
        JobHandler handler = registry.find(job.getHandlerType()).orElse(null);
        if (handler == null) {
            // 正常情况下提交时已校验；此处兜底，保证调度器不会因处理器缺失而卡死
            finish(job, JobStatus.FAILED, TerminalReason.NON_RETRYABLE,
                    "处理器不存在: " + job.getHandlerType());
            return;
        }
        Future<String> future = null;
        String result = null;
        Throwable failure = null;
        boolean interrupted = false;
        try {
            job.setRunnerThread(Thread.currentThread());
            Future<String> handle = future = workers.submit(
                    () -> handler.execute(job.getPayload(), job.getAttempt()));
            result = handle.get(job.getTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            failure = e;
        } catch (InterruptedException e) {
            interrupted = true;
            failure = e;
        } catch (Throwable t) {
            // 处理器抛出的任何异常（含 ExecutionException 包裹的业务异常 / RuntimeException）都被隔离
            failure = unwrap(t);
        } finally {
            job.setRunnerThread(null);
            if (future != null) {
                future.cancel(true);
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        concludeAttempt(job, result, failure);
    }

    private static Throwable unwrap(Throwable t) {
        Throwable cause = t.getCause();
        return cause != null ? cause : t;
    }

    /**
     * 对一次尝试给出结论。所有终态迁移都使用 tryTransition 比较并交换：
     * 若取消方已经把 RUNNING 改为 CANCELED，这里的任何迁移都会失败，最终结论保持唯一。
     */
    private void concludeAttempt(Job job, String result, Throwable failure) {
        if (failure == null) {
            finish(job, JobStatus.SUCCEEDED, TerminalReason.SUCCESS, result);
            return;
        }

        if (failure instanceof TimeoutException) {
            if (job.getAttempt() >= job.getMaxAttempts()) {
                finish(job, JobStatus.TIMED_OUT, TerminalReason.RETRY_EXHAUSTED_TIMEOUT,
                        "连续 " + job.getAttempt() + " 次尝试均超时（单次超时="
                                + job.getTimeoutMillis() + "ms）");
            } else {
                scheduleRetry(job, "第 " + job.getAttempt() + " 次尝试超时", true);
            }
            return;
        }

        if (job.getStatus() == JobStatus.CANCELED) {
            // 取消方已赢得最终结论，执行方只需归还运行计数
            releaseRunningSlot(job);
            log.info("执行尝试晚于取消完成，维持取消结论: jobId={}, attempt={}",
                    job.getJobId(), job.getAttempt());
            return;
        }

        if (failure instanceof JobExecutionException je) {
            if (!je.isRetryable()) {
                finish(job, JobStatus.FAILED, TerminalReason.NON_RETRYABLE, je.getMessage());
                return;
            }
        } else if (failure instanceof InterruptedException) {
            // 非取消路径下的中断（如停机）：按可重试失败处理；若已被取消则上面的分支接管
            if (job.getStatus() != JobStatus.RUNNING) {
                releaseRunningSlot(job);
                return;
            }
        }
        // JobExecutionException(retryable=true) 与未预期 RuntimeException 均按可重试失败处理
        if (job.getAttempt() >= job.getMaxAttempts()) {
            boolean timeoutExhausted = failure instanceof TimeoutException;
            finish(job, JobStatus.FAILED, TerminalReason.RETRY_EXHAUSTED,
                    "重试次数已耗尽(" + job.getMaxAttempts() + ")，最后错误: " + failure);
        } else {
            scheduleRetry(job, String.valueOf(failure), failure instanceof TimeoutException);
        }
    }

    private void scheduleRetry(Job job, String error, boolean timeout) {
        long backoff = backoffMillis(job.getAttempt());
        long now = System.currentTimeMillis();
        boolean requeued;
        synchronized (job) {
            requeued = job.requeueForRetry(now, backoff, error);
        }
        if (!requeued) {
            // 在给出结论前被取消
            releaseRunningSlot(job);
            return;
        }
        synchronized (queueLock) {
            globalRunning--;
            runningByTenant.merge(job.getTenantId(), -1, Integer::sum);
            queuedJobs.add(job);
            queueLock.notifyAll();
        }
        log.info("作业退避重试: jobId={}, nextAttempt={}, backoff={}ms, timeout={}, error={}",
                job.getJobId(), job.getAttempt() + 1, backoff, timeout, error);
    }

    private long backoffMillis(int failedAttempt) {
        long shift = Math.min(failedAttempt - 1, 16);
        long backoff = props.getBaseBackoffMillis() * (1L << shift);
        return Math.min(backoff, props.getMaxBackoffMillis());
    }

    private void finish(Job job, JobStatus terminal, TerminalReason reason, String message) {
        synchronized (job) {
            if (job.getStatus().isTerminal()) {
                // 取消等路径已先赢得终态：不覆盖结论，仅在需要时归还计数
                if (job.getStatus() == JobStatus.CANCELED) {
                    releaseRunningSlot(job);
                }
                return;
            }
            job.markFinished(terminal, reason, message, System.currentTimeMillis());
        }
        releaseRunningSlot(job);
        log.info("作业结束: jobId={}, status={}, reason={}, attempts={}",
                job.getJobId(), terminal, reason, job.getAttempt());
    }

    private void releaseQueuedCancel(Job job) {
        synchronized (queueLock) {
            if (queuedJobs.remove(job)) {
                releaseActiveQuota(job);
            }
            queueLock.notifyAll();
        }
    }

    /** RUNNING -> 任意终态后归还“运行槽 + 活跃配额”，无论成功/失败/取消/超时都恰好执行一次。 */
    private void releaseRunningSlot(Job job) {
        synchronized (queueLock) {
            globalRunning--;
            runningByTenant.merge(job.getTenantId(), -1, Integer::sum);
            releaseActiveQuota(job);
            queueLock.notifyAll();
        }
    }

    private void releaseActiveQuota(Job job) {
        activeByTenant.merge(job.getTenantId(), -1, Integer::sum);
    }
}
