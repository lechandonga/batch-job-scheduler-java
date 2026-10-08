package com.github.lechandonga.batchjobschedulerjava.tests;

import com.github.lechandonga.batchjobschedulerjava.api.dto.SubmitJobRequest;
import com.github.lechandonga.batchjobschedulerjava.core.exception.IllegalJobTransitionException;
import com.github.lechandonga.batchjobschedulerjava.core.handler.JobHandler;
import com.github.lechandonga.batchjobschedulerjava.core.model.Job;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobPriority;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobStatus;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("业务3: 执行控制 - 超时/取消/重试/非法状态/故障隔离")
class ExecutionControlTest {

    private static final Logger log = LoggerFactory.getLogger(ExecutionControlTest.class);

    static class LatchSleepHandler implements JobHandler {
        final CountDownLatch entered;
        LatchSleepHandler(CountDownLatch entered) { this.entered = entered; }
        @Override public String jobType() { return "latched-sleep"; }
        @Override public String execute(String payload) throws Exception {
            entered.countDown();
            Thread.sleep(Long.parseLong(payload.trim()));
            return "done";
        }
    }

    static class CountingFailHandler implements JobHandler {
        final AtomicInteger calls = new AtomicInteger();
        final int succeedOn;
        CountingFailHandler(int succeedOn) { this.succeedOn = succeedOn; }
        @Override public String jobType() { return "counting-fail"; }
        @Override public String execute(String payload) {
            int n = calls.incrementAndGet();
            if (n < succeedOn) {
                throw new IllegalStateException("boom attempt " + n);
            }
            return "ok-on-" + n;
        }
    }

    private Job submitRun(SchedulerTestHarness h, String idem, String type, String payload,
                          Long timeout, Integer maxAttempts) {
        SubmitJobRequest r = SchedulerTestHarness.request(idem, "t-exec", type, JobPriority.NORMAL);
        r.setPayload(payload);
        if (timeout != null) {
            r.setTimeoutMillis(timeout);
        }
        if (maxAttempts != null) {
            r.setMaxAttempts(maxAttempts);
        }
        return h.require(h.service.submit(r).jobId());
    }

    @Test
    @DisplayName("[CASE:执行超时] 超过 timeoutMillis 的作业以 TIMED_OUT 结束，配额正确归还")
    void timeoutEndsJobAsTimedOut() throws Exception {
        log.info("[CASE:执行超时] start: timeout=150ms, 作业睡眠 1000ms");
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .handler(new com.github.lechandonga.batchjobschedulerjava.core.handler.BuiltinHandlers.SleepHandler())
                .build()) {
            h.start();
            Job job = submitRun(h, "to-1", "sleep", "1000", 150L, 1);
            h.awaitIdle(10);
            Job done = h.require(job.getJobId());
            assertEquals(JobStatus.TIMED_OUT, done.getStatus());
            assertNotNull(done.getErrorMessage());
            assertEquals(0, h.quotas.runningUsedOf("t-exec"));
            assertEquals(0, h.quotas.queuedUsedOf("t-exec"));
            log.info("[CASE:执行超时] pass: status=TIMED_OUT, error={}, 租户配额归零", done.getErrorMessage());
        }
    }

    @Test
    @DisplayName("[CASE:取消排队作业] QUEUED 阶段取消立即 CANCELLED，作业从未执行")
    void cancelQueuedJobNeverRuns() {
        log.info("[CASE:取消排队作业] start: 不启动调度器直接取消");
        CountDownLatch entered = new CountDownLatch(1);
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .handler(new LatchSleepHandler(entered))
                .build()) {
            Job job = submitRun(h, "cq-1", "latched-sleep", "1000", null, 1);
            assertEquals(JobStatus.QUEUED, job.getStatus());

            JobViewCancel(h, job.getJobId());
            Job done = h.require(job.getJobId());
            assertEquals(JobStatus.CANCELLED, done.getStatus());
            assertEquals(0, done.getAttemptCount(), "排队取消的作业不得执行任何一次");
            assertEquals(0, h.quotas.queuedUsedOf("t-exec"));
            log.info("[CASE:取消排队作业] pass: status=CANCELLED, attemptCount=0, 配额归零");
        }
    }

    @Test
    @DisplayName("[CASE:取消运行中作业] RUNNING 阶段取消会中断工作线程并以 CANCELLED 结束")
    void cancelRunningJobInterruptsWorker() throws Exception {
        log.info("[CASE:取消运行中作业] start: 作业运行中被取消");
        CountDownLatch entered = new CountDownLatch(1);
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .workers(1)
                .queuedQuota("t-exec", 10)
                .runningQuota("t-exec", 1)
                .handler(new LatchSleepHandler(entered))
                .build()) {
            h.start();
            Job job = submitRun(h, "cr-1", "latched-sleep", "60000", 60_000L, 1);
            assertTrue(entered.await(5, TimeUnit.SECONDS), "作业必须已经进入运行");
            assertEquals(JobStatus.RUNNING, h.require(job.getJobId()).getStatus());

            h.service.cancel(job.getJobId());
            h.awaitIdle(10);
            Job done = h.require(job.getJobId());
            assertEquals(JobStatus.CANCELLED, done.getStatus());
            assertEquals(0, h.quotas.runningUsedOf("t-exec"));
            assertEquals(0, h.quotas.queuedUsedOf("t-exec"));
            log.info("[CASE:取消运行中作业] pass: status=CANCELLED, running/queued 配额均归零");
        }
    }

    @Test
    @DisplayName("[CASE:失败重试] 前两次失败第三次成功：共执行 3 次，最终 SUCCEEDED，含指数退避")
    void retryWithBackoffThenSucceeds() throws Exception {
        log.info("[CASE:失败重试] start: maxAttempts=3, backoff=30ms, 第 3 次成功");
        CountingFailHandler handler = new CountingFailHandler(3);
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .backoff(30)
                .handler(handler)
                .build()) {
            h.start();
            long t0 = System.nanoTime();
            Job job = submitRun(h, "rt-ok", "counting-fail", "x", 2_000L, 3);
            h.awaitIdle(15);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            Job done = h.require(job.getJobId());
            assertEquals(JobStatus.SUCCEEDED, done.getStatus());
            assertEquals(3, done.getAttemptCount());
            assertEquals(3, handler.calls.get());
            assertTrue(elapsedMs >= 30 + 60 - 20, "必须经历两次指数退避(30ms+60ms)");
            log.info("[CASE:失败重试] pass: attempts=3, 实际调用={}, 总耗时={}ms, result={}",
                    handler.calls.get(), elapsedMs, done.getResult());
        }
    }

    @Test
    @DisplayName("[CASE:重试耗尽] 持续失败达到 maxAttempts 后以 FAILED 终态结束，不再重试")
    void retryExhaustionEndsFailed() throws Exception {
        log.info("[CASE:重试耗尽] start: maxAttempts=2, 每次都失败");
        CountingFailHandler handler = new CountingFailHandler(Integer.MAX_VALUE);
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .backoff(10)
                .handler(handler)
                .build()) {
            h.start();
            Job job = submitRun(h, "rt-fail", "counting-fail", "x", 2_000L, 2);
            h.awaitIdle(15);
            Job done = h.require(job.getJobId());
            assertEquals(JobStatus.FAILED, done.getStatus());
            assertEquals(2, done.getAttemptCount());
            assertEquals(2, handler.calls.get(), "超过上限后不得继续执行");
            assertEquals(0, h.quotas.queuedUsedOf("t-exec"));
            log.info("[CASE:重试耗尽] pass: status=FAILED, attempts=2, 配额归零");
        }
    }

    @Test
    @DisplayName("[CASE:超时后重试] 超时也算一次尝试；最后一次成功则 SUCCEEDED")
    void timeoutIsRetriedLikeFailure() throws Exception {
        log.info("[CASE:超时后重试] start: 第1次 50ms 超时, 第2次快速成功");
        CountingFailHandler handler = new CountingFailHandler(2);
        // counting-fail 第 1 次直接抛异常，这里改用“第1次超时”语义：自定义 handler
        AtomicInteger n = new AtomicInteger();
        JobHandler onceSlow = new JobHandler() {
            @Override public String jobType() { return "once-slow"; }
            @Override public String execute(String payload) throws Exception {
                if (n.incrementAndGet() == 1) {
                    Thread.sleep(500);
                }
                return "fast";
            }
        };
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .backoff(10)
                .handler(onceSlow)
                .build()) {
            h.start();
            Job job = submitRun(h, "to-retry", "once-slow", "x", 80L, 2);
            h.awaitIdle(15);
            Job done = h.require(job.getJobId());
            assertEquals(JobStatus.SUCCEEDED, done.getStatus());
            assertEquals(2, done.getAttemptCount());
            log.info("[CASE:超时后重试] pass: attempts=2, status=SUCCEEDED");
        }
    }

    @Test
    @DisplayName("[CASE:非法状态迁移] 对终态作业再次取消被明确拒绝，原因=ILLEGAL_STATE_TRANSITION")
    void cancelTerminalJobIsRejected() throws Exception {
        log.info("[CASE:非法状态迁移] start: 成功后重复取消");
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .handler(new com.github.lechandonga.batchjobschedulerjava.core.handler.BuiltinHandlers.EchoHandler())
                .build()) {
            h.start();
            Job job = submitRun(h, "illegal-1", "echo", "hi", null, 1);
            h.awaitIdle(10);
            assertEquals(JobStatus.SUCCEEDED, h.require(job.getJobId()).getStatus());

            IllegalJobTransitionException e = assertThrows(IllegalJobTransitionException.class,
                    () -> h.service.cancel(job.getJobId()));
            assertEquals(JobStatus.SUCCEEDED, e.getCurrentStatus());
            log.info("[CASE:非法状态迁移] pass: reason={}, currentStatus={}", e.getReason(),
                    e.getCurrentStatus());
        }
    }

    @Test
    @DisplayName("[CASE:故障隔离] 单个作业抛异常不影响其他作业成功，调度器继续工作")
    void oneJobFailureDoesNotAffectOthers() throws Exception {
        log.info("[CASE:故障隔离] start: 1 个必失败作业 + 2 个正常作业");
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .backoff(10)
                .handler(new com.github.lechandonga.batchjobschedulerjava.core.handler.BuiltinHandlers.FlakyHandler())
                .handler(new com.github.lechandonga.batchjobschedulerjava.core.handler.BuiltinHandlers.EchoHandler())
                .build()) {
            h.start();
            Job bad = submitRun(h, "iso-bad", "flaky", "x", 1_000L, 2);
            Job good1 = submitRun(h, "iso-good-1", "echo", "a", null, 1);
            Job good2 = submitRun(h, "iso-good-2", "echo", "b", null, 1);
            h.awaitIdle(15);
            assertEquals(JobStatus.FAILED, h.require(bad.getJobId()).getStatus());
            assertEquals(JobStatus.SUCCEEDED, h.require(good1.getJobId()).getStatus());
            assertEquals(JobStatus.SUCCEEDED, h.require(good2.getJobId()).getStatus());
            assertEquals("echo:a", h.require(good1.getJobId()).getResult());
            log.info("[CASE:故障隔离] pass: flaky=FAILED, echo 作业仍 SUCCEEDED，调度器存活");
        }
    }

    private static void JobViewCancel(SchedulerTestHarness h, String jobId) {
        h.service.cancel(jobId);
    }
}
