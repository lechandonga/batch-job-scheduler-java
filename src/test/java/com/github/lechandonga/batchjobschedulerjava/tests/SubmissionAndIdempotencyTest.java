package com.github.lechandonga.batchjobschedulerjava.tests;

import com.github.lechandonga.batchjobschedulerjava.api.dto.JobView;
import com.github.lechandonga.batchjobschedulerjava.api.dto.SubmitJobRequest;
import com.github.lechandonga.batchjobschedulerjava.core.exception.JobRejectedException;
import com.github.lechandonga.batchjobschedulerjava.core.exception.RejectReason;
import com.github.lechandonga.batchjobschedulerjava.core.handler.BuiltinHandlers;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobPriority;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("业务1: 作业提交 - 幂等去重与可区分拒绝原因")
class SubmissionAndIdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(SubmissionAndIdempotencyTest.class);

    @Test
    @DisplayName("[CASE:并发重复提交] 20 线程并发提交同一幂等键，只产生一个作业")
    void concurrentDuplicateSubmissionCreatesOneJob() throws Exception {
        log.info("[CASE:并发重复提交] start: 20 个线程并发提交同一个 idempotencyKey");
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .handler(new BuiltinHandlers.EchoHandler())
                .build()) {
            h.start();
            int threads = 20;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            Set<String> jobIds = ConcurrentHashMap.newKeySet();
            Set<Boolean> reusedFlags = ConcurrentHashMap.newKeySet();
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        JobView view = h.service.submit(
                                SchedulerTestHarness.request("idem-concurrent-1", "t-a", "echo",
                                        JobPriority.NORMAL));
                        jobIds.add(view.jobId());
                        reusedFlags.add(view.reused());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
            pool.shutdownNow();

            assertEquals(1, jobIds.size(), "并发重复提交只能产生一个 jobId");
            assertTrue(reusedFlags.contains(Boolean.TRUE), "部分调用必须返回已有作业(reused=true)");
            h.awaitIdle(10);
            assertEquals(0, h.quotas.queuedUsedOf("t-a"), "作业结束后租户已用配额必须归零");
            log.info("[CASE:并发重复提交] pass: uniqueJobs={}, 出现 reused=true 的调用={}", 1, true);
        }
    }

    @Test
    @DisplayName("[CASE:非法参数] 缺少必填字段/非法超时，返回 INVALID_PARAMETER 且不占配额")
    void invalidParametersAreRejectedDistinguishably() {
        log.info("[CASE:非法参数] start: 提交缺字段与负超时的请求");
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .handler(new BuiltinHandlers.EchoHandler()).build()) {
            h.start();

            SubmitJobRequest blankTenant = SchedulerTestHarness.request("k1", "  ", "echo",
                    JobPriority.NORMAL);
            JobRejectedException e1 = assertThrows(JobRejectedException.class,
                    () -> h.service.submit(blankTenant));
            assertEquals(RejectReason.INVALID_PARAMETER, e1.getReason());

            SubmitJobRequest badTimeout = SchedulerTestHarness.request("k2", "t-a", "echo",
                    JobPriority.NORMAL);
            badTimeout.setTimeoutMillis(0L);
            JobRejectedException e2 = assertThrows(JobRejectedException.class,
                    () -> h.service.submit(badTimeout));
            assertEquals(RejectReason.INVALID_PARAMETER, e2.getReason());

            SubmitJobRequest unknownType = SchedulerTestHarness.request("k3", "t-a", "nope",
                    JobPriority.NORMAL);
            JobRejectedException e3 = assertThrows(JobRejectedException.class,
                    () -> h.service.submit(unknownType));
            assertEquals(RejectReason.HANDLER_NOT_FOUND, e3.getReason());

            assertEquals(0, h.quotas.queuedUsedOf("t-a"), "被拒绝的提交不得占用任何配额");
            log.info("[CASE:非法参数] pass: INVALID_PARAMETER / HANDLER_NOT_FOUND 可区分，配额占用=0");
        }
    }

    @Test
    @DisplayName("[CASE:队列已满] 全局队列容量耗尽返回 QUEUE_FULL")
    void queueFullIsRejectedWithDistinctReason() {
        log.info("[CASE:队列已满] start: 全局容量=2，提交第 3 个作业");
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .capacity(2)
                .handler(new BuiltinHandlers.SleepHandler())
                .build()) {
            // 不 start 调度器，作业稳定停留在 QUEUED，精确占满容量
            SubmitJobRequest r1 = SchedulerTestHarness.request("qf-1", "t-a", "sleep",
                    JobPriority.NORMAL);
            r1.setPayload("500");
            SubmitJobRequest r2 = SchedulerTestHarness.request("qf-2", "t-a", "sleep",
                    JobPriority.NORMAL);
            r2.setPayload("500");
            h.service.submit(r1);
            h.service.submit(r2);

            JobRejectedException e = assertThrows(JobRejectedException.class, () -> h.service.submit(
                    SchedulerTestHarness.request("qf-3", "t-b", "sleep", JobPriority.NORMAL)));
            assertEquals(RejectReason.QUEUE_FULL, e.getReason());
            assertEquals(0, h.quotas.queuedUsedOf("t-b"), "队列满时新租户不得被扣配额");
            log.info("[CASE:队列已满] pass: reason=QUEUE_FULL，t-b 配额占用=0");
        }
    }

    @Test
    @DisplayName("[CASE:租户配额不足] 超过租户提交配额返回 TENANT_QUOTA_EXCEEDED 且不占配额")
    void tenantQuotaExceededIsRejectedWithDistinctReason() {
        log.info("[CASE:租户配额不足] start: tenant-small 提交配额=2");
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .capacity(100)
                .queuedQuota("tenant-small", 2)
                .handler(new BuiltinHandlers.SleepHandler())
                .build()) {
            SubmitJobRequest a = SchedulerTestHarness.request("tq-1", "tenant-small", "sleep",
                    JobPriority.NORMAL);
            a.setPayload("500");
            SubmitJobRequest b = SchedulerTestHarness.request("tq-2", "tenant-small", "sleep",
                    JobPriority.NORMAL);
            b.setPayload("500");
            h.service.submit(a);
            h.service.submit(b);

            JobRejectedException e = assertThrows(JobRejectedException.class, () -> h.service.submit(
                    SchedulerTestHarness.request("tq-3", "tenant-small", "sleep",
                            JobPriority.NORMAL)));
            assertEquals(RejectReason.TENANT_QUOTA_EXCEEDED, e.getReason());
            assertEquals(2, h.quotas.queuedUsedOf("tenant-small"), "拒绝后配额占用不得变化");
            log.info("[CASE:租户配额不足] pass: reason=TENANT_QUOTA_EXCEEDED，已用配额仍为 2");
        }
    }
}
