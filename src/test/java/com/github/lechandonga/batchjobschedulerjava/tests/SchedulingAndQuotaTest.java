package com.github.lechandonga.batchjobschedulerjava.tests;

import com.github.lechandonga.batchjobschedulerjava.api.dto.SubmitJobRequest;
import com.github.lechandonga.batchjobschedulerjava.core.handler.JobHandler;
import com.github.lechandonga.batchjobschedulerjava.core.model.Job;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobPriority;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("业务2: 调度 - 租户并发配额、优先级与防饿死公平性")
class SchedulingAndQuotaTest {

    private static final Logger log = LoggerFactory.getLogger(SchedulingAndQuotaTest.class);

    /** Handler blocks until an external latch is released, so we can observe concurrency. */
    static class BlockingHandler implements JobHandler {
        final CountDownLatch gate;
        final AtomicInteger concurrent = new AtomicInteger();
        final AtomicInteger maxConcurrent = new AtomicInteger();
        final List<String> finishOrder = new CopyOnWriteArrayList<>();

        BlockingHandler(CountDownLatch gate) {
            this.gate = gate;
        }

        @Override
        public String jobType() {
            return "block";
        }

        @Override
        public String execute(String payload) throws InterruptedException {
            int now = concurrent.incrementAndGet();
            maxConcurrent.accumulateAndGet(now, Math::max);
            gate.await(5, TimeUnit.SECONDS);
            concurrent.decrementAndGet();
            finishOrder.add(payload);
            return payload;
        }
    }

    static class FastOrderHandler implements JobHandler {
        final List<String> order = new CopyOnWriteArrayList<>();

        @Override
        public String jobType() {
            return "order";
        }

        @Override
        public String execute(String payload) throws InterruptedException {
            order.add(payload);
            Thread.sleep(15);
            return payload;
        }
    }

    @Test
    @DisplayName("[CASE:租户并发占满] runningQuota=2 时该租户任何时刻运行数不超过 2，结束后配额归零")
    void tenantRunningQuotaNeverExceededAndAlwaysReturned() throws Exception {
        log.info("[CASE:租户并发占满] start: t-limited runningQuota=2，提交 6 个阻塞作业");
        CountDownLatch gate = new CountDownLatch(1);
        BlockingHandler handler = new BlockingHandler(gate);
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .workers(8)
                .runningQuota("t-limited", 2)
                .queuedQuota("t-limited", 10)
                .handler(handler)
                .build()) {
            h.start();
            for (int i = 0; i < 6; i++) {
                SubmitJobRequest r = SchedulerTestHarness.request("rq-" + i, "t-limited", "block",
                        JobPriority.NORMAL);
                r.setPayload("job-" + i);
                h.service.submit(r);
            }
            Thread.sleep(400);
            assertEquals(2, h.quotas.runningUsedOf("t-limited"), "租户运行配额绝不能被突破");
            assertEquals(2, handler.maxConcurrent.get(), "业务实际并发也必须等于配额上限");
            log.info("[CASE:租户并发占满] 释放闸门前后观察到 runningUsed=2, maxConcurrent=2");
            gate.countDown();

            h.awaitIdle(15);
            assertEquals(0, h.quotas.runningUsedOf("t-limited"), "全部结束后 running 配额归零");
            assertEquals(0, h.quotas.queuedUsedOf("t-limited"), "全部结束后 queued 配额归零");
            assertEquals(6, handler.finishOrder.size());
            log.info("[CASE:租户并发占满] pass: 6 个作业全部完成，租户 used 配额=0");
        }
    }

    @Test
    @DisplayName("[CASE:优先级调度] 空系统中同一批 HIGH/NORMAL/LOW 按优先级顺序完成")
    void higherPriorityRunsFirst() throws Exception {
        log.info("[CASE:优先级调度] start: 串行 worker=1，按 LOW,NORMAL,HIGH 顺序提交");
        FastOrderHandler handler = new FastOrderHandler();
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .workers(1)
                .queuedQuota("t", 20)
                .runningQuota("t", 1)
                .fairnessQuantum(100_000) // 测试期间关闭老化，只看原始优先级
                .handler(handler)
                .build()) {
            // 先不启动 dispatcher，让三个作业稳定排队
            String[] seq = {"low", "normal", "high"};
            JobPriority[] pri = {JobPriority.LOW, JobPriority.NORMAL, JobPriority.HIGH};
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                SubmitJobRequest r = SchedulerTestHarness.request("pr-" + seq[i], "t", "order", pri[i]);
                r.setPayload(seq[i]);
                ids.add(h.service.submit(r).jobId());
            }
            h.start();
            h.awaitIdle(10);
            assertEquals(List.of("high", "normal", "low"), handler.order,
                    "高优先级必须先被调度，同优先级 FIFO");
            log.info("[CASE:优先级调度] pass: 实际完成顺序={}", handler.order);
        }
    }

    @Test
    @DisplayName("[CASE:防饿死] 持续提交 HIGH 时，老化后的 LOW 作业仍能在有限时间内被执行")
    void lowPriorityCannotBeStarvedByContinuousHighPriority() throws Exception {
        log.info("[CASE:防饿死] start: worker=1, 老化量子=150ms; 先提交 LOW 再持续灌 HIGH");
        FastOrderHandler handler = new FastOrderHandler();
        try (SchedulerTestHarness h = SchedulerTestHarness.builder()
                .workers(1)
                .queuedQuota("t", 1_000)
                .runningQuota("t", 1)
                .fairnessQuantum(150)
                .tick(5)
                .handler(handler)
                .build()) {
            h.start();

            SubmitJobRequest low = SchedulerTestHarness.request("starvation-low", "t", "order",
                    JobPriority.LOW);
            low.setPayload("the-low-job");
            String lowId = h.service.submit(low).jobId();

            // 持续制造高优先级负载，每个作业稍占时间，模拟“高优先级源源不断”
            AtomicInteger produced = new AtomicInteger();
            java.util.concurrent.ScheduledExecutorService feeder =
                    java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
            feeder.scheduleAtFixedRate(() -> {
                int i = produced.incrementAndGet();
                SubmitJobRequest high = SchedulerTestHarness.request("high-" + i, "t", "order",
                        JobPriority.HIGH);
                high.setPayload("high-" + i);
                try {
                    h.service.submit(high);
                } catch (RuntimeException ignore) {
                    // 容量瞬满可接受
                }
            }, 0, 25, TimeUnit.MILLISECONDS);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            Job lowJob;
            while (true) {
                lowJob = h.require(lowId);
                if (lowJob.getStatus() == JobStatus.SUCCEEDED) {
                    break;
                }
                if (System.nanoTime() > deadline) {
                    break;
                }
                Thread.sleep(20);
            }
            feeder.shutdownNow();
            assertEquals(JobStatus.SUCCEEDED, lowJob.getStatus(),
                    "在持续 HIGH 压力下 LOW 作业仍必须在有限时间内完成");
            log.info("[CASE:防饿死] pass: LOW 作业完成，此前已完成 HIGH 作业数={}, 完成顺序片段={}",
                    produced.get(), handler.order.subList(0, Math.min(12, handler.order.size())));
            h.awaitIdle(15);
            assertEquals(0, h.quotas.queuedUsedOf("t"));
        }
    }
}
