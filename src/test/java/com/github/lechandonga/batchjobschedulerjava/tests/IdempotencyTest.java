package com.github.lechandonga.batchjobschedulerjava.tests;

import com.github.lechandonga.batchjobschedulerjava.model.JobView;
import com.github.lechandonga.batchjobschedulerjava.service.scheduler.BatchJobScheduler;
import com.github.lechandonga.batchjobschedulerjava.tests.support.AbstractBusinessTest;
import com.github.lechandonga.batchjobschedulerjava.tests.support.TestRequests;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "scheduler.default-tenant-quota=50",
        "scheduler.worker-threads=16",
        "scheduler.queue-capacity=500"
})
@DisplayName("业务01：幂等提交（含并发重复提交只产生一个作业）")
class IdempotencyTest extends AbstractBusinessTest {

    @Autowired
    private BatchJobScheduler scheduler;

    @Test
    @DisplayName("同一幂等键串行重复提交返回同一个作业，不会新建")
    void sequentialDuplicateReturnsSameJob() {
        JobView first = scheduler.submit(TestRequests.of("idem-seq", "t1", 5, "success"));
        JobView second = scheduler.submit(TestRequests.of("idem-seq", "t1", 99, "success"));
        JobView third = scheduler.submit(TestRequests.of("idem-seq", "t1", 5, "fatal"));

        assertThat(second.getJobId()).isEqualTo(first.getJobId());
        assertThat(third.getJobId()).isEqualTo(first.getJobId());
        // 重复提交沿用首次请求的参数
        assertThat(second.getPriority()).isEqualTo(5);
        assertThat(second.getHandlerType()).isEqualTo("success");
    }

    @Test
    @DisplayName("20 个线程用同一幂等键并发提交，只产生一个作业，所有调用方拿到同一 jobId")
    void concurrentDuplicateSubmissionCreatesOnlyOneJob() throws Exception {
        int threads = 20;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        Set<String> jobIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
        try {
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    start.await();
                    JobView view = scheduler.submit(TestRequests.of("idem-concurrent", "t1", 5, "success"));
                    jobIds.add(view.getJobId());
                    return null;
                });
            }
            start.countDown();
        } finally {
            pool.shutdown();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(jobIds).hasSize(1);
        scheduler.awaitQuiet(10, TimeUnit.SECONDS);
        JobView finalView = scheduler.getByIdempotencyKey("idem-concurrent");
        assertThat(finalView.getStatus().name()).isEqualTo("SUCCEEDED");
        assertThat(((Number) scheduler.tenantQuotaView("t1").get("active")).intValue()).isZero();
    }

    @Test
    @DisplayName("不同幂等键产生不同作业")
    void differentKeysCreateDifferentJobs() {
        Set<String> ids = new HashSet<>();
        ids.add(scheduler.submit(TestRequests.of("k-a", "t1", 5, "success")).getJobId());
        ids.add(scheduler.submit(TestRequests.of("k-b", "t1", 5, "success")).getJobId());
        assertThat(ids).hasSize(2);
    }
}
