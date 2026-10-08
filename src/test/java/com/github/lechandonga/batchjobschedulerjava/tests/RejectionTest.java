package com.github.lechandonga.batchjobschedulerjava.tests;

import com.github.lechandonga.batchjobschedulerjava.exception.JobRejectedException;
import com.github.lechandonga.batchjobschedulerjava.exception.RejectReason;
import com.github.lechandonga.batchjobschedulerjava.model.JobView;
import com.github.lechandonga.batchjobschedulerjava.model.SubmitJobRequest;
import com.github.lechandonga.batchjobschedulerjava.service.handler.JobHandlerRegistry;
import com.github.lechandonga.batchjobschedulerjava.service.scheduler.BatchJobScheduler;
import com.github.lechandonga.batchjobschedulerjava.tests.support.AbstractBusinessTest;
import com.github.lechandonga.batchjobschedulerjava.tests.support.LatchHandler;
import com.github.lechandonga.batchjobschedulerjava.tests.support.TestRequests;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "scheduler.default-tenant-quota=3",
        "scheduler.queue-capacity=5",
        "scheduler.worker-threads=2",
        "scheduler.default-timeout-millis=30000"
})
@DisplayName("业务02：提交拒绝原因可区分，且被拒绝的提交不占配额")
class RejectionTest extends AbstractBusinessTest {

    @Autowired
    private BatchJobScheduler scheduler;
    @Autowired
    private JobHandlerRegistry registry;

    private final AtomicInteger counter = new AtomicInteger();

    private String key(String prefix) {
        return prefix + "-" + counter.incrementAndGet();
    }

    @Test
    @DisplayName("参数非法的各种情形均返回 INVALID_PARAMETER")
    void invalidParametersRejected() {
        assertReason(null, RejectReason.INVALID_PARAMETER);
        assertReason(TestRequests.of("", "t", 1, "success"), RejectReason.INVALID_PARAMETER);
        assertReason(TestRequests.of("k", " ", 1, "success"), RejectReason.INVALID_PARAMETER);
        assertReason(TestRequests.of("k", "t", 1, " "), RejectReason.INVALID_PARAMETER);

        SubmitJobRequest badPriority = TestRequests.of("k", "t", 101, "success");
        assertReason(badPriority, RejectReason.INVALID_PARAMETER);
        SubmitJobRequest badTimeout = TestRequests.of("k", "t", 1, "success", null, 0L, null);
        assertReason(badTimeout, RejectReason.INVALID_PARAMETER);
        SubmitJobRequest badAttempts = TestRequests.of("k", "t", 1, "success", null, null, 0);
        assertReason(badAttempts, RejectReason.INVALID_PARAMETER);
    }

    @Test
    @DisplayName("未知处理器类型返回 UNKNOWN_HANDLER，且不占用任何配额")
    void unknownHandlerRejected() {
        int activeBefore = ((Number) scheduler.tenantQuotaView("t1").get("active")).intValue();
        assertThatThrownBy(() -> scheduler.submit(TestRequests.of(key("uh"), "t1", 1, "not-exist")))
                .isInstanceOf(JobRejectedException.class)
                .extracting(e -> ((JobRejectedException) e).getReason())
                .isEqualTo(RejectReason.UNKNOWN_HANDLER);
        assertThat(((Number) scheduler.tenantQuotaView("t1").get("active")).intValue())
                .isEqualTo(activeBefore);
    }

    @Test
    @DisplayName("租户活跃配额用尽返回 TENANT_QUOTA_EXCEEDED；其他租户不受影响；拒绝不占配额")
    void tenantQuotaEnforcedAndRejectionDoesNotConsumeQuota() {
        for (int i = 0; i < 3; i++) {
            scheduler.submit(TestRequests.of(key("q"), "tenantA", 1, "sleep", "30000", null, 1));
        }
        // 两个进入 RUNNING、一个 QUEUED，活跃数恰好等于配额 3
        waitFor(() -> ((Number) scheduler.tenantQuotaView("tenantA").get("active")).intValue() == 3);

        assertThatThrownBy(() -> scheduler.submit(TestRequests.of(key("q"), "tenantA", 1, "success")))
                .isInstanceOf(JobRejectedException.class)
                .extracting(e -> ((JobRejectedException) e).getReason())
                .isEqualTo(RejectReason.TENANT_QUOTA_EXCEEDED);
        // 被拒绝后已用配额不增加
        assertThat(((Number) scheduler.tenantQuotaView("tenantA").get("active")).intValue()).isEqualTo(3);
        // 其他租户仍可提交
        JobView other = scheduler.submit(TestRequests.of(key("q"), "tenantB", 1, "success"));
        assertThat(other.getTenantId()).isEqualTo("tenantB");
    }

    @Test
    @DisplayName("全局队列容量用尽返回 QUEUE_FULL，且拒绝后仍保持容量边界")
    void queueFullRejected() {
        // 占住 2 个工作线程，让后续作业全部排队
        scheduler.submit(TestRequests.of(key("cap"), "occupy", 1, "sleep", "30000", null, 1));
        scheduler.submit(TestRequests.of(key("cap"), "occupy", 1, "sleep", "30000", null, 1));
        // 队列容量 5：恰好放 5 个排队作业
        for (int i = 0; i < 5; i++) {
            scheduler.submit(TestRequests.of(key("cap"), "q", 1, "success"));
        }
        assertThatThrownBy(() -> scheduler.submit(TestRequests.of(key("cap"), "q", 1, "success")))
                .isInstanceOf(JobRejectedException.class)
                .extracting(e -> ((JobRejectedException) e).getReason())
                .isEqualTo(RejectReason.QUEUE_FULL);
    }

    @Test
    @DisplayName("查询不存在的作业返回 JOB_NOT_FOUND")
    void jobNotFound() {
        assertThatThrownBy(() -> scheduler.getJob("no-such-id"))
                .isInstanceOf(JobRejectedException.class)
                .extracting(e -> ((JobRejectedException) e).getReason())
                .isEqualTo(RejectReason.JOB_NOT_FOUND);
    }

    private void assertReason(SubmitJobRequest request, RejectReason expected) {
        assertThatThrownBy(() -> scheduler.submit(request))
                .isInstanceOf(JobRejectedException.class)
                .extracting(e -> ((JobRejectedException) e).getReason())
                .isEqualTo(expected);
    }

    private void waitFor(Runnable condition) {
        long deadline = System.currentTimeMillis() + 5000;
        AssertionError last;
        while (true) {
            try {
                org.assertj.core.api.Assertions.assertThatCode(condition::run).doesNotThrowAnyException();
                return;
            } catch (AssertionError e) {
                last = e;
            }
            if (System.currentTimeMillis() > deadline) {
                throw last;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }
}
