package com.github.lechandonga.batchjobschedulerjava.core.service;

import com.github.lechandonga.batchjobschedulerjava.api.dto.JobView;
import com.github.lechandonga.batchjobschedulerjava.api.dto.SubmitJobRequest;
import com.github.lechandonga.batchjobschedulerjava.config.SchedulerProperties;
import com.github.lechandonga.batchjobschedulerjava.core.exception.IllegalJobTransitionException;
import com.github.lechandonga.batchjobschedulerjava.core.exception.JobRejectedException;
import com.github.lechandonga.batchjobschedulerjava.core.exception.RejectReason;
import com.github.lechandonga.batchjobschedulerjava.core.model.Job;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobPriority;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobRequest;
import com.github.lechandonga.batchjobschedulerjava.core.handler.JobHandlerRegistry;
import com.github.lechandonga.batchjobschedulerjava.core.model.JobStatus;
import com.github.lechandonga.batchjobschedulerjava.core.quota.TenantQuotaManager;
import com.github.lechandonga.batchjobschedulerjava.core.scheduler.JobScheduler;
import com.github.lechandonga.batchjobschedulerjava.core.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Application boundary: validates parameters, performs idempotent submission, and delegates
 * execution to the scheduler. Validation happens before any quota/capacity acquisition, so an
 * invalid request can never hold resources.
 */
@Service
public class JobService {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);

    private final JobStore store;
    private final JobScheduler scheduler;
    private final TenantQuotaManager quotaManager;
    private final SchedulerProperties props;
    private final JobHandlerRegistry handlerRegistry;

    public JobService(JobStore store, JobScheduler scheduler, TenantQuotaManager quotaManager,
                      SchedulerProperties props, JobHandlerRegistry handlerRegistry) {
        this.store = store;
        this.scheduler = scheduler;
        this.quotaManager = quotaManager;
        this.props = props;
        this.handlerRegistry = handlerRegistry;
    }

    public JobView submit(SubmitJobRequest request) {
        validate(request);

        JobPriority priority = request.getPriority() == null ? JobPriority.NORMAL : request.getPriority();
        long timeout = request.getTimeoutMillis() == null ? props.getDefaultTimeoutMillis()
                : request.getTimeoutMillis();
        int maxAttempts = request.getMaxAttempts() == null ? props.getDefaultMaxAttempts()
                : request.getMaxAttempts();

        JobRequest jobRequest = new JobRequest(
                UUID.randomUUID().toString(),
                request.getIdempotencyKey().trim(),
                request.getTenantId().trim(),
                request.getJobType().trim(),
                priority,
                timeout,
                maxAttempts,
                request.getPayload());

        handlerRegistry.find(jobRequest.jobType()).orElseThrow(() -> new JobRejectedException(
                RejectReason.HANDLER_NOT_FOUND,
                "no handler registered for jobType=" + jobRequest.jobType()));

        JobStore.Submission submission = store.submit(jobRequest,
                () -> quotaManager.tryAcquireQueued(jobRequest.tenantId()),
                props.getGlobalQueueCapacity());
        Job job = submission.job();
        if (submission.created()) {
            log.info("[submit] accepted jobId={}, idempotencyKey={}, tenant={}, type={}, priority={}",
                    job.getJobId(), job.getIdempotencyKey(), job.getTenantId(), job.getJobType(),
                    job.getPriority());
            scheduler.enqueue(job);
        } else {
            log.info("[submit] idempotent hit idempotencyKey={}, existingJobId={}, status={}",
                    job.getIdempotencyKey(), job.getJobId(), job.getStatus());
        }
        return JobView.of(job, !submission.created());
    }

    private void validate(SubmitJobRequest request) {
        if (request == null) {
            reject(RejectReason.INVALID_PARAMETER, "request body is required");
        }
        requireText(request.getIdempotencyKey(), "idempotencyKey");
        requireText(request.getTenantId(), "tenantId");
        requireText(request.getJobType(), "jobType");
        if (request.getTimeoutMillis() != null && request.getTimeoutMillis() <= 0) {
            reject(RejectReason.INVALID_PARAMETER, "timeoutMillis must be positive");
        }
        if (request.getMaxAttempts() != null && request.getMaxAttempts() <= 0) {
            reject(RejectReason.INVALID_PARAMETER, "maxAttempts must be >= 1");
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            reject(RejectReason.INVALID_PARAMETER, field + " must not be blank");
        }
    }

    private static void reject(RejectReason reason, String message) {
        throw new JobRejectedException(reason, message);
    }

    public JobView get(String jobId) {
        return JobView.of(store.require(jobId), false);
    }

    public JobView cancel(String jobId) {
        Job job = store.require(jobId);
        if (job.getStatus().isTerminal()) {
            throw new IllegalJobTransitionException(job.getStatus(), "cancel");
        }
        boolean cancelled = scheduler.cancel(jobId);
        Job refreshed = store.require(jobId);
        if (!cancelled && refreshed.getStatus() != JobStatus.CANCELLED) {
            throw new IllegalJobTransitionException(refreshed.getStatus(), "cancel");
        }
        log.info("[cancel-api] jobId={}, finalStatus={}", jobId, refreshed.getStatus());
        return JobView.of(refreshed, false);
    }
}
