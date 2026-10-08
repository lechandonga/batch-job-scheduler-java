package com.github.lechandonga.batchjobschedulerjava.exception;

import com.github.lechandonga.batchjobschedulerjava.model.JobStatus;

/**
 * 非法状态迁移：作业当前状态不允许目标操作，原因中携带期望状态与实际状态，调用方可区分。
 */
public class IllegalJobTransitionException extends JobRejectedException {

    private final String jobId;
    private final JobStatus expected;
    private final JobStatus actual;

    public IllegalJobTransitionException(String jobId, JobStatus expected, JobStatus actual) {
        super(RejectReason.ILLEGAL_TRANSITION,
                "非法状态迁移: jobId=" + jobId + ", 期望状态=" + expected + ", 实际状态=" + actual);
        this.jobId = jobId;
        this.expected = expected;
        this.actual = actual;
    }

    public String getJobId() {
        return jobId;
    }

    public JobStatus getExpected() {
        return expected;
    }

    public JobStatus getActual() {
        return actual;
    }
}
