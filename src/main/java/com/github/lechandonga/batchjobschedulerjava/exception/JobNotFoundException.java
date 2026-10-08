package com.github.lechandonga.batchjobschedulerjava.exception;

public class JobNotFoundException extends JobRejectedException {

    public JobNotFoundException(String jobId) {
        super(RejectReason.JOB_NOT_FOUND, "作业不存在: " + jobId);
    }
}
