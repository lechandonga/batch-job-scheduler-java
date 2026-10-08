package com.github.lechandonga.batchjobschedulerjava.core.exception;

public class JobNotFoundException extends RuntimeException {
    public JobNotFoundException(String jobId) {
        super("job not found: " + jobId);
    }
}
