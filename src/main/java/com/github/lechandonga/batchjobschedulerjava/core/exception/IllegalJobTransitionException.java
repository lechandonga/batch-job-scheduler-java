package com.github.lechandonga.batchjobschedulerjava.core.exception;

import com.github.lechandonga.batchjobschedulerjava.core.model.JobStatus;

/** Thrown when an operation is attempted from a job state that does not allow it. */
public class IllegalJobTransitionException extends RuntimeException {

    private final RejectReason reason = RejectReason.ILLEGAL_STATE_TRANSITION;
    private final JobStatus currentStatus;

    public IllegalJobTransitionException(JobStatus currentStatus, String operation) {
        super("illegal state transition: operation=" + operation + ", currentStatus=" + currentStatus);
        this.currentStatus = currentStatus;
    }

    public RejectReason getReason() { return reason; }
    public JobStatus getCurrentStatus() { return currentStatus; }
}
