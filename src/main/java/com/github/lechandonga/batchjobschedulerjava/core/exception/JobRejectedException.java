package com.github.lechandonga.batchjobschedulerjava.core.exception;

/** Thrown when a submission is rejected. Rejected submissions never consume any quota. */
public class JobRejectedException extends RuntimeException {

    private final RejectReason reason;

    public JobRejectedException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
